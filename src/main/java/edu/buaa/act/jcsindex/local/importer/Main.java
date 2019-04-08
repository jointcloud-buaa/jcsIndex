package edu.buaa.act.jcsindex.local.importer;

import edu.buaa.act.jcsindex.local.utils.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONObject;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by shmin at 2018/5/9 16:32
 * 1.　定义接口参数，Main函数应该传入这些参数（sourceZkAddr, destZkAddr, sourceTableName, destTableName, startTime, endTime, splits, index）
 * 2. 读取车俩ID文件，分组，读取对应范围的车俩ID。
 * 3. 读取指定时间段指定车俩的数据，并将数据放入一个队列中
 * 4. 另外一个线程池读取队列中的数据，存如本地的HBase.
 * 5. 任务完成
 **/
public class Main {
    public static void main(String[] args) throws IOException {
        // 获取传入参数
        if (args.length != 8) {
            System.out.println("Missing argument!");
            return;
        }
        long beginTime = System.currentTimeMillis();
        String sourceZkAddr = args[0];
        String destZkAddr = args[1];
        String sourceTableName = args[2];
        String destTableName = args[3];
        int startTime = Integer.parseInt(args[4]);
        int stopTime = Integer.parseInt(args[5]);
        int splits = Integer.parseInt(args[6]);
        int block = Integer.parseInt(args[7]);
        // 读取devicesn.txt获取北京地区所有车俩ID
        Scanner sc = new Scanner(Main.class.getResourceAsStream("/devicesn_beijing.txt"));
        List<String> devicesns = new ArrayList<>();
        while (sc.hasNextLine()) {
            String devicesn = sc.nextLine();
            devicesns.add(devicesn.substring(1, devicesn.length()-1));
        }
        // 获取某个云的数据，云的编号为block
        List<String> blockDevicesns = new ArrayList<>();
        int begin = 0, end = devicesns.size();
        if (block == 1) {
            end = end / splits;
        } else if (block == splits) {
            begin = end / splits * (block - 1);
        } else {
            begin = end / splits * (block - 1);
            end = end / splits * block;
        }
        for (int i = begin; i < end; i++) {
            blockDevicesns.add(devicesns.get(i));
        }
        System.out.println(blockDevicesns.size());
        // 读取数据，并把数据放在BlockQueue里面
        BlockingQueue<JSONObject> inputQueue = new LinkedBlockingDeque<>();
        Producer producer = new Producer(sourceZkAddr, sourceTableName, startTime, stopTime, blockDevicesns, inputQueue);
        Consumer consumer = new Consumer(destZkAddr, destTableName, inputQueue);

        Thread prodThread = new Thread(producer);
        Thread consThread = new Thread(consumer);

        prodThread.start();
        consThread.start();
        while (true) {
            if (!producer.isFinished || inputQueue.size() != 0) {
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
            } else {
                break;
            }
        }
        consumer.putRest();
        long endTime = System.currentTimeMillis();
        System.out.println("程序运行时间：" + (endTime - beginTime) / 1000 + "s");
        System.out.println(inputQueue.size());
        System.exit(0);
    }
}

// Producer Class in Java
class Producer implements Runnable {
    private final BlockingQueue<JSONObject> sharedQueue;
    private final String zkAddr;
    private final String tableName;
    private final int startTime;
    private final int stopTime;
    private final List<String> devicesns;
    public boolean isFinished = false;

    private static final byte[] TABLE_NAME = Bytes.toBytes("trustcars");
    private final byte[] GPSCF = Bytes.toBytes("G");
    private static Configuration conf;
    private static Map<String, byte[]> columns;

    public Producer(String zkAddr, String tableName, int startTime, int stopTime, List<String> devicesns, BlockingQueue<JSONObject> sharedQueue) {
        this.zkAddr = zkAddr;
        this.tableName = tableName;
        this.startTime = startTime;
        this.stopTime = stopTime;
        this.devicesns = devicesns;
        this.sharedQueue = sharedQueue;
        init();
    }

    private void init() {
        // TODO: 初始化连接
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", zkAddr);
        conf.setLong("hbase.rpc.timeout", 600000);
        conf.setLong("hbase.client.scanner.caching", 1000);
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        columns = new HashMap<String, byte[]>();
        columns.put("longitude", Bytes.toBytes("JD"));
        columns.put("latitude", Bytes.toBytes("WD"));
        columns.put("direction", Bytes.toBytes("D"));
        columns.put("speed", Bytes.toBytes("S"));
    }

    @Override
    public void run() {
        for (int i = 0; i < devicesns.size(); i++) {
            scanGPS(startTime, stopTime, devicesns.get(i));
        }
        isFinished = true;
    }


    public long scanGPS(long startTime, long stopTime, String devicesn) {
        Integer count = 0;
        HTable table;
        try {
            table = new HTable(conf, TABLE_NAME);

            byte[] startRow = Utils.generateRowkeyPM(startTime, devicesn);
            byte[] stopRow = Utils.generateRowkeyPM(stopTime, devicesn);

            Scan scanner = new Scan();
            // scanner.setFilter(new PageFilter(1));
            scanner.setCaching(100);
            scanner.addFamily(GPSCF);
            scanner.addColumn(GPSCF, columns.get("longitude"));
            scanner.addColumn(GPSCF, columns.get("latitude"));
            scanner.addColumn(GPSCF, columns.get("direction"));
            scanner.addColumn(GPSCF, columns.get("speed"));

            scanner.setStartRow(startRow);
            scanner.setStopRow(stopRow);
            scanner.setReversed(true);

            ResultScanner resultScanner = table.getScanner(scanner);
            for (Result res : resultScanner) {
                res.size();
                if (res != null) {
                    Double longitude = Bytes.toDouble(res.getValue(GPSCF, columns.get("longitude")));
                    Double latitude = Bytes.toDouble(res.getValue(GPSCF, columns.get("latitude")));
                    Integer direction = Bytes.toInt(res.getValue(GPSCF, columns.get("direction")));
                    Double speed = Bytes.toDouble(res.getValue(GPSCF, columns.get("speed")));
                    String rk = Bytes.toString(res.getRow());

                    // get timestamp from rowkey
                    long timestamp = Long.MAX_VALUE - Long.valueOf(rk.substring(rk.length() - 19, rk.length()));

                    JSONObject object = new JSONObject();
                    object.put("rowkey", rk);
                    object.put("devicesn", devicesn);
                    object.put("longitude", longitude);
                    object.put("latitude", latitude);
                    object.put("speed", speed);
                    object.put("direction", direction);
                    object.put("timestamp", timestamp);
                    sharedQueue.put(object);
                    count++;
                }
            }
            resultScanner.close();
            table.close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return count;
        }
        return count;
    }
}

// Consumer Class in Java
class Consumer implements Runnable {
    private final String zkAddr;
    private final String tableName;
    private final BlockingQueue<JSONObject> sharedQueue;
    private final List<Put> array = new ArrayList<>();

    private static final byte[] TABLE_NAME = Bytes.toBytes("cartest");
    private final byte[] GPSCF = Bytes.toBytes("G");
    private static Configuration conf;
    private static Map<String, byte[]> columns;

    public Consumer(String zkAddr, String tableName, BlockingQueue<JSONObject> sharedQueue) {
        this.zkAddr = zkAddr;
        this.tableName = tableName;
        this.sharedQueue = sharedQueue;
        init();
    }

    private void init() {
        // TODO: 初始化连接，暂时不做操作
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", zkAddr);
        conf.setLong("hbase.rpc.timeout", 600000);
        conf.setLong("hbase.client.scanner.caching", 1000);
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        columns = new HashMap<String, byte[]>();
        columns.put("longitude", Bytes.toBytes("JD"));
        columns.put("latitude", Bytes.toBytes("WD"));
        columns.put("direction", Bytes.toBytes("D"));
        columns.put("speed", Bytes.toBytes("S"));
    }

    @Override
    public void run() {
        Connection conn = null;
        Table table = null;
        try {
            conn = ConnectionFactory.createConnection(conf);
            table = conn.getTable(TableName.valueOf(this.tableName));
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        while (true) {
            try {
                JSONObject object = sharedQueue.take();
                Put put = new Put(Bytes.toBytes(object.getString("rowkey")));
                put.addColumn(GPSCF, Bytes.toBytes("devicesn"), Bytes.toBytes(object.getString("devicesn")));
                put.addColumn(GPSCF, Bytes.toBytes("longitude"), Bytes.toBytes(object.getDouble("longitude")));
                put.addColumn(GPSCF, Bytes.toBytes("latitude"), Bytes.toBytes(object.getDouble("latitude")));
                put.addColumn(GPSCF, Bytes.toBytes("speed"), Bytes.toBytes(object.getDouble("speed")));
                put.addColumn(GPSCF, Bytes.toBytes("direction"), Bytes.toBytes(object.getInt("direction")));
                put.addColumn(GPSCF, Bytes.toBytes("timestamp"), Bytes.toBytes(object.getLong("timestamp")));
                array.add(put);
                if (array.size() == 1500) {
                    table.put(array);
                    array.clear();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }  catch (InterruptedException ex) {
                ex.printStackTrace();
            }
        }
    }

    public void putRest() {
        if (array.size() != 0) {
            Connection conn = null;
            Table table = null;
            try {
                conn = ConnectionFactory.createConnection(conf);
                table = conn.getTable(TableName.valueOf(this.tableName));
                table.put(array);
                array.clear();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }
}
