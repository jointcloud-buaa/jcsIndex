package edu.buaa.act.jcsindex.local.jcssti.index.node;

import bplus.bptree.BPlusConfiguration;
import bplus.bptree.BPlusTree;
import bplus.bptree.BPlusTreePerformanceCounter;
import bplus.bptree.SearchResult;
import edu.buaa.act.jcsindex.local.bean.ParaGPSRecord;
import edu.buaa.act.jcsindex.local.bean.ParaRectangle;
import edu.buaa.act.jcsindex.local.jcssti.Constants;
import edu.buaa.act.jcsindex.local.jcssti.index.grid.Grid;
import edu.buaa.act.jcsindex.local.jcssti.index.grid.ZOrder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by shimin at 2018/7/6 0:04
 * TODO: 暂时默认为索引文件已经构建好，直接获取；后期需要加入创建索引的命令
 **/
public class DataNodeImpl implements IDataNode {
    // 本节点的机器名称
    private final String serverName;

    // 本节点的IP地址
    private final String ip;

    // global index的通信端口
    private final int gport;

    // 本集群的id
    private final int clusterID;

    // HBase表的名字
    private final String tableName;

    // RangeServer上的表
    private List<HRegionInfo> regionInfos;

    // 本节点需要管理的B*tree索引
    private BPlusTree[] indexs;

    // 本节点管理的索引摘要
    private IndexSummary indexSummary;

    // 索引文件存访地址
    private String dbPath;

    // HBase相关配置信息
    private Configuration conf;

    // GridInstance，确定如何切分网格
    private Grid gridInstance;

    // 查询线程池
    private ExecutorService queryExecutorService;

    // HBase线程池
    private HConnection pool;

    public DataNodeImpl(Configuration conf, int poolSize, String tableName, String serverName, String ip, int gport, int clusterID, Grid gridInstance, String dbPath, int N) {
        this.conf = conf;
        this.tableName = tableName;
        this.serverName = serverName;
        this.ip = ip;
        this.gport = gport;
        this.clusterID = clusterID;
        this.regionInfos = new ArrayList<>();
        this.gridInstance = gridInstance;
        this.dbPath = dbPath;
        this.indexs = new BPlusTree[N];

        queryExecutorService = Executors.newFixedThreadPool(poolSize);
        try {
            pool = HConnectionManager.createConnection(conf, queryExecutorService);
        } catch (Exception e) {
            throw new RuntimeException("无法获取线程池");
        }
        initRegions();
        initIndexs();
    }

    // 获取RegionServer上所有的RegionInfo
    private void initRegions() {
        try {
            ClusterConnection connection = (ClusterConnection )ConnectionFactory.createConnection(conf);
            Admin admin = connection.getAdmin();
            ClusterStatus status = admin.getClusterStatus();
            Collection<ServerName> regionServers = status.getServers();

            for (ServerName rsinfo : regionServers) {
                AdminProtos.AdminService.BlockingInterface server = connection.getAdmin(rsinfo);

                // List all online region from this region server
                System.out.println("(RegionServer, " + rsinfo.getServerName() + ")");
                if (rsinfo.getServerName().startsWith(serverName)) {
                    List<HRegionInfo> rInfos = ProtobufUtil.getOnlineRegions(server);
                    for (HRegionInfo rinfo : rInfos) {
                        System.out.println("-- Region metadata: " + rinfo.getRegionNameAsString());
                        if (rinfo.getRegionNameAsString().startsWith(tableName)) {
                            regionInfos.add(rinfo);
                        }
                    }
                }
            }
            connection.close();
            System.out.println("RegionServer region size is: " + regionInfos.size());
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("无法获取RegionServer上的所有Region");
            System.exit(0);
        }
    }

    // 初始化索引
    private void initIndexs() {
        BPlusConfiguration bconf = new BPlusConfiguration(2048);
        BPlusTreePerformanceCounter bPerf = new BPlusTreePerformanceCounter(true);
        boolean recreateTree = false;
        // 判断是否为空目录
        File file = new File(dbPath);
        if (file.listFiles() == null || file.listFiles().length == 0) {
            recreateTree = true;
        }
        try {
            for (int i = 0; i < indexs.length; i++) {
                // TODO: 后缀是bin,改成db更合适
                indexs[i] = new BPlusTree(bconf, recreateTree ? "rw+" : "rw", dbPath + "/" + "tree" + i + ".bin", bPerf);
            }
            indexSummary = new IndexSummary(indexs, ip, gport);
        } catch (Exception e) {
            throw new RuntimeException("索引初始化失败", e);
        }
    }

    @Override
    public List<ParaGPSRecord> rangeQuery(ParaRectangle rectangle, int timeIndex) {
        List<String> sub = new ArrayList<>();
        int x1 = gridInstance.getX(rectangle.minX);
        int y1 = gridInstance.getY(rectangle.minY);
        int x2 = gridInstance.getX(rectangle.maxX);
        int y2 = gridInstance.getY(rectangle.maxY);
        System.out.println("Search Rectangle: " + rectangle.minX + " " + rectangle.minY + " " +  rectangle.maxX + " " + rectangle.maxY + " clusterid: " + clusterID + " timeIndex: " + timeIndex);
        try {
            for (int i = x1; i < x2; i++) {
                for (int j = y1; j < y2; j++) {
                    int gridId = ZOrder.getZOrderStr(i, j);
                    if (!indexSummary.contains(timeIndex, gridId)) continue;
                    SearchResult sr = indexs[timeIndex].searchKey(gridId, true);
                    if (sr.getValues() != null) {
                        sub.addAll(sr.getValues());
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        List<ParaGPSRecord> res = new ArrayList<>();
        try {
            HTableInterface hTable = pool.getTable(tableName);
            List<Get> gets = new ArrayList<>();
            for (int i = 0; i < sub.size(); i++) {
                String rowkey = sub.get(i);
                Get get = new Get(Bytes.toBytes(rowkey.trim()));
                get.addColumn(Constants.GPSCF, Constants.GPSCF_LONGTITUDE);
                get.addColumn(Constants.GPSCF, Constants.GPSCF_LATITUDE);
                get.addColumn(Constants.GPSCF, Constants.GPSCF_DEVICESN);
                get.addColumn(Constants.GPSCF, Constants.GPSCF_TIMESTAMP);
                gets.add(get);
            }
            Result[] results = hTable.get(gets);
            for (int i = 0; i < results.length; i++) {
                ParaGPSRecord gpsRecord = new ParaGPSRecord();
                gpsRecord.setLongitude((float )Bytes.toDouble(results[i].getValue(Constants.GPSCF, Constants.GPSCF_LONGTITUDE)));
                gpsRecord.setLatitude((float )Bytes.toDouble(results[i].getValue(Constants.GPSCF, Constants.GPSCF_LATITUDE)));
                gpsRecord.setDevicesn(Long.parseLong(Bytes.toString(results[i].getValue(Constants.GPSCF, Constants.GPSCF_DEVICESN))));
                gpsRecord.setGpstime(Bytes.toLong(results[i].getValue(Constants.GPSCF, Constants.GPSCF_TIMESTAMP)));
                gpsRecord.setClusterid(clusterID);
                res.add(gpsRecord);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Res size: " + res.size());
        return res;
    }

    @Override
    public List<ParaGPSRecord> rangeQuery(ParaRectangle rectangle, long startTime, long endTime) {
        // TODO：该部分存在魔数，后续需要修改
        int start = (int )(startTime - Constants.STARTTIME) / 3600 % Constants.N;
        int end = (int )(endTime - Constants.STARTTIME) / 3600 % Constants.N;
        List<String> sub = new ArrayList<>();
        int x1 = gridInstance.getX(rectangle.minX);
        int y1 = gridInstance.getY(rectangle.minY);
        int x2 = gridInstance.getX(rectangle.maxX);
        int y2 = gridInstance.getY(rectangle.maxY);
        System.out.println("Search Rectangle: " + rectangle.minX + " " + rectangle.minY + " " +  rectangle.maxX + " " + rectangle.maxY + " clusterid: " + clusterID);
        for (int time = start; time < end; time++) {
            try {
                for (int i = x1; i < x2; i++) {
                    for (int j = y1; j < y2; j++) {
                        int gridId = ZOrder.getZOrderStr(i, j);
                        if (!indexSummary.contains(time, gridId)) continue;
                        SearchResult sr = indexs[time].searchKey(gridId, true);
                        if (sr.getValues() != null) {
                            sub.addAll(sr.getValues());
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        List<ParaGPSRecord> res = new ArrayList<>();
        try {
            HTableInterface hTable = pool.getTable(tableName);
            List<Get> gets = new ArrayList<>();
            for (int i = 0; i < sub.size(); i++) {
                String rowkey = sub.get(i);
                Get get = new Get(Bytes.toBytes(rowkey.trim()));
                get.addColumn(Constants.GPSCF, Constants.GPSCF_LONGTITUDE);
                get.addColumn(Constants.GPSCF, Constants.GPSCF_LATITUDE);
                get.addColumn(Constants.GPSCF, Constants.GPSCF_DEVICESN);
                get.addColumn(Constants.GPSCF, Constants.GPSCF_TIMESTAMP);
                gets.add(get);
            }
            Result[] results = hTable.get(gets);
            for (int i = 0; i < results.length; i++) {
                ParaGPSRecord gpsRecord = new ParaGPSRecord();
                gpsRecord.setLongitude((float )Bytes.toDouble(results[i].getValue(Constants.GPSCF, Constants.GPSCF_LONGTITUDE)));
                gpsRecord.setLatitude((float )Bytes.toDouble(results[i].getValue(Constants.GPSCF, Constants.GPSCF_LATITUDE)));
                gpsRecord.setDevicesn(Long.parseLong(Bytes.toString(results[i].getValue(Constants.GPSCF, Constants.GPSCF_DEVICESN))));
                gpsRecord.setGpstime(Bytes.toLong(results[i].getValue(Constants.GPSCF, Constants.GPSCF_TIMESTAMP)));
                gpsRecord.setClusterid(clusterID);
                res.add(gpsRecord);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return res;
    }

    /**
     * 该函数仅用于测试时使用
     * @param timeIndex
     * @return
     */
    @Override
    public List<ParaGPSRecord> rangeQuery(int timeIndex) {
        if (timeIndex < 0 || timeIndex > indexs.length) {
            // TODO：Invalid args, Just return null
            return null;
        }
        int x1 = gridInstance.getX(116.3405697064F);
        int y1 = gridInstance.getY(39.9766254614F);
        int x2 = gridInstance.getX(116.3599334540F);
        int y2 = gridInstance.getY(39.9860593276F);
        List<String> sub = new ArrayList<>();
        try {
            for (int i = x1; i < x2; i++) {
                for (int j = y1; j < y2; j++) {
                    int gridId = ZOrder.getZOrderStr(i, j);
                    if (!indexSummary.contains(timeIndex, gridId)) continue;
                    SearchResult sr = indexs[timeIndex].searchKey(gridId, true);
                    if (sr.getValues() != null) {
                        if (sr.getValues().size() > 1) {
                            System.out.println(sr.getValues().size());
                        }
                        sub.addAll(sr.getValues());
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("HBase Query Size: " + sub.size());
        List<ParaGPSRecord> res = new ArrayList<>();
        try {
            HTableInterface hTable = pool.getTable(tableName);
            List<Get> gets = new ArrayList<>();
            for (int i = 0; i < sub.size(); i++) {
                String rowkey = sub.get(i);
                Get get = new Get(Bytes.toBytes(rowkey.trim()));
                get.addColumn(Constants.GPSCF, Constants.GPSCF_LONGTITUDE);
                get.addColumn(Constants.GPSCF, Constants.GPSCF_LATITUDE);
                get.addColumn(Constants.GPSCF, Constants.GPSCF_DEVICESN);
                get.addColumn(Constants.GPSCF, Constants.GPSCF_TIMESTAMP);
                gets.add(get);
            }
            Result[] results = hTable.get(gets);
            for (int i = 0; i < results.length; i++) {
                ParaGPSRecord gpsRecord = new ParaGPSRecord();
                gpsRecord.setLongitude((float )Bytes.toDouble(results[i].getValue(Constants.GPSCF, Constants.GPSCF_LONGTITUDE)));
                gpsRecord.setLatitude((float )Bytes.toDouble(results[i].getValue(Constants.GPSCF, Constants.GPSCF_LATITUDE)));
                gpsRecord.setDevicesn(Long.parseLong(Bytes.toString(results[i].getValue(Constants.GPSCF, Constants.GPSCF_DEVICESN))));
                gpsRecord.setGpstime(Bytes.toLong(results[i].getValue(Constants.GPSCF, Constants.GPSCF_TIMESTAMP)));
                gpsRecord.setClusterid(clusterID);
                res.add(gpsRecord);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return res;
    }

    @Override
    public void publishRange() {
        // 发布索引摘要
        System.out.println("start to publish index summary");
        indexSummary.publishRange();
        System.out.println("end to publish index summary");
    }

    public void initializeRange() {
        long current = System.currentTimeMillis();
        System.out.println("start to initialize index summary");
        indexSummary.initialize();
        long end = System.currentTimeMillis();
        System.out.println("end to initialize index summary, elapsed: " + (end - current));
    }

    public void initializeAndpublish() {
        long current = System.currentTimeMillis();
        System.out.println("start to initialize and publish");
        indexSummary.initializeAndpublish();
        long end = System.currentTimeMillis();
        System.out.println("end to initialize and publish, elapsed: " + (end - current));
    }

    /**
     * 插入数据
     */
    @Override
    public void insert() {
        System.out.println("Begin to insert data");
        long start = System.currentTimeMillis();
        for (int ix = 0; ix < regionInfos.size(); ix++) {
            System.out.println("Read " + ix + "`th region metadata");
            final int index = ix;

            // split region to two half, then insert
//            String startKey = new String(regionInfos.get(index).getStartKey());
//            String endKey = new String(regionInfos.get(index).getEndKey());
//            if (startKey.length() == 0) {
//                BigInteger midKeyBig = new BigInteger(endKey).divide(new BigInteger("2"));
//                String midKey= midKeyBig.toString();
//                for (int i = 0; i < endKey.length(); i++) {
//                    if (endKey.charAt(i) == '0') {
//                        midKey = '0' + midKey;
//                    } else {
//                        break;
//                    }
//                }
//                realInsert(regionInfos.get(ix).getStartKey(), midKey.getBytes());
//                realInsert(midKey.getBytes(), regionInfos.get(ix).getEndKey());
//            } else if (endKey.length() == 0) {
//                realInsert(regionInfos.get(index).getStartKey(), regionInfos.get(index).getEndKey());
//            } else {
//                int prefix = 0;
//                if (startKey.charAt(0) == '0' && endKey.charAt(0) == '0') {
//                    for (int i = 0; i < startKey.length(); i++) {
//                        if (startKey.charAt(i) == '0' && endKey.charAt(0) == '0') {
//                            prefix++;
//                        } else {
//                            break;
//                        }
//                    }
//                }
//                String midKey = "";
//                if (startKey.length() != endKey.length()) {
//                    if (startKey.length() < endKey.length()) {
//                        int len = endKey.length() - startKey.length();
//                        for (int i = 0; i < len; i++) {
//                            startKey = startKey + "0";
//                        }
//                        midKey = new BigInteger(startKey).add(new BigInteger(endKey)).divide(new BigInteger("2")).toString();
//                    } else {
//                        int len = startKey.length() - endKey.length();
//                        for (int i = 0; i < len; i++) {
//                            endKey = endKey + "0";
//                        }
//                        midKey = new BigInteger(startKey).add(new BigInteger(endKey)).divide(new BigInteger("2")).toString();
//                    }
//                    for (int i = 0; i < prefix; i++) {
//                        midKey = "0" + midKey;
//                    }
//                    realInsert(regionInfos.get(ix).getStartKey(), midKey.getBytes());
//                    realInsert(midKey.getBytes(), regionInfos.get(ix).getEndKey());
//                } else {
//                    // 二者长度相等，正好
//                    midKey = new BigInteger(startKey).add(new BigInteger(endKey)).divide(new BigInteger("2")).toString();
//                    for (int i = 0; i < prefix; i++) {
//                        midKey = "0" + midKey;
//                    }
//                    realInsert(regionInfos.get(ix).getStartKey(), midKey.getBytes());
//                    realInsert(midKey.getBytes(), regionInfos.get(ix).getEndKey());
//                }
//            }
            // 考虑到目前Region都比较小，可以直接读取了
            realInsert(regionInfos.get(ix).getStartKey(), regionInfos.get(ix).getEndKey());
            long now = System.currentTimeMillis();
            System.out.println("Data inserting, elapsed: " + (now - start) + "ms");
        }
        long end = System.currentTimeMillis();
        System.out.println("Data insertion completed, elapsed " + (end - start) + " ms");
    }

    private void realInsert(byte[] startKey, byte[] endKey) {
        try {
            long start = System.currentTimeMillis();

            List<GPSBean> records = new ArrayList<>();
            Thread insertThreads = new Thread(new Runnable() {
                @Override
                public void run(){
                    try {
                        scanGPS(conf, Bytes.toBytes(tableName), startKey, endKey, records);
                    } catch (IOException e) {
                        System.out.println("Read failed");
                        e.printStackTrace();
                    }
                }
            });
            insertThreads.run();

            GPSBuffer gpsBuffer = new GPSBuffer(records);
            gpsBuffer.shuffle();

            AtomicInteger count = new AtomicInteger(0);
            System.out.println("Print Thread：\n");
            Thread printThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    while (!gpsBuffer.isFinished()) {
                        try {
                            System.out.println("Already insert data num is " + count.get());
                            Threads.sleep(5000);
                        } catch (Exception ex) {
                            ex.printStackTrace();
                        }
                    }
                }
            });
            printThread.start();

            Thread[] threads = new Thread[24];
            for (int i = 0; i < threads.length; i++) {
                threads[i] = new Thread(new Runnable() {
                    GPSBean gpsBean;

                    @Override
                    public void run() {
                        while ((gpsBean = gpsBuffer.getNextGPSBeanForInsert()) != null) {
                            int index = (int )((gpsBean.timestamp - Constants.STARTTIME) / 3600 % Constants.N);
                            int gridId = ZOrder.getZOrderStr(gridInstance.getX(gpsBean.longitude), gridInstance.getY(gpsBean.latitude));
                            try {
                                // TODO: 坑的一点是，unique为true，表示Key相同，不插入
                                // 基于两点原因改为true: 1是插入效率太慢了，为了保证能做实验；2. 生成文件体积太大，我自己都有点怀疑数据的真实性了
                                indexs[index].insertKey(gridId, gpsBean.rowKey, true);
                                count.incrementAndGet();
                            } catch (Exception ex) {
                                ex.printStackTrace();
                            }
                        }
                    }
                });
                threads[i].start();
            }
            for (int i = 0; i < threads.length; i++) {
                threads[i].join();
            }
            printThread.join();
            long now = System.currentTimeMillis();
            System.out.println("Data inserting, elapsed " + (now - start));
        } catch (Exception e) {
            exit();
            e.printStackTrace();
        }
    }

    private int scanGPS(Configuration conf, byte[] tableName, byte[] startRow, byte[] stopRow, List<GPSBean> records) throws IOException {
        long start = System.currentTimeMillis();
        HTable table = new HTable(conf, tableName);
        Scan scanner = new Scan();
        scanner.setCaching(1000);
        scanner.addFamily(Constants.GPSCF);
        scanner.addColumn(Constants.GPSCF, Bytes.toBytes("longitude"));
        scanner.addColumn(Constants.GPSCF, Bytes.toBytes("latitude"));

        scanner.setStartRow(startRow);
        scanner.setStopRow(stopRow);
        scanner.setReversed(false);

        ResultScanner resultScanner = table.getScanner(scanner);
        int count = 0;
        for (Result res : resultScanner) {
            if (res != null) {
                String rk = Bytes.toString(res.getRow());
                float longitude = (float )Bytes.toDouble(res.getValue(Constants.GPSCF, Bytes.toBytes("longitude")));
                float latitude = (float )Bytes.toDouble(res.getValue(Constants.GPSCF, Bytes.toBytes("latitude")));
                long timestamp = Long.MAX_VALUE - Long.valueOf(rk.substring(rk.length() - 19, rk.length()));
                if (isValidGPS(longitude, latitude)) {
                    GPSBean bean = new GPSBean(rk, longitude, latitude, timestamp);
                    records.add(bean);
                    count++;
                }
            }
        }
        long end = System.currentTimeMillis();
        System.out.println("All consume data` num is " + count + " and elapse " + (end - start));
        resultScanner.close();
        table.close();
        return 0;
    }

    private boolean isValidGPS(float longitude, float latitude) {
        if (longitude > 116.848701941655 || longitude < 115.542991693034 || latitude > 40.3485250469718 || latitude < 39.523733567851) {
            return false;
        }
        return true;
    }

    public void exit() {
        try {
            for (int i = 0; i < indexs.length; i++) {
                indexs[i].commitTree();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        queryExecutorService.shutdown();
    }
}

class GPSBuffer {
    private List<GPSBean> records;
    private volatile int count;
    private volatile boolean isFinshed;

    public GPSBuffer(List<GPSBean> records) {
        this.records = records;
    }

    public void shuffle() {
        Collections.shuffle(this.records);
    }
    /**
     * 同步方法获得GPSRecord
     */
    public synchronized GPSBean getNextGPSBeanForInsert() {
        GPSBean gpsBean = null;
        if (count < records.size()) {
            gpsBean = records.get(count++);
        }
        if (gpsBean == null) {
            isFinshed = true;
        }
        return gpsBean;
    }

    public boolean isFinished() {
        return isFinshed;
    }
}

class ByteBufferGet {
    private ByteBuffer byteBuffer;
    private int count;
    private volatile boolean isFinshed;
    private List<ParaGPSRecord> records;

    public ByteBufferGet(ByteBuffer buffer) {
        this.byteBuffer = buffer;
        this.count = 0;
        this.isFinshed = false;
        this.records = new ArrayList<>();
    }

    public void readAndShuffle() {
        ParaGPSRecord gpsRecord = null;
        while (byteBuffer.hasRemaining()) {
            gpsRecord = new ParaGPSRecord();
            gpsRecord.setDevicesn(byteBuffer.getLong());
            gpsRecord.setGpstime(byteBuffer.getLong());
            gpsRecord.setLongitude((float) Double.longBitsToDouble(byteBuffer.getLong()));
            gpsRecord.setLatitude((float) Double.longBitsToDouble(byteBuffer.getLong()));
            gpsRecord.setRowKey(Long.toString(gpsRecord.getDevicesn()) + Long.toString(gpsRecord.getGpstime()));
            records.add(gpsRecord);
        }
        Collections.shuffle(records);
    }

    /**
     * 同步方法获得GPSRecord
     */
    public synchronized ParaGPSRecord getNextGPSRecordForInsert() {
        ParaGPSRecord gpsRecord = null;
        if (count < records.size()) {
            gpsRecord = records.get(count++);
        }
        if (gpsRecord == null) {
            isFinshed = true;
        }
        return gpsRecord;
    }

    public boolean isFinished() {
        return isFinshed;
    }
}

class GPSBean {
    String rowKey;
    float longitude;
    float latitude;
    long timestamp;
//    int index;

    public GPSBean(String rowKey, float longitude, float latitude, long timestamp) {
        this.rowKey = rowKey;
        this.longitude = longitude;
        this.latitude = latitude;
        this.timestamp = timestamp;
//        this.index = (int )((timestamp - Constants.STARTTIME) / 3600 % Constants.N);
    }
}
