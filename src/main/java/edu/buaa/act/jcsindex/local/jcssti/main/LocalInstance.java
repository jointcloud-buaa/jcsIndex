package edu.buaa.act.jcsindex.local.jcssti.main;

import edu.buaa.act.jcsindex.local.jcssti.Constants;
import edu.buaa.act.jcsindex.local.jcssti.index.node.DataNodeImpl;
import edu.buaa.act.jcsindex.local.jcssti.index.node.rmi.DataNodeSkeleton;
import edu.buaa.act.jcsindex.local.jcssti.index.grid.Grid;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by shimin at 2018/7/6 0:59
 **/
public class LocalInstance {
    public void start(int type, String serverName, String ip) throws Exception {
        // 获取Configuration
        Configuration conf = getConfiguration(type);

        // 获取Hbase Connection的poolSize
        int poolSize = Constants.HBASE_POOL_SIZE;

        // 获取Hbase的tableName
        String tableName = Constants.HBASE_TABLE_NAME;


        // 获取GridInstance
        Grid gridInstance = new Grid(Constants.MIN_LONGTITUDE, Constants.MIN_LATITUDE, Constants.MAX_LONGTITUDE, Constants.MAX_LATITUDE, Constants.GRID_SIZE_X, Constants.GRID_SIZE_Y);

        // 索引存储路径
        String dbPath = Constants.DB_PATH;

        // 索引项个数
        int N = Constants.N;

        // 初始化DataNodeImpl
        DataNodeImpl dataNode = new DataNodeImpl(conf, poolSize, tableName, serverName, ip, type, gridInstance, dbPath, N);


        // 初始化Sketlon线程并启动
        new Thread(new DataNodeSkeleton(dataNode, ip, Constants.NODE_PORT)).start();

        // 初始化CMDServer线程并启动
        new Thread(new CMDServer(dataNode)).start();
    }

    public static void main(String[] args) throws Exception{
        // new LocalInstance().start(1, "test55", "192.168.7.55");
        BigInteger l = new BigInteger("0");
        BigInteger r = new BigInteger("0286319677901368209223372035402814804");
        BigInteger ans = l.add(r);
        System.out.println(ans.toString());
        BigInteger divider = new BigInteger("2");
        ans = ans.divide(divider);
        System.out.println(ans.toString());
    }

    private static Configuration getConfiguration(int type) {
        if (type == 1) {
            // 192.168.7.55集群
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "192.168.7.55");
            conf.set("hbase.zookeeper.property.clientPort", Constants.ZK_PORT);
            conf.setLong("hbase.client.scanner.caching", Constants.SCANNER_CACHING);
            conf.setInt("hbase.rpc.timeout",Constants.RPC_TIMEOUT);
            conf.setInt("hbase.client.operation.timeout",Constants.RPC_TIMEOUT + Constants.RPC_TIMEOUT / 2);
            conf.setInt("hbase.client.scanner.timeout.period",Constants.RPC_TIMEOUT);
            return conf;
        } else if (type == 2) {
            // 192.168.7.50集群
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "192.168.7.51");
            conf.set("hbase.zookeeper.property.clientPort", Constants.ZK_PORT);
            conf.setLong("hbase.client.scanner.caching", Constants.SCANNER_CACHING);
            conf.setInt("hbase.rpc.timeout",Constants.RPC_TIMEOUT);
            conf.setInt("hbase.client.operation.timeout",Constants.RPC_TIMEOUT + Constants.RPC_TIMEOUT / 2);
            conf.setInt("hbase.client.scanner.timeout.period",Constants.RPC_TIMEOUT);
            return conf;
        } else if (type == 3) {
            // 192.168.7.60集群
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "192.168.7.60");
            conf.set("hbase.zookeeper.property.clientPort", Constants.ZK_PORT);
            conf.setLong("hbase.client.scanner.caching", Constants.SCANNER_CACHING);
            conf.setInt("hbase.rpc.timeout",Constants.RPC_TIMEOUT);
            conf.setInt("hbase.client.operation.timeout",Constants.RPC_TIMEOUT + Constants.RPC_TIMEOUT / 2);
            conf.setInt("hbase.client.scanner.timeout.period",Constants.RPC_TIMEOUT);
            return conf;
        } else {
            // TODO: 需要注意，可能存在问题
            return null;
        }
    }

    private static String getServerName() {
        try {
            return (InetAddress.getLocalHost()).getHostName();
        } catch (UnknownHostException e) {
            String host = e.getMessage();
            if (host != null) {
                int colon = host.indexOf(":");
                if (colon > 0) {
                    return host.substring(0, colon);
                }
            }
            // TODO: 测试时返回test55
            return "test55";
        }
    }
}
