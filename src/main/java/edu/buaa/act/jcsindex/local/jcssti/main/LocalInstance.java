package edu.buaa.act.jcsindex.local.jcssti.main;

import edu.buaa.act.jcsindex.local.jcssti.Constants;
import edu.buaa.act.jcsindex.local.jcssti.index.node.DataNodeImpl;
import edu.buaa.act.jcsindex.local.jcssti.index.node.rmi.DataNodeSkeleton;
import edu.buaa.act.jcsindex.local.jcssti.index.grid.Grid;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by shimin at 2018/7/6 0:59
 **/
public class LocalInstance {
    public static void main(String[] args) throws Exception{
        // 获取Configuration
        Configuration conf = getConfiguration();

        // 获取Hbase Connection的poolSize
        int poolSize = Constants.HBASE_POOL_SIZE;

        // 获取Hbase的tableName
        String tableName = Constants.HBASE_TABLE_NAME;

        // 获取本机的主机名，后期可通过主机名获取本机相应的Region
        String serverName = getServerName();

        // 获取GridInstance
        Grid gridInstance = new Grid(Constants.MIN_LONGTITUDE, Constants.MIN_LATITUDE, Constants.MAX_LONGTITUDE, Constants.MAX_LATITUDE, Constants.GRID_SIZE_X, Constants.GRID_SIZE_Y);

        // 索引存储路径
        String dbPath = Constants.DB_PATH;

        // 索引项个数
        int N = Constants.N;

        // 初始化DataNodeImpl
        DataNodeImpl dataNode = new DataNodeImpl(conf, poolSize, tableName, serverName, gridInstance, dbPath, N);


        // 初始化Sketlon线程并启动
        new Thread(new DataNodeSkeleton(dataNode, Constants.NODE_HOST, Constants.NODE_PORT)).start();

        // 初始化CMDServer线程并启动
        new Thread(new CMDServer(dataNode)).start();
    }

    private static Configuration getConfiguration() {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", Constants.ZK_IP);
        conf.set("hbase.zookeeper.property.clientPort", Constants.ZK_PORT);
        conf.setLong("hbase.rpc.timeout", Constants.RPC_TIMEOUT);
        conf.setLong("hbase.client.scanner.caching", Constants.SCANNER_CACHING);
        return conf;
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
