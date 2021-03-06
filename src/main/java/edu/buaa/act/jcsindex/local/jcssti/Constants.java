package edu.buaa.act.jcsindex.local.jcssti;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by shmin at 2018/7/16 23:58
 **/
public class Constants {
    public static final byte[] GPSCF = Bytes.toBytes("G");
    public static final byte[] GPSCF_DEVICESN = Bytes.toBytes("devicesn");
    public static final byte[] GPSCF_LONGTITUDE = Bytes.toBytes("longitude");
    public static final byte[] GPSCF_LATITUDE = Bytes.toBytes("latitude");
    public static final byte[] GPSCF_SPEED = Bytes.toBytes("speed");
    public static final byte[] GPSCF_DIRECTION = Bytes.toBytes("direction");
    public static final byte[] GPSCF_TIMESTAMP = Bytes.toBytes("timestamp");

    // TODO: JUST FOR TEST
    public static final String NODE_HOST = "127.0.0.1";
    public static final int NODE_PORT = 9090;

    public static final String DB_PATH = "D:\\workspace\\jointcloudstorage\\localIndex\\BPlusTree\\DB";

    public static final int N = 744;

    public static final float MIN_LONGTITUDE = 115.542991693034F;
    public static final float MAX_LONGTITUDE = 116.848701941655F;
    public static final float MIN_LATITUDE = 39.523733567851F;
    public static final float MAX_LATITUDE = 40.3485250469718F;
    public static final int GRID_SIZE_X = 1000;
    public static final int GRID_SIZE_Y = 1000;

    public static final int HBASE_POOL_SIZE = 4;
    public static final String HBASE_TABLE_NAME = "carstest";

    public static final String ZK_IP = "192.168.7.55";
    public static final String ZK_PORT = "2181";
    public static final int RPC_TIMEOUT = 60000;
    public static final int SCANNER_CACHING = 1000;
}
