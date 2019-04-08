package edu.buaa.act.jcsindex.local.utils;

import org.apache.hadoop.hbase.util.Bytes;

import java.sql.Timestamp;
import java.util.Date;

/**
 * Created by shmin at 2018/5/10 21:59
 **/
public class Utils {
    /*
     * rowkey的设计 反转的devicesn后六位+devicesn+ long最大值减去当前时间戳
     */

    public static String getDecPrefix(String devicesn){
        StringBuffer sb = new StringBuffer(devicesn.substring(6,devicesn.length()));
        return sb.reverse().toString();
    }


    public static byte[] generateRowkeyPM(long time, String devicesn) {
        return Bytes.toBytes(Utils.getDecPrefix(devicesn) + devicesn
                + Long.toString(Long.MAX_VALUE - time));
    }
    public static long getTimeStamp(String dateString) {
        Timestamp ts = new Timestamp(System.currentTimeMillis());
        ts = Timestamp.valueOf(dateString);
        // format timestamp from milliseonds to seconds
        return ts.getTime() / 1000;

    }

    public static String getDate(long timestamp) {
        Date date = new Date(timestamp * 1000);
        return date.toString();
    }
    public static void main(String[] args) {
        System.out.println(new String(generateRowkeyPM(getTimeStamp("2017-03-23 17:30:00"), "967790223393")));
        System.out.println(new String(generateRowkeyPM(getTimeStamp("2017-03-23 19:00:00"), "967790223393")));
        System.out.println(Utils.getTimeStamp("2015-01-01 00:00:00.000"));
    }
}

