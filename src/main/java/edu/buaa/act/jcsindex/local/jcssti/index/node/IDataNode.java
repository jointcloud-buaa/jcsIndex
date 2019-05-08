package edu.buaa.act.jcsindex.local.jcssti.index.node;

import edu.buaa.act.jcsindex.local.bean.ParaRectangle;
import edu.buaa.act.jcsindex.local.bean.ParaGPSRecord;

import java.util.List;

/**
 * Created by shmin at 2018/7/6 0:16
 **/
public interface IDataNode {
    /**
     * 实际调用函数
     * @param range
     * @param startTime
     * @param endTime
     * @return
     */
    List<ParaGPSRecord> rangeQuery(ParaRectangle range, long startTime, long endTime);

    List<ParaGPSRecord> rangeQuery(ParaRectangle rectangle, int timeIndex);

    /**
     * 该函数仅用于测试时使用
     * @param time
     * @return
     */
    List<ParaGPSRecord> rangeQuery(int time);

    // insert data
    void insert();

    // 发布索引摘要
    void publishRange();
}
