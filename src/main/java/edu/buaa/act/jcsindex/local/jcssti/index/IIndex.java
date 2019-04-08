package edu.buaa.act.jcsindex.local.jcssti.index;

import edu.buaa.act.jcsindex.local.bean.ParaGPSRecord;
import edu.buaa.act.jcsindex.local.bean.ParaRectangle;

import java.util.List;

/**
 * Created by shmin at 2018/7/5 23:24
 * 定义查询接口
 * NOTE：暂时只定义了一个接口，后续会定义Index的其他接口，暂时不需要该接口
 **/
public interface IIndex {
    List<ParaGPSRecord> rangeQuery(ParaRectangle range, long startTime, long endTime);
}
