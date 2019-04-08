package edu.buaa.act.jcsindex.local.jcssti.index.node.rmi;

import edu.buaa.act.jcsindex.local.bean.ParaRectangle;
import edu.buaa.act.jcsindex.local.bean.ParaGPSRecord;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

/**
 * Created by shmin at 2018/6/29 0:33
 **/
public interface IOperator extends Remote {
    /**
     * 该函数仅用于测试时使用
     */
    List<ParaGPSRecord> rangeQuery(int time) throws RemoteException;

    /**
     * 实际调用函数
     * @param rectangle 空间范围
     * @param startTime 开始时间
     * @param endTime 结束时间
     * @return 返回点的集合
     */
    List<ParaGPSRecord> rangeQuery(ParaRectangle rectangle, long startTime, long endTime) throws RemoteException;

    /**
     * 以防万一，留下一个接口用来插入数据
     */
    void insert() throws RemoteException;

    /**
     * 发布索引摘要
     */
    void publishRange() throws RemoteException;
}
