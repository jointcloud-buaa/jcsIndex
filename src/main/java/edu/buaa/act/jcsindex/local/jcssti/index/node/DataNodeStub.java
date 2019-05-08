package edu.buaa.act.jcsindex.local.jcssti.index.node;

import edu.buaa.act.jcsindex.local.bean.ParaRectangle;
import edu.buaa.act.jcsindex.local.bean.ParaGPSRecord;
import edu.buaa.act.jcsindex.local.jcssti.index.node.rmi.IOperator;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by shmin at 2018/7/6 0:33
 **/
public class DataNodeStub implements IDataNode {
    private IOperator operator;
    private static final int retryCount = 5;    // 连接失败时的重试次数
    private static final int sleepMsBetweenRetry = 2000;    // 连接失败时的重连间隔时长
    private NodeInfoPojo nodeInfoPojo;

    public DataNodeStub(NodeInfoPojo nodeInfoPojo) {
        this.nodeInfoPojo = nodeInfoPojo;

        // 开始连接到目标DataNode
        for (int i = 0; i < retryCount; i++) {
            try {
                operator = (IOperator) Naming.lookup("rmi://" + nodeInfoPojo.toString() + "/operator");
            } catch (MalformedURLException | RemoteException | NotBoundException e) {
                System.err.printf("连接DataNode [%s]失败，原因是: %s, 重试%d...\n", nodeInfoPojo.toString(), e.getMessage(), i);
                operator = null;
                try {
                    Thread.sleep(sleepMsBetweenRetry);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
        }
    }

    @Override
    public List<ParaGPSRecord> rangeQuery(ParaRectangle range, long startTime, long endTime) {
        try {
            return operator.rangeQuery(range, startTime, endTime);
        } catch (RemoteException e) {
            e.printStackTrace();
            // 异常时返回空的数组
            return new ArrayList<>();
        }
    }

    @Override
    public List<ParaGPSRecord> rangeQuery(ParaRectangle rectangle, int timeIndex) {
        try {
            return operator.rangeQuery(rectangle, timeIndex);
        } catch (RemoteException e) {
            e.printStackTrace();
            return new ArrayList<>();
        }
    }

    @Override
    public List<ParaGPSRecord> rangeQuery(int time) {
        try {
            return operator.rangeQuery(time);
        } catch (RemoteException e) {
            e.printStackTrace();
            // 异常时返回空的数组
            return new ArrayList<>();
        }
    }

    @Override
    public void insert(){
        try {
            operator.insert();
        } catch (RemoteException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void publishRange() {
        try {
            operator.publishRange();
        } catch (RemoteException e) {
            e.printStackTrace();
        }
    }
}
