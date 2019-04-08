package edu.buaa.act.jcsindex.local.jcssti.index.node.rmi;

/**
 * Created by shmin at 2018/7/6 0:06
 **/

import edu.buaa.act.jcsindex.local.jcssti.index.node.DataNodeImpl;
import edu.buaa.act.jcsindex.local.bean.ParaGPSRecord;
import edu.buaa.act.jcsindex.local.bean.ParaRectangle;

import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;

/**
 * Created by shmin at 2018/6/29 0:36
 **/
public class DataNodeSkeleton extends UnicastRemoteObject implements IOperator, Runnable{

    private static final long serialVersionUID = -8127167536274311112L;
    private final DataNodeImpl dataNode;
    private final String nodeHost;
    private final int nodePort;
    private final String serverName;

    public DataNodeSkeleton(final DataNodeImpl dataNode, final String nodeHost, int nodePort) throws RemoteException {
        this.dataNode = dataNode;
        this.nodeHost = nodeHost;
        this.nodePort = nodePort;
        this.serverName = "rmi://" + nodeHost + ":" + nodePort + "/operator";
    }

    @Override
    public void run() {
        try {
            LocateRegistry.createRegistry(nodePort);
            Naming.bind(serverName, this);
            System.out.println("DataNode" + " listen on " + nodeHost + ":" + nodePort);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    /**
     * 该函数仅用于测试时使用
     * @param time
     * @return
     */
    @Override
    public List<ParaGPSRecord> rangeQuery(int time) {
        return dataNode.rangeQuery(time);
    }

    /**
     * 实际调用函数，时空范围查询
     * @param rectangle 空间范围
     * @param startTime 开始时间
     * @param endTime 结束时间
     * @return
     */
    @Override
    public List<ParaGPSRecord> rangeQuery(ParaRectangle rectangle, long startTime, long endTime) {
        return dataNode.rangeQuery(rectangle, startTime, endTime);
    }

    @Override
    public void insert() throws RemoteException{
        dataNode.insert();
    }

    @Override
    public void publishRange() throws RemoteException{
        dataNode.publishRange();
    }

    /**
     * 方便rmi服务器退出
     */
    public void exit() {
        try {
            Registry registry = LocateRegistry.getRegistry();
            registry.unbind(serverName);
            UnicastRemoteObject.unexportObject(this,false);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
