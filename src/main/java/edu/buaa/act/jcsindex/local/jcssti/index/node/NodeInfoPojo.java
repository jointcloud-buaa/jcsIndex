package edu.buaa.act.jcsindex.local.jcssti.index.node;

/**
 * Created by shimin at 2018/7/6 0:36
 **/
public class NodeInfoPojo {
    private String nodeHost;
    private int nodePort;

    public NodeInfoPojo(String nodeHost, int nodePort) {
        this.nodeHost = nodeHost;
        this.nodePort = nodePort;
    }

    public void setNodeHost(String nodeHost) {
        this.nodeHost = nodeHost;
    }

    public void setNodePort(int nodePort) {
        this.nodePort = nodePort;
    }

    public String getNodeHost() {
        return nodeHost;
    }

    public int getNodePort() {
        return nodePort;
    }

    @Override
    public String toString() {
        return nodeHost + ":" + nodePort;
    }
}
