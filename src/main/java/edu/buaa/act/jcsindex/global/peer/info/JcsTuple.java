package edu.buaa.act.jcsindex.global.peer.info;

import java.io.Serializable;

/**
 * Created by shimin at 4/10/2019 10:19 PM
 **/
public class JcsTuple implements Comparable, Serializable {
    private static final long serialVersionUID = 8358627612268617718L;

    // 对应的time index
    private int timeIndex;
    // 左届
    private long leftBound;
    // 右届
    private long rightBound;
    // ip及端口信息
    private String dest;

    public JcsTuple(int timeIndex, long leftBound, long rightBound, String dest) {
        this.timeIndex = timeIndex;
        this.leftBound = leftBound;
        this.rightBound = rightBound;
        this.dest = dest;
    }

    public long getKey() {
        return leftBound;
    }

    public int compareTo(Object that) throws ClassCastException{
        if (that instanceof JcsTuple) {
            return ((Long ) this.leftBound).compareTo(((JcsTuple) that).leftBound);
        } else if (that instanceof BoundaryValue){
            BoundaryValue compareValue = (BoundaryValue )that;
            return ((Long ) this.leftBound).compareTo(compareValue.getLongValue());
        } else {
            throw new ClassCastException("JcsTuple compareTo");
        }
    }

    public int getTimeIndex() {
        return timeIndex;
    }

    public void setTimeIndex(int timeIndex) {
        this.timeIndex = timeIndex;
    }

    public long getLeftBound() {
        return leftBound;
    }

    public void setLeftBound(long leftBound) {
        this.leftBound = leftBound;
    }

    public long getRightBound() {
        return rightBound;
    }

    public void setRightBound(long rightBound) {
        this.rightBound = rightBound;
    }

    public String getDest() {
        return dest;
    }

    public void setDest(String dest) {
        this.dest = dest;
    }

    @Override
    public String toString() {
        String outMsg = "";
        outMsg += "(Destination: " + dest;
        outMsg += ", TimeIndex: " + timeIndex;
        outMsg += ", LeftBound: " + leftBound;
        outMsg += ", RightBound: " + rightBound;
        outMsg += ")";
        return  outMsg;
    }
}
