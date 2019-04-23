package edu.buaa.act.jcsindex.global.protocol.body;

import edu.buaa.act.jcsindex.global.peer.info.JcsTuple;
import edu.buaa.act.jcsindex.global.peer.info.LogicalInfo;
import edu.buaa.act.jcsindex.global.peer.info.PhysicalInfo;

import java.io.Serializable;
import java.util.List;

/**
 * Created by shmin at 4/11/2019 10:18 PM
 **/
public class SPSearchParentBody extends Body implements Serializable {
    // private members
    private static final long serialVersionUID = 8463152637562874898L;

    private PhysicalInfo physicalSender;
    private LogicalInfo logicalSender;
    private PhysicalInfo physicalRequester;
    private LogicalInfo logicalRequester;
    private String requestId;
    // TODO: 只能对应一个时段的Search
    private JcsTuple tuple;
    private List<String> dests;
    private LogicalInfo  logicalDestination;

    /**
     * Construct the message body with specified parameters.
     *
     * @param physicalSender physical address of the sender
     * @param logicalSender logical address of the sender
     * @param tuple tuple item wanted to insert
     * @param logicalDestination logical address of the receiver
     */
    public SPSearchParentBody(PhysicalInfo physicalSender, LogicalInfo logicalSender, PhysicalInfo physicalRequester, LogicalInfo logicalRequester,
                            JcsTuple tuple, List<String> dests, LogicalInfo logicalDestination)
    {
        this.physicalSender = physicalSender;
        this.logicalSender = logicalSender;
        this.physicalRequester = physicalRequester;
        this.logicalRequester = logicalRequester;
        this.tuple = tuple;
        this.dests = dests;
        this.logicalDestination = logicalDestination;
    }

    public SPSearchParentBody(PhysicalInfo physicalSender, LogicalInfo logicalSender, PhysicalInfo physicalRequester, LogicalInfo logicalRequester, String requestId,
                              JcsTuple tuple, List<String> dests, LogicalInfo logicalDestination)
    {
        this.physicalSender = physicalSender;
        this.logicalSender = logicalSender;
        this.physicalRequester = physicalRequester;
        this.logicalRequester = logicalRequester;
        this.requestId = requestId;
        this.tuple = tuple;
        this.dests = dests;
        this.logicalDestination = logicalDestination;
    }

    public SPSearchParentBody(SPParallelSearchBody body) {
        this.physicalSender = body.getPhysicalSender();
        this.logicalSender = body.getLogicalSender();
        this.physicalRequester = body.getPhysicalRequester();
        this.logicalRequester = body.getLogicalRequester();
        this.requestId = body.getRequestId();
        this.tuple = body.getTuple();
        this.logicalDestination = body.getLogicalDestination();
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public List<String> getDests() {
        return dests;
    }

    public void setDests(List<String> dests) {
        this.dests = dests;
    }

    public PhysicalInfo getPhysicalRequester() {
        return physicalRequester;
    }

    public void setPhysicalRequester(PhysicalInfo physicalRequester) {
        this.physicalRequester = physicalRequester;
    }

    public LogicalInfo getLogicalRequester() {
        return logicalRequester;
    }

    public void setLogicalRequester(LogicalInfo logicalRequester) {
        this.logicalRequester = logicalRequester;
    }

    /**
     * Update physical address of the sender
     *
     * @param physicalSender physical address of the sender
     */
    public void setPhysicalSender(PhysicalInfo physicalSender)
    {
        this.physicalSender = physicalSender;
    }

    /**
     * Get physical address of the sender
     *
     * @return physical address of the sender
     */
    public PhysicalInfo getPhysicalSender()
    {
        return this.physicalSender;
    }

    /**
     * Update logical address of the sender
     *
     * @param logicalSender logical address of the sender
     */
    public void setLogicalSender(LogicalInfo logicalSender)
    {
        this.logicalSender = logicalSender;
    }

    /**
     * Get logical address of the sender
     *
     * @return logical address of the sender
     */
    public LogicalInfo getLogicalSender()
    {
        return this.logicalSender;
    }

    /**
     * Get tuple item wanted to insert
     *
     * @return tuple item wanted to insert
     */
    public JcsTuple getTuple()
    {
        return this.tuple;
    }

    /**
     * Update logical address of the receiver
     *
     * @param logicalDestination logical address of the receiver
     */
    public void setLogicalDestination(LogicalInfo logicalDestination)
    {
        this.logicalDestination = logicalDestination;
    }

    /**
     * Get logical address of the receiver
     *
     * @return logical address of the receiver
     */
    public LogicalInfo getLogicalDestination()
    {
        return this.logicalDestination;
    }


    @Override
    public String toString()
    {
        String outMsg;

        outMsg = "INSERT";
        outMsg += ":" + physicalSender.toString();
        if (logicalSender == null)
        {
            outMsg += ":null";
        }
        else
        {
            outMsg += ":" + logicalSender.toString();
        }

        outMsg += ":" + tuple.toString();
        if (logicalDestination == null)
        {
            outMsg += ":null";
        }
        else
        {
            outMsg += ":" + logicalDestination.toString();
        }
        return outMsg;
    }
}
