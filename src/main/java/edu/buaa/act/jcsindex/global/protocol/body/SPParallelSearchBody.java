package edu.buaa.act.jcsindex.global.protocol.body;

import edu.buaa.act.jcsindex.global.peer.info.JcsTuple;
import edu.buaa.act.jcsindex.global.peer.info.LogicalInfo;
import edu.buaa.act.jcsindex.global.peer.info.PhysicalInfo;

import java.io.Serializable;

/**
 * Created by shimin at 4/11/2019 9:05 PM
 **/
public class SPParallelSearchBody extends Body implements Serializable {
    // private members
    private static final long serialVersionUID = 8563152687562874898L;

    private PhysicalInfo physicalSender;
    private LogicalInfo logicalSender;
    private PhysicalInfo physicalRequester;
    private LogicalInfo logicalRequester;
    // TODO: 只能对应一个时段的Search
    private JcsTuple tuple;
    private LogicalInfo  logicalDestination;

    /**
     * Construct the message body with specified parameters.
     *
     * @param physicalSender physical address of the sender
     * @param logicalSender logical address of the sender
     * @param tuple tuple item wanted to insert
     * @param logicalDestination logical address of the receiver
     */
    public SPParallelSearchBody(PhysicalInfo physicalSender, LogicalInfo logicalSender, PhysicalInfo physicalRequester, LogicalInfo logicalRequester,
                         JcsTuple tuple, LogicalInfo logicalDestination)
    {
        this.physicalSender = physicalSender;
        this.logicalSender = logicalSender;
        this.physicalRequester = physicalRequester;
        this.logicalRequester = logicalRequester;
        this.tuple = tuple;
        this.logicalDestination = logicalDestination;
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

    /**
     * Return a readable string for testing or writing in the log file
     *
     * @return a readable string
     */
    public String getString()
    {
        String outMsg;

        outMsg = "PUBLISH";
        outMsg += "\n\t Physical Sender:" + physicalSender.toString();
        if (logicalSender == null)
        {
            outMsg += "\n\t Logical Sender:null";
        }
        else
        {
            outMsg += "\n\t Logical Sender:" + logicalSender.toString();
        }

        outMsg += "\n\t Physical Requester:" + physicalRequester.toString();

        if (logicalRequester == null) {
            outMsg += "\n\t Logical Requestor null";
        } else {
            outMsg += "\n\t Logical Requestor:" + logicalRequester.toString();
        }

        outMsg += "\n\t Tuple:" + tuple.toString();
        if (logicalDestination == null)
        {
            outMsg += "\n\t Logical Destination:null";
        }
        else
        {
            outMsg += "\n\t Logical Destination:" + logicalDestination.toString();
        }

        return outMsg;
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
