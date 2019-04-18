package edu.buaa.act.jcsindex.global.protocol.body;

import edu.buaa.act.jcsindex.global.peer.info.JcsTuple;
import edu.buaa.act.jcsindex.global.peer.info.LogicalInfo;
import edu.buaa.act.jcsindex.global.peer.info.PhysicalInfo;

import java.io.Serializable;

/**
 * Created by shmin at 4/11/2019 4:23 PM
 **/
public class SPUpdateTagBody extends Body implements Serializable {
    // private members
    private static final long serialVersionUID = 8563152697562474898L;

    private PhysicalInfo physicalSender;
    private LogicalInfo logicalSender;
    private JcsTuple tuple;
    private LogicalInfo  logicalDestination;

    /**
     * Construct the message body with specified parameters.
     *
     * @param physicalSender physical address of the sender
     * @param logicalSender logical address of the sender
     * @param tuple tagValue
     * @param logicalDestination logical address of the receiver
     */
    public SPUpdateTagBody(PhysicalInfo physicalSender, LogicalInfo logicalSender,
                         JcsTuple tuple, LogicalInfo logicalDestination)
    {
        this.physicalSender = physicalSender;
        this.logicalSender = logicalSender;
        this.tuple = tuple;
        this.logicalDestination = logicalDestination;
    }

    public SPUpdateTagBody(SPPublishBody body) {
        this.physicalSender = body.getPhysicalSender();
        this.logicalSender = body.getLogicalSender();
        this.tuple = body.getTuple();
        this.logicalDestination = body.getLogicalDestination();
    }

    public SPUpdateTagBody(SPPublishParentBody body) {
        this.physicalSender = body.getPhysicalSender();
        this.logicalSender = body.getLogicalSender();
        this.tuple = body.getTuple();
        this.logicalDestination = body.getLogicalDestination();
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