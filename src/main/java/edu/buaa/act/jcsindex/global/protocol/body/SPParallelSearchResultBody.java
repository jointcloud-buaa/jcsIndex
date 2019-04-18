package edu.buaa.act.jcsindex.global.protocol.body;

import edu.buaa.act.jcsindex.global.peer.info.JcsTuple;
import edu.buaa.act.jcsindex.global.peer.info.LogicalInfo;
import edu.buaa.act.jcsindex.global.peer.info.PhysicalInfo;

import java.io.Serializable;
import java.util.List;

/**
 * Created by shmin at 4/18/2019 10:33 PM
 **/
public class SPParallelSearchResultBody extends Body implements Serializable {
    // private members
    private static final long serialVersionUID = 8463152687562574898L;

    private PhysicalInfo physicalSender;
    private LogicalInfo logicalSender;
    private List<String> dests;
    private LogicalInfo  logicalDestination;

    /**
     * Construct the message body with specified parameters.
     *
     * @param physicalSender physical address of the sender
     * @param logicalSender logical address of the sender
     * @param dests dests of node
     * @param logicalDestination logical address of the receiver
     */
    public SPParallelSearchResultBody(PhysicalInfo physicalSender, LogicalInfo logicalSender,
                            List<String> dests, LogicalInfo logicalDestination)
    {
        this.physicalSender = physicalSender;
        this.logicalSender = logicalSender;
        this.dests = dests;
        this.logicalDestination = logicalDestination;
    }

    public List<String> getDests() {
        return dests;
    }

    public void setDests(List<String> dests) {
        this.dests = dests;
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

