/*
 * @(#) SPUpdateMaxMinValueBody.java 1.0 2006-2-22
 * 
 * Copyright 2006, National University of Singapore.
 * All right reserved.
 */

package edu.buaa.act.jcsindex.global.protocol.body;

import edu.buaa.act.jcsindex.global.peer.info.LogicalInfo;
import edu.buaa.act.jcsindex.global.peer.info.PhysicalInfo;
import edu.buaa.act.jcsindex.global.peer.info.RoutingItemInfo;

import java.io.Serializable;

/**
 * Implement the message body used for updating the maximal 
 * and minimal index value of the index range of a neighbor node
 * 
 * @author Vu Quang Hieu
 * @version 1.0 2006-2-22
 */

public class SPUpdateMaxMinValueBody extends Body implements Serializable 
{

	// private members
	private static final long serialVersionUID = -3665744018850878365L;
	
	private PhysicalInfo physicalSender;
	private LogicalInfo logicalSender;
	private RoutingItemInfo nodeInfo;
	private int index;
	private boolean direction;
	private LogicalInfo logicalDestination;

	/**
	 * Construct the message body with specified parameters. 
	 * 
	 * @param physicalSender physical address of the sender
	 * @param logicalSender logical address of the sender
	 * @param nodeInfo information of a neighbor node
	 * @param index position of the neighbor node inside the routing table
	 * @param direction direction of the changed node
	 * @param logicalDestination logical address of the receiver
	 */
	public SPUpdateMaxMinValueBody(PhysicalInfo physicalSender, LogicalInfo logicalSender,
									RoutingItemInfo nodeInfo, int index,
									boolean direction, LogicalInfo logicalDestination)
	{
		this.physicalSender = physicalSender;
		this.logicalSender = logicalSender;
		this.nodeInfo = nodeInfo;
		this.index = index;
		this.direction = direction;
		this.logicalDestination = logicalDestination;
	}

	/**
	 * Construct the message body with a string value.
	 * 
	 * @param serializeData the string value that contains 
	 * the serialized message body
	 */
	public SPUpdateMaxMinValueBody(String serializeData)
	{
		String[] arrData = serializeData.split(":");
		try
		{
			this.physicalSender = new PhysicalInfo(arrData[1]);
			this.logicalSender = new LogicalInfo(arrData[2]);
			this.nodeInfo = new RoutingItemInfo(arrData[3]);
			this.index = Integer.parseInt(arrData[4]);
			this.direction = Boolean.valueOf(arrData[5]).booleanValue();
			this.logicalDestination = new LogicalInfo(arrData[6]);
		}
		catch(Exception e)
		{
			System.out.println("Incorrect serialize data at LBSplitData:" + serializeData);
			System.out.println(e.getMessage());
		}
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
	 * Get logical address of the sender
	 * 
	 * @return logical address of the sender
	 */
	public LogicalInfo getLogicalSender()
	{
		return this.logicalSender;
	}

	/**
	 * Get information of a neighbor node
	 * 
	 * @return information of a neighbor node
	 */
	public RoutingItemInfo getNodeInfo()
	{
		return this.nodeInfo;
	}

	/**
	 * Get index of the neighbor node inside routing table
	 * 
	 * @return index of the neighbor node
	 */
	public int getIndex()
	{
		return this.index;
	}

	/**
	 * Get direction of the changed node
	 * 
	 * @return direction of the changed node
	 */
	public boolean getDirection()
	{
		return this.direction;
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

		outMsg = "UPDATEMAXMINVALUE";
		outMsg += "\n\t Physical Sender:" + physicalSender.toString();
		outMsg += "\n\t Logical Sender:" + logicalSender.toString();
		outMsg += "\n\t Node Info:" + nodeInfo.toString();
		outMsg += "\n\t Index:" + index;
		outMsg += "\n\t Direction:" + direction;
		outMsg += "\n\t Logical Destination:" + logicalDestination.toString();

		return outMsg;
	}

	@Override
	public String toString()
	{
		String outMsg;

		outMsg = "UPDATEMAXMINVALUE";
		outMsg += ":" + physicalSender.toString();
		outMsg += ":" + logicalSender.toString();
		outMsg += ":" + nodeInfo.toString();
		outMsg += ":" + index;
		outMsg += ":" + direction;
		outMsg += ":" + logicalDestination.toString();

		return outMsg;  
	}
	
}
