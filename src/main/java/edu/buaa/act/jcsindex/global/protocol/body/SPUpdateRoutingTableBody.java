/*
 * @(#) SPUpdateRoutingTableBody.java 1.0 2006-2-22
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.protocol.body;

import edu.buaa.act.jcsindex.global.peer.info.LogicalInfo;
import edu.buaa.act.jcsindex.global.peer.info.PhysicalInfo;
import edu.buaa.act.jcsindex.global.peer.info.RoutingItemInfo;

import java.io.Serializable;

/**
 * Implement the message body used for updating the routing table of a super peer.
 * 
 * @author Vu Quang Hieu
 * @version 1.0 2006-2-22
 */

public class SPUpdateRoutingTableBody extends Body implements Serializable 
{

	// private members
	private static final long serialVersionUID = -1846269889597852678L;

	private PhysicalInfo physicalSender;
	private LogicalInfo logicalSender;
	private RoutingItemInfo nodeInfo;
	private LogicalInfo logicalDestination;

	/**
	 * Construct the message body used for updating the routing table of a super peer.
	 * 
	 * @param physicalSender physical address of the sender
	 * @param logicalSender logical address of the sender
	 * @param nodeInfo information of the changed node
	 * @param logicalDestination logical address of the receiver
	 */
	public SPUpdateRoutingTableBody(PhysicalInfo physicalSender, LogicalInfo logicalSender,
									RoutingItemInfo nodeInfo, LogicalInfo logicalDestination)
	{
		this.physicalSender = physicalSender;
		this.logicalSender = logicalSender;
		this.nodeInfo = nodeInfo;
		this.logicalDestination = logicalDestination;
	}

	/**
	 * Construct the message body with a string value.
	 * 
	 * @param serializeData the string value that contains 
	 * the serialized message body
	 */
	public SPUpdateRoutingTableBody(String serializeData)
	{
		String[] arrData = serializeData.split(":");
		try
		{
			this.physicalSender = new PhysicalInfo(arrData[1]);
			this.logicalSender = new LogicalInfo(arrData[2]);
			this.nodeInfo = new RoutingItemInfo(arrData[3]);
			this.logicalDestination = new LogicalInfo(arrData[4]);
		}
		catch(Exception e)
		{
			System.out.println("Incorrect serialize data at UpdateRoutingTable:" + serializeData);
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
	 * Get information of the changed node
	 * 
	 * @return information of the changed node
	 */
	public RoutingItemInfo getNodeInfo()
	{
		return this.nodeInfo;
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

		outMsg = "UPDATEROUTINGTABLE";
		outMsg += "\n\t Physical Sender:" + physicalSender.toString();
		outMsg += "\n\t Logical Sender:" + logicalSender.toString();
		outMsg += "\n\t Node Info:" + nodeInfo.toString();
		outMsg += "\n\t Logical Destination:" + logicalDestination.toString();

		return outMsg;
	}

	@Override
	public String toString()
	{
		String outMsg;

		outMsg = "UPDATEROUTINGTABLE";
		outMsg += ":" + physicalSender.toString();
		outMsg += ":" + logicalSender.toString();
		outMsg += ":" + nodeInfo.toString();
		outMsg += ":" + logicalDestination.toString();

		return outMsg;
	}
	
}