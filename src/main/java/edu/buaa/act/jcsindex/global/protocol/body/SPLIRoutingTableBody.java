/*
 * @(#) SPLIRoutingTableBody.java 1.0 2006-12-03
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.protocol.body;

import edu.buaa.act.jcsindex.global.peer.info.LogicalInfo;
import edu.buaa.act.jcsindex.global.peer.info.PhysicalInfo;

import java.io.Serializable;

/**
 * Implement the message body used for looking routing table info in case of node failure
 * 
 * @author Vu Quang Hieu
 * @version 1.0 2006-12-03
 */

public class SPLIRoutingTableBody extends Body implements Serializable 
{
	
	// constant definition for direction
	public static final boolean FROM_LEFT_TO_RIGHT = false;
	public static final boolean FROM_RIGHT_TO_LEFT = true;
	
	private static final long serialVersionUID = -3857757600307329829L;

	private PhysicalInfo physicalSender;
	private LogicalInfo logicalSender;
	private PhysicalInfo physicalRequester;
	private LogicalInfo logicalRequester;
	private LogicalInfo logicalFailedNode;
	private int index;
	private boolean direction;
	private LogicalInfo logicalDestination;	
	
	/**
	 * Construct the message body with speicified parameters.
	 * 
	 * @param physicalSender physical address of the sender
	 * @param logicalSender logical address of the sender
	 * @param physicalRequester physical address of the requester
	 * @param logicalRequester logical address of the requester
	 * @param logicalFailedNode logical address of the failed node
	 * @param index position of the new node in the routing table
	 * @param direction direction of the new node
	 * @param logicalDestination logical address of the receiver
	 */
	public SPLIRoutingTableBody(PhysicalInfo physicalSender, 
			LogicalInfo logicalSender, PhysicalInfo physicalRequester, 
			LogicalInfo logicalRequester, LogicalInfo logicalFailedNode,
			int index, boolean direction, LogicalInfo logicalDestination)
	{
		this.physicalSender = physicalSender;
		this.logicalSender = logicalSender;
		this.physicalRequester = physicalRequester;
		this.logicalRequester = logicalRequester;
		this.logicalFailedNode = logicalFailedNode;
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
	public SPLIRoutingTableBody(String serializeData)
	{
		String[] arrData = serializeData.split(":");
		try
		{
			this.physicalSender = new PhysicalInfo(arrData[1]);
			this.logicalSender = new LogicalInfo(arrData[2]);	
			this.physicalRequester = new PhysicalInfo(arrData[3]);
			this.logicalRequester = new LogicalInfo(arrData[4]);
			this.logicalFailedNode = new LogicalInfo(arrData[5]);
			this.index = Integer.parseInt(arrData[6]);
			this.direction = Boolean.valueOf(arrData[7]).booleanValue();
			this.logicalDestination = new LogicalInfo(arrData[8]);
		}
		catch(Exception e)
		{
			System.out.println("Incorrect serialize data at LIRoutingTable:" + serializeData);
			System.out.println(e.getMessage());
		}
	}

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

	public PhysicalInfo getPhysicalRequester()
	{
		return this.physicalRequester;
	}
	
	public LogicalInfo getLogicalRequester()
	{
		return this.logicalRequester;
	}

	/**
	 * Get logical address of the failed node
	 * 
	 * @return logical address of the failed node
	 */
	public LogicalInfo getLogicalFailedNode()
	{
		return this.logicalFailedNode;
	}
	
	public void setIndex(int index)
	{
		this.index = index;
	}
	
	/**
	 * Get index of the new node inside routing table
	 * 
	 * @return index of the new node
	 */
	public int getIndex()
	{
		return this.index;
	}

	/**
	 * Get direction of the new node
	 * 
	 * @return direction of the new node
	 */
	public boolean getDirection()
	{
		return this.direction;
	}

	/**
	 * Set logical address of the receiver
	 * @param logicalDestination
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

		outMsg = "LIROUTINGTABLE";
		outMsg += "\n\t Physical Sender:" + physicalSender.toString();
		outMsg += "\n\t Logical Sender:" + logicalSender.toString();
		outMsg += "\n\t Physical Requester:" + physicalRequester.toString();
		outMsg += "\n\t Logical Requester:" + logicalRequester.toString();
		outMsg += "\n\t Logical Failed Node:" + logicalFailedNode.toString();
		outMsg += "\n\t Index:" + index;
		outMsg += "\n\t Direction:" + String.valueOf(direction);
		outMsg += "\n\t Logical Destination:" + logicalDestination.toString();

		return outMsg;  
	}

	@Override
	public String toString()
	{
		String outMsg;

		outMsg = "LIROUTINGTABLE";
		outMsg += ":" + physicalSender.toString();
		outMsg += ":" + logicalSender.toString();
		outMsg += ":" + physicalRequester.toString();
		outMsg += ":" + logicalRequester.toString();
		outMsg += ":" + logicalFailedNode.toString();
		outMsg += ":" + index;
		outMsg += ":" + String.valueOf(direction);
		outMsg += ":" + logicalDestination.toString();

		return outMsg;  
	}
}
