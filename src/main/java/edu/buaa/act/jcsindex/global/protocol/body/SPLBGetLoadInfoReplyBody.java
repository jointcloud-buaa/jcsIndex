/*
 * @(#) SPLBGetLoadInfoReplyBody.java 1.0 2006-2-22
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.protocol.body;

import edu.buaa.act.jcsindex.global.peer.info.LogicalInfo;
import edu.buaa.act.jcsindex.global.peer.info.PhysicalInfo;

import java.io.Serializable;

/**
 * Implement the message body used for replying workload
 * information of a super peer to the requester 
 * 
 * @author Vu Quang Hieu
 * @version 1.0 2006-2-22
 */

public class SPLBGetLoadInfoReplyBody extends Body implements Serializable 
{

	// private members
	private static final long serialVersionUID = -3445922255968248352L;
	
	private PhysicalInfo physicalSender;
	private LogicalInfo logicalSender;
	private int numOfElement;
	private int order;
	private boolean direction;
	private LogicalInfo logicalDestination;

	/**
	 * Construct the message body with speicified parameters.
	 * 
	 * @param physicalSender physical address of the sender
	 * @param logicalSender logical address of the sender
	 * @param numOfElement number of stored item
	 * @param order order of stored item
	 * @param direction direction of sending request
	 * @param logicalDestination logical address of the receiver
	 */
	public SPLBGetLoadInfoReplyBody(PhysicalInfo physicalSender, LogicalInfo logicalSender,
									int numOfElement, int order, boolean direction,
									LogicalInfo logicalDestination) 
	{
		this.physicalSender = physicalSender;
		this.logicalSender = logicalSender;
		this.numOfElement = numOfElement;
		this.order = order;
		this.direction = direction;
		this.logicalDestination = logicalDestination;
	}

	/**
	 * Construct the message body with a string value.
	 * 
	 * @param serializeData the string value that contains 
	 * the serialized message body
	 */
	public SPLBGetLoadInfoReplyBody(String serializeData)
	{
		String[] arrData = serializeData.split(":");
		try
		{
			this.physicalSender = new PhysicalInfo(arrData[1]);
			this.logicalSender = new LogicalInfo(arrData[2]);
			this.numOfElement = Integer.parseInt(arrData[3]);
			this.order = Integer.parseInt(arrData[4]);
			this.direction = Boolean.valueOf(arrData[5]).booleanValue();
			this.logicalDestination = new LogicalInfo(arrData[6]);
		}
		catch(Exception e)
		{
			System.out.println("Incorrect serialize data at LBGetLoadInfoReply:" + serializeData);
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
	 * Get number of stored item at the node
	 * 
	 * @return number of stored item at the node
	 */
	public int getNumOfElement()
	{
		return this.numOfElement;
	}

	/**
	 * Get order of the node
	 * 
	 * @return order of the node
	 */
	public int getOrder()
	{
		return this.order;
	}

	/**
	 * Get direction of sending request
	 * 
	 * @return direction of sending request
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

		outMsg = "LBGETLOADINFOREPLY";
		outMsg += "\n\t Physical Sender:" + physicalSender.toString();
		outMsg += "\n\t Logical Sender:" + logicalSender.toString();
		outMsg += "\n\t Num of element:" + numOfElement;
		outMsg += "\n\t Order:" + order;
		outMsg += "\n\t Direction:" + direction;
		outMsg += "\n\t Logical Destination:" + logicalDestination.toString();

		return outMsg;
	}

	@Override
	public String toString()
	{
		String outMsg;

		outMsg = "LBGETLOADINFOREPLY";
		outMsg += ":" + physicalSender.toString();
		outMsg += ":" + logicalSender.toString();
		outMsg += ":" + numOfElement;
		outMsg += ":" + order;
		outMsg += ":" + direction;
		outMsg += ":" + logicalDestination.toString();

		return outMsg;
	}
	
}
