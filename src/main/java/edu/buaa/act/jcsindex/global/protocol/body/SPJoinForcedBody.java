/*
 * @(#) SPJoinForceBody.java 1.0 2006-2-22
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.protocol.body;

import edu.buaa.act.jcsindex.global.peer.info.LogicalInfo;
import edu.buaa.act.jcsindex.global.peer.info.PhysicalInfo;

import java.io.Serializable;

/**
 * Implement the message body used for forcing a super peer
 * node joins as a child of another super peer node. This
 * protocol is only used during network restructuring process
 * 
 * @author Vu Quang Hieu
 * @version 1.0 2006-2-22
 */

public class SPJoinForcedBody extends Body implements Serializable 
{

	// private members
	private static final long serialVersionUID = 4242320776590609176L;
	
	private PhysicalInfo physicalSender;
	private boolean direction;
	private LogicalInfo logicalDestination;

	/**
	 * Construct the message body with the specified parameters.
	 * 
	 * @param physicalSender physical address of the sender
	 * @param direction <code>true</code>: node is forced to
	 * join as a right child of another node, <code>false</code>:
	 * node is forced to join as a left child
	 * @param logicalDestination logical address of the receiver
	 */
	public SPJoinForcedBody(PhysicalInfo physicalSender, boolean direction, 
			LogicalInfo logicalDestination) 
	{
		this.physicalSender = physicalSender;
		this.direction = direction;
		this.logicalDestination = logicalDestination;
	}

	/**
	 * Construct the message body with a string value.
	 * 
	 * @param serializeData the string value that contains 
	 * the serialized message body
	 */
	public SPJoinForcedBody(String serializeData)
	{
		String[] arrData = serializeData.split(":");
		try
		{
			this.physicalSender = new PhysicalInfo(arrData[1]);
			this.direction = Boolean.valueOf(arrData[2]).booleanValue();
			this.logicalDestination = new LogicalInfo(arrData[3]);
		}
		catch(Exception e)
		{
			System.out.println("Incorrect serialize data at JoinForced:" + serializeData);
			System.out.println(e.getMessage());
		}
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
	 * Get direction
	 * 
	 * @return direction
	 */
	public boolean getDirection()
	{
		return this.direction;
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

		outMsg = "JOINFORCED";
		outMsg += "\n\t Physical Sender:" + physicalSender.toString();
		outMsg += "\n\t Direction:" + String.valueOf(direction);
		outMsg += "\n\t Logical Destination:" + logicalDestination.toString();

		return outMsg;
	}

	@Override
	public String toString()
	{
		String outMsg;

		outMsg = "JOINFORCED";
		outMsg += ":" + physicalSender.toString();
		outMsg += ":" + String.valueOf(direction);
		outMsg += ":" + logicalDestination.toString();

		return outMsg;
	}
	
}
