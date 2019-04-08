/*
 * @(#) SPLeaveFindReplacementNodeReplyBody.java 1.0 2006-2-22
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.protocol.body;

import edu.buaa.act.jcsindex.global.peer.info.LogicalInfo;
import edu.buaa.act.jcsindex.global.peer.info.PhysicalInfo;

import java.io.Serializable;

/**
 * Implement the message body used for a node to reply the leaving 
 * node that it can come to replace the leaving node's position 
 *
 * @author Vu Quang Hieu
 * @version 1.0 2006-2-22
 */

public class SPLeaveFindReplacementNodeReplyBody extends Body implements Serializable 
{

	// private members
	private static final long serialVersionUID = -798447371485189153L;

	private PhysicalInfo physicalSender;
	private LogicalInfo logicalDestination;

	/**
	 * Construct the message body with specified parameters.
	 * 
	 * @param physicalSender physical address of the sender
	 * @param logicalDestination logical address of the receiver
	 */
	public SPLeaveFindReplacementNodeReplyBody(PhysicalInfo physicalSender, 
			LogicalInfo logicalDestination) 
	{
		this.physicalSender = physicalSender;
		this.logicalDestination = logicalDestination;
	}

	/**
	 * Construct the message body with a string value.
	 * 
	 * @param serializeData the string value that contains the serialized message body
	 */
	public SPLeaveFindReplacementNodeReplyBody(String serializeData)
	{
		String[] arrData = serializeData.split(":");	
		try
		{
			this.physicalSender = new PhysicalInfo(arrData[1]);
			this.logicalDestination = new LogicalInfo(arrData[2]);
		}
		catch(Exception e)
		{
			System.out.println("Incorrect serialize data at LeaveFindReplacementNodeReply:" + serializeData);
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

		outMsg = "LEAVEFINDREPLACEMENTNODEREPLY";
		outMsg += "\n\t Physical Sender:" + physicalSender.toString();
		outMsg += "\n\t Logical Destination:" + logicalDestination.toString();

		return outMsg;
	}

	@Override
	public String toString()
	{
		String outMsg;
		
		outMsg = "LEAVEFINDREPLACEMENTNODEREPLY";
		outMsg += ":" + physicalSender.toString();
		outMsg += ":" + logicalDestination.toString();

		return outMsg;
	}
}
