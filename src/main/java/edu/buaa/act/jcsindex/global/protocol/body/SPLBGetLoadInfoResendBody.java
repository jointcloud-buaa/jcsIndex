/*
 * @(#) SPLBGetLoadInfoResendBody.java 1.0 2006-2-22
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.protocol.body;

import edu.buaa.act.jcsindex.global.peer.info.LogicalInfo;
import edu.buaa.act.jcsindex.global.peer.info.PhysicalInfo;

import java.io.Serializable;

/**
 * Implement the message body used for resending request
 * of getting workload information to the requester since
 * the receiver is not its adjacent
 * 
 * @author Vu Quang Hieu
 * @version 1.0 2006-2-22
 */

public class SPLBGetLoadInfoResendBody extends Body implements Serializable
{

	private static final long serialVersionUID = -6213569484838387417L;

	private PhysicalInfo physicalSender;
	private boolean direction;
	private LogicalInfo logicalDestination;

	/**
	 * Construct the message body with speicified parameters.
	 * 
	 * @param physicalSender physical address of the sender
	 * @param direction direction of sending request
	 * @param logicalDestination logical address of the receiver 
	 */
	public SPLBGetLoadInfoResendBody(PhysicalInfo physicalSender, 
			boolean direction, LogicalInfo logicalDestination) 
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
	public SPLBGetLoadInfoResendBody(String serializeData)
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
			System.out.println("Incorrect serialize data at LBGetLoadInfoResend:" + serializeData);
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

		outMsg = "LBGETLOADINFORESEND";
		outMsg += "\n\t Physical Sender:" + physicalSender.toString();
		outMsg += "\n\t Direction:" + direction;
		outMsg += "\n\t Logical Destination:" + logicalDestination.toString();

		return outMsg;
	}

	@Override
	public String toString()
	{
		String outMsg;

		outMsg = "LBGETLOADINFORESEND";
		outMsg += ":" + physicalSender.toString();
		outMsg += ":" + direction;
		outMsg += ":" + logicalDestination.toString();

		return outMsg;
	}
	
}
