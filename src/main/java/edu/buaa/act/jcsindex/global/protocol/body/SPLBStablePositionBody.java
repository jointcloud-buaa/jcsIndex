/*
 * @(#) SPLBStablePositionBody.java 1.0 2006-2-22
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.protocol.body;

import edu.buaa.act.jcsindex.global.peer.info.LogicalInfo;
import edu.buaa.act.jcsindex.global.peer.info.PhysicalInfo;

import java.io.Serializable;

/**
 * Implement the message body used for notifying co-owners of 
 * the same logical tree node to drop their slave nodes because
 * the master node now is stable
 * 
 * @author Vu Quang Hieu
 * @version 1.0 2006-2-22
 */

public class SPLBStablePositionBody extends Body implements Serializable
{

	// private members
	private static final long serialVersionUID = -4801132193922168727L;
	
	private PhysicalInfo physicalSender;
	private LogicalInfo logicalSender;
	private LogicalInfo logicalDestination;

	/**
	 * Construct the message body with speicified parameters.
	 * 
	 * @param physicalSender physical address of the sender
	 * @param logicalSender logical address of the sender
	 * @param logicalDestination logical address of the receiver
	 */
	public SPLBStablePositionBody(PhysicalInfo physicalSender, LogicalInfo logicalSender, LogicalInfo logicalDestination) 
	{
		this.physicalSender = physicalSender;
		this.logicalSender = logicalSender;
		this.logicalDestination = logicalDestination;
	}

	/**
	 * Construct the message body with a string value.
	 * 
	 * @param serializeData the string value that contains 
	 * the serialized message body
	 */
	public SPLBStablePositionBody(String serializeData)
	{
		String[] arrData = serializeData.split(":");
		try
		{
			this.physicalSender = new PhysicalInfo(arrData[1]);
			this.logicalSender = new LogicalInfo(arrData[2]);
			this.logicalDestination = new LogicalInfo(arrData[3]);
		}
		catch(Exception e)
		{
			System.out.println("Incorrect serialize data at LBStablePosition:" + serializeData);
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

		outMsg = "LBSTABLEPOSITION";
		outMsg += "\n\t Physical Sender:" + physicalSender.toString();
		outMsg += "\n\t Logical Sender:" + logicalSender.toString();
		outMsg += "\n\t Logical Destination:" + logicalDestination.toString();

		return outMsg;
	}

	@Override
	public String toString()
	{
		String outMsg;

		outMsg = "LBSTABLEPOSITION";
		outMsg += ":" + physicalSender.toString();
		outMsg += ":" + logicalSender.toString();
		outMsg += ":" + logicalDestination.toString();

		return outMsg;
	}
	
}
