/*
 * @(#) SPLeaveFindReplacementNodeBody.java 1.0 2006-2-22
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.protocol.body;

import edu.buaa.act.jcsindex.global.peer.info.LogicalInfo;
import edu.buaa.act.jcsindex.global.peer.info.PhysicalInfo;

import java.io.Serializable;

/**
 * Implement the message body used for finding a leaf node
 * to replace the position of the leaving node
 * 
 * @author Vu Quang Hieu
 * @version 1.0 2006-2-22
 */

public class SPLeaveFindReplacementNodeBody extends Body implements Serializable 
{

	// private members
	private static final long serialVersionUID = 3488146603561710621L;

	private PhysicalInfo physicalSender;
	private LogicalInfo logicalSender;
	private PhysicalInfo physicalRequester;
	private LogicalInfo logicalRequester;
	private LogicalInfo logicalDestination;

	/**
	 * Construct the message body with specified parameters.
	 * 
	 * @param physicalSender physical address of the sender
	 * @param logicalSender logical address of the sender
	 * @param physicalRequester physical address of the leaving node
	 * @param logicalRequester logical address of the leaving node
	 * @param logicalDestination logical address of the receiver
	 */
	public SPLeaveFindReplacementNodeBody(PhysicalInfo physicalSender, LogicalInfo logicalSender,
										  PhysicalInfo physicalRequester, LogicalInfo logicalRequester,
										  LogicalInfo logicalDestination) 
	{
		this.physicalSender = physicalSender;
		this.logicalSender = logicalSender;
		this.physicalRequester = physicalRequester;
		this.logicalRequester = logicalRequester;
		this.logicalDestination = logicalDestination;
	}

	/**
	 * Construct the message body with a string value.
	 * 
	 * @param serializeData the string value that contains 
	 * the serialized message body
	 */
	public SPLeaveFindReplacementNodeBody(String serializeData)
	{
		String[] arrData = serializeData.split(":");
		try
		{
			this.physicalSender = new PhysicalInfo(arrData[1]);
			this.logicalSender = new LogicalInfo(arrData[2]);
			this.physicalRequester = new PhysicalInfo(arrData[3]);
			this.logicalRequester = new LogicalInfo(arrData[4]);
			this.logicalDestination = new LogicalInfo(arrData[5]);
		}
		catch(Exception e)
		{
			System.out.println("Incorrect serialize data at LeaveFindReplacementNode:" + serializeData);
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
	 * Update logical address of the sender
	 * 
	 * @param logicalSender
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
	 * Get physical address of the leaving node
	 * 
	 * @return physical address of the leaving node
	 */
	public PhysicalInfo getPhysicalRequester()
	{
		return this.physicalRequester;
	}

	/**
	 * Get logical address of the leaving node
	 * 
	 * @return logical address of the leaving node
	 */
	public LogicalInfo getLogicalRequester()
	{
		return this.logicalRequester;
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

		outMsg = "LEAVEFINDREPLACEMENTNODE";
		outMsg += "\n\t Physical Sender:" + physicalSender.toString();
		outMsg += "\n\t Logical Sender:" + logicalSender.toString();
		outMsg += "\n\t Physical Requester:" + physicalRequester.toString();
		outMsg += "\n\t Logical Requester:" + logicalRequester.toString();
		outMsg += "\n\t Logical Destination:" + logicalDestination.toString();

		return outMsg;
	}

	@Override
	public String toString()
	{
		String outMsg;

		outMsg = "LEAVEFINDREPLACEMENTNODE";
		outMsg += ":" + physicalSender.toString();
		outMsg += ":" + logicalSender.toString();
		outMsg += ":" + physicalRequester.toString();
		outMsg += ":" + logicalRequester.toString();
		outMsg += ":" + logicalDestination.toString();

		return outMsg;
	}
	
}
