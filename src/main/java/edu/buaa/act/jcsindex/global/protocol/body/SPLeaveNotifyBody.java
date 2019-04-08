/*
 * @(#) SPLeaveNotifyBody.java 1.0 2006-3-3
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.protocol.body;

import edu.buaa.act.jcsindex.global.peer.info.LogicalInfo;
import edu.buaa.act.jcsindex.global.peer.info.PhysicalInfo;

import java.io.Serializable;

/**
 * Implement the message body used for notify all nodes except the parent
 * when a node leaves the network 
 * 
 * @author Vu Quang Hieu
 * @version 1.0 2006-3-3
 */

public class SPLeaveNotifyBody extends Body implements Serializable 
{

	// private members	
	private static final long serialVersionUID = 3092041444815714216L;	
	
	private PhysicalInfo physicalSender;
	private LogicalInfo logicalSender;
	private PhysicalInfo physicalReplacer;
	private int position;
	private int index;
	private LogicalInfo logicalDestination;

	/**
	 * Construct the message body with specified parameters.
	 * 
	 * @param physicalSender physical address of the sender
	 * @param logicalSender logical address of the sender
	 * @param physicalReplacer physical address of the replacer
	 * @param position position of the departure node
	 * 0: departure node is the parent of the receiver node
	 * 1: departure node is the left adjacent of the receiver node
	 * 2: departure node is the right adjacent of the receiver node
	 * 3: departure node is the left neighbor of the receiver node
	 * 4: departure node is the right neighbor of the receiver node
	 * @param index having meaning on if position is 1 or 2: the index
	 * of the departure node inside the routing table
	 * @param logicalDestination logical address of the receiver
	 */
	public SPLeaveNotifyBody(PhysicalInfo physicalSender, LogicalInfo logicalSender,
							PhysicalInfo physicalReplacer, int position, int index,
							LogicalInfo logicalDestination) 
	{
		this.physicalSender = physicalSender;
		this.logicalSender = logicalSender;
		this.physicalReplacer = physicalReplacer;
		this.position = position;
		this.index = index;
		this.logicalDestination = logicalDestination;
	}

	/**
	 * Construct the message body with a string value.
	 * 
	 * @param serializeData the string value that contains 
	 * the serialized message body
	 */
	public SPLeaveNotifyBody(String serializeData)
	{
		String[] arrData = serializeData.split(":");
		try
		{
			this.physicalSender = new PhysicalInfo(arrData[1]);
			this.logicalSender = new LogicalInfo(arrData[2]);
			this.physicalReplacer = new PhysicalInfo(arrData[3]);
			this.position = Integer.parseInt(arrData[4]);
			this.index = Integer.parseInt(arrData[5]);
			this.logicalDestination = new LogicalInfo(arrData[6]);
		}
		catch(Exception e)
		{
			System.out.println("Incorrect serialize data at LeaveNotify:" + serializeData);
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
	 * Get physical address of the replacer
	 * 
	 * @return tree node
	 */
	public PhysicalInfo getPhysicalReplacer()
	{
		return this.physicalReplacer;
	}

	/**
	 * Get the position of the departure node
	 * 
	 * @return position of the departure node
	 */
	public int getPosition()
	{
		return this.position;
	}
	
	/**
	 * Get the index of the departure node inside the routing table
	 * 
	 * @return index of the departure node
	 */
	public int getIndex()
	{
		return this.index;
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

		outMsg = "LEAVENOTIFY";
		outMsg += "\n\t Physical Sender:" + physicalSender.toString();
		outMsg += "\n\t Logical Sender:" + logicalSender.toString();
		outMsg += "\n\t Physical Replacer:" + physicalReplacer.toString();
		outMsg += "\n\t Position:" + position;
		outMsg += "\n\t Index:" + index;
		outMsg += "\n\t Logical Destination:" + logicalDestination.toString();

		return outMsg;
	}

	@Override
	public String toString()
	{
		String outMsg;

		outMsg = "LEAVENOTIFY";
		outMsg += ":" + physicalSender.toString();
		outMsg += ":" + logicalSender.toString();
		outMsg += ":" + physicalReplacer.toString();
		outMsg += ":" + position;
		outMsg += ":" + index;
		outMsg += ":" + logicalDestination.toString();

		return outMsg;
	}
	
}
