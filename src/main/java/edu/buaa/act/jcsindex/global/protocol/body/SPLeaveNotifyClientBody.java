/*
 * @(#) SPLeaveNotifyClientBody.java 1.0 2006-3-5
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.protocol.body;

import edu.buaa.act.jcsindex.global.peer.info.PhysicalInfo;

import java.io.Serializable;

/**
 * Implement the message body used for notify attached client nodes
 * when a node leaves the network 
 * 
 * @author Vu Quang Hieu
 * @version 1.0 2006-3-3
 */

public class SPLeaveNotifyClientBody extends Body implements Serializable 
{

	// private members	
	private static final long serialVersionUID = -5868849379584514066L;

	private PhysicalInfo physicalSender;
	private PhysicalInfo physicalReplacer;
	

	/**
	 * Construct the message body with specified parameters.
	 * 
	 * @param physicalSender physical address of the sender
	 * @param physicalReplacer physical address of the replacer
	 */
	public SPLeaveNotifyClientBody(PhysicalInfo physicalSender, PhysicalInfo physicalReplacer) 
	{
		this.physicalSender = physicalSender;	
		this.physicalReplacer = physicalReplacer;		
	}

	/**
	 * Construct the message body with a string value.
	 * 
	 * @param serializeData the string value that contains 
	 * the serialized message body
	 */
	public SPLeaveNotifyClientBody(String serializeData)
	{
		String[] arrData = serializeData.split(":");
		try
		{
			this.physicalSender = new PhysicalInfo(arrData[1]);			
			this.physicalReplacer = new PhysicalInfo(arrData[2]);
		}
		catch(Exception e)
		{
			System.out.println("Incorrect serialize data at LeaveNotifyClient:" + serializeData);
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
	 * Get physical address of the replacer
	 * 
	 * @return tree node
	 */
	public PhysicalInfo getPhysicalReplacer()
	{
		return this.physicalReplacer;
	}

	/**
	 * Return a readable string for testing or writing in the log file 
	 * 
	 * @return a readable string
	 */
	public String getString()
	{
		String outMsg;

		outMsg = "LEAVENOTIFYCLIENT";
		outMsg += "\n\t Physical Sender:" + physicalSender.toString();
		outMsg += "\n\t Physical Replacer:" + physicalReplacer.toString();

		return outMsg;
	}

	@Override
	public String toString()
	{
		String outMsg;

		outMsg = "LEAVENOTIFYCLIENT";
		outMsg += ":" + physicalSender.toString();		
		outMsg += ":" + physicalReplacer.toString();

		return outMsg;
	}
	
}
