/*
 * @(#) SPJoinBody.java 1.0
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.protocol.body;

import edu.buaa.act.jcsindex.global.peer.info.LogicalInfo;
import edu.buaa.act.jcsindex.global.peer.info.PhysicalInfo;

import java.io.Serializable;

/**
 * Implement the message body used for joining a new
 * super peer into the network
 * 
 * @author Vu Quang Hieu
 * @version 1.0
 */

public class SPJoinBody extends Body implements Serializable
{
	
	// private members
	/**
	 * The serialVersionUID is used for serializing and de-serializing
	 * this class and be SURE NOT CHANGE THIS VALUE! 
	 */
	private static final long serialVersionUID = 5114749364748796246L;
	
	private PhysicalInfo physicalSender;
	private LogicalInfo logicalSender;
	private PhysicalInfo newNode;
	private LogicalInfo logicalDestination;

	/**
	 * Construct the message body with specified parameters.
	 * 
	 * @param physicalSender physical address of the sender
	 * @param logicalSender logical address of the sender
	 * @param newNode physical address of the new peer
	 * @param logicalDestination logical address of the receiver
	 */	
	public SPJoinBody(PhysicalInfo physicalSender, LogicalInfo logicalSender, 
			PhysicalInfo newNode, LogicalInfo logicalDestination) 
	{
		this.physicalSender = physicalSender;
	    this.logicalSender = logicalSender;
	    this.newNode = newNode;
	    this.logicalDestination = logicalDestination;
	}

	/**
	 * Construct the message body from a string value.
	 * 
	 * @param serializeData the string value that contains 
	 * the serialized message body
	 */	
	public SPJoinBody(String serializeData)
	{
	    String[] arrData = serializeData.split(":");
	    try
	    {
	    	this.physicalSender = new PhysicalInfo(arrData[1]);
	    	if (arrData[2].equals("null")) 
	    	{
	    		this.logicalSender = null;
	    	}
	    	else 
	    	{
	    		this.logicalSender = new LogicalInfo(arrData[2]);
	    	}
	    	
	    	this.newNode = new PhysicalInfo(arrData[3]);
	    	if (arrData[4].equals("null")) 
	    	{
	    		this.logicalDestination = null;
	    	}
	    	else 
	    	{
	    		this.logicalDestination = new LogicalInfo(arrData[4]);
	    	}
	    }
	    catch(Exception e){
	    	System.out.println("Incorrect serialize data at Join:" + serializeData);
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
	 * @param logicalSender logical address of the sender
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
	 * Get physical address of the new node
	 * 
	 * @return physical address of the new node
	 */
	public PhysicalInfo getNewNode()
	{
	    return this.newNode;
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
	 * Get logica address of the receiver
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

	    outMsg = "JOIN";
	    outMsg += "\n\t Physical Sender:" + physicalSender.toString();
	    if (logicalSender == null)
	    	outMsg += "\n\t Logical Sender:null";
	    else
	    	outMsg += "\n\t Logical Sender:" + logicalSender.toString();
	    outMsg += "\n\t New Node:" + newNode.toString();
	    if (logicalDestination == null)
	    	outMsg += "\n\t Logical Destination:null";
	    else
	    	outMsg += "\n\t Logical Destination:" + logicalDestination.toString();

	    return outMsg;
	}

	@Override
	public String toString()
	{
		String outMsg;

	    outMsg = "JOIN";
	    outMsg += ":" + physicalSender.toString();
	    if (logicalSender == null)
	    	outMsg += ":" + "null";
	    else
	    	outMsg += ":" + logicalSender.toString();
	    outMsg += ":" + newNode.toString();
	    if (logicalDestination == null)
	    	outMsg += ":" + "null";
	    else
	    	outMsg += ":" + logicalDestination.toString();

	    return outMsg;
	}
	
}
