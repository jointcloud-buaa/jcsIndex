/*
 * @(#) SPLIChildReplyBody.java 1.0 2006-12-08
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.protocol.body;

import edu.buaa.act.jcsindex.global.peer.info.ChildNodeInfo;
import edu.buaa.act.jcsindex.global.peer.info.LogicalInfo;
import edu.buaa.act.jcsindex.global.peer.info.PhysicalInfo;

import java.io.Serializable;

/**
 * Implement the message body used for replying child links
 * to the parent of the failed node
 * @author Vu Quang Hieu
 * @version 1.0 2006-12-08
 */

public class SPLIChildReplyBody extends Body implements Serializable
{

	// private members
  	private static final long serialVersionUID = 6439936843022381187L;
  	
  	private PhysicalInfo physicalSender;
  	private LogicalInfo logicalSender;
  	private ChildNodeInfo leftChild;
  	private ChildNodeInfo rightChild;
  	private LogicalInfo logicalDestination;

  	/**
	 * Construct the message body with speicified parameters.
  	 * 
  	 * @param physicalSender physical address of the sender
  	 * @param logicalSender logical address of the sender
  	 * @param leftChild left child information of the failed node
  	 * @param rightChild right child information of the failed node
  	 * @param logicalDestination logical address of the receiver
  	 */
  	public SPLIChildReplyBody(PhysicalInfo physicalSender, LogicalInfo logicalSender,
  			ChildNodeInfo leftChild, ChildNodeInfo rightChild, LogicalInfo logicalDestination) 
  	{
  		this.physicalSender = physicalSender;
  		this.logicalSender = logicalSender;
  		this.leftChild = leftChild;
  		this.rightChild = rightChild;
  		this.logicalDestination = logicalDestination;
  	}

	/**
	 * Construct the message body with a string value.
	 * 
	 * @param serializeData the string value that contains 
	 * the serialized message body
	 */
  	public SPLIChildReplyBody(String serializeData)
  	{
  		String[] arrData = serializeData.split(":");
  		try
  		{
  			this.physicalSender = new PhysicalInfo(arrData[1]);
  			this.logicalSender = new LogicalInfo(arrData[2]);
  			if (arrData[3].equals("null"))
  				this.leftChild = null;
  			else
  				this.leftChild = new ChildNodeInfo(arrData[3]);
  			if (arrData[4].equals("null"))
  				this.rightChild = null;
  			else
  				this.rightChild = new ChildNodeInfo(arrData[4]);
  			this.logicalDestination = new LogicalInfo(arrData[5]);
  		}
  		catch(Exception e)
  		{
  			System.out.println("Incorrect serialize data at SPLIChildReplyBody:" + serializeData);
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
  	 * Get left child of the failed node
  	 * 
  	 * @return left child of the failed node
  	 */
  	public ChildNodeInfo getLeftChild()
  	{
  		return this.leftChild;
  	}

  	/**
  	 * Get right child of the failed node
  	 * 
  	 * @return right child of the failed node
  	 */
  	public ChildNodeInfo getRightChild()
  	{
  		return this.rightChild;
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

  		outMsg = "LICHILDREPLY";
  		outMsg += "\n\t Physical Sender:" + physicalSender.toString();
  		outMsg += "\n\t Logical Sender:" + logicalSender.toString();
  		if (this.leftChild == null)
  			outMsg += "\n\t Left Child:null";
  		else
  			outMsg += "\n\t Left Child:" + leftChild.toString();
  		if (this.rightChild == null)
  			outMsg += "\n\t Right Child:null";
  		else
  			outMsg += "\n\t Right Child:" + rightChild.toString();
  		outMsg += "\n\t Logical Destination:" + logicalDestination.toString();

  		return outMsg;
  	}

	@Override
  	public String toString()
  	{
  		String outMsg;

  		outMsg = "LICHILDREPLY";
  		outMsg += ":" + physicalSender.toString();
  		outMsg += ":" + logicalSender.toString();
  		if (this.leftChild == null)
  			outMsg += ":null";
  		else
  			outMsg += ":" + leftChild.toString();
  		if (this.rightChild == null)
  			outMsg += ":null";
  		else
  			outMsg += ":" + rightChild.toString();
  		outMsg += ":" + logicalDestination.toString();

  		return outMsg;
  	}
}
