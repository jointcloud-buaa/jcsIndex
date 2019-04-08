/*
 * @(#) SPJoinAcceptBody.java 1.0 2006-2-22
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.protocol.body;

import edu.buaa.act.jcsindex.global.peer.info.AdjacentNodeInfo;
import edu.buaa.act.jcsindex.global.peer.info.ContentInfo;
import edu.buaa.act.jcsindex.global.peer.info.LogicalInfo;
import edu.buaa.act.jcsindex.global.peer.info.PhysicalInfo;

import java.io.Serializable;

/**
 * Implement the message body used for sending acceptance to 
 * a new super peer  
 * 
 * @author Vu Quang Hieu
 * @version 1.0 2006-2-22
 */

public class SPJoinAcceptBody extends Body implements Serializable
{

	// private members
	/**
	 * The serialVersionUID is used for serializing and de-serializing
	 * this class and be SURE NOT CHANGE THIS VALUE!
	 */
	private static final long serialVersionUID = 396977498337932972L;

	private PhysicalInfo physicalSender;
	private LogicalInfo logicalSender;
	private LogicalInfo  newNodePosition;
	private AdjacentNodeInfo leftAdjacent;
	private AdjacentNodeInfo rightAdjacent;
	private ContentInfo content;
	private int numOfExpectedRTReply;
	private boolean isFake;
	private boolean direction;

	/**
	 * Construct the message body with specified parameters.
	 * 
	 * @param physicalSender physical address of the sender
	 * @param logicalSender logical address of the sender
	 * @param newNodePosition logical address of the new peer
	 * @param leftAdjacent information of the new peer's left adjacent
	 * @param rightAdjacent information of the new peer's right adjacent
	 * @param content range of values and data the new peer is in charge
	 * @param numOfExpectedRTReply number of expected routing table reply
	 * from new peer's neighbor nodes
	 * @param isFake <code>true</code>: the new node is a fake node,
	 * which is created for network restructuring purpose and will be 
	 * transfered to another node, <code>false</node>: the new node 
	 * is a real new joining node
	 * @param direction if the node is a fake node, it will be transfered 
	 * towards the direction: <code>true</code> to the right, 
	 * <code>false</code> to the left 
	 */	
	public SPJoinAcceptBody(PhysicalInfo physicalSender, LogicalInfo logicalSender,
							LogicalInfo newNodePosition, AdjacentNodeInfo leftAdjacent,
							AdjacentNodeInfo rightAdjacent, ContentInfo content,
							int numOfExpectedRTReply, boolean isFake, boolean direction) 
	{
	    this.physicalSender = physicalSender;
	    this.logicalSender = logicalSender;
	    this.newNodePosition = newNodePosition;
	    this.leftAdjacent = leftAdjacent;
	    this.rightAdjacent = rightAdjacent;
	    this.content = content;
	    this.numOfExpectedRTReply = numOfExpectedRTReply;
	    this.isFake = isFake;
	    this.direction = direction;
	}

	/**
	 * Construct the message body with a string value.
	 * 
	 * @param serializeData the string value that contains 
	 * the serialized message body
	 */
	public SPJoinAcceptBody(String serializeData)
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
	    	
	    	this.newNodePosition = new LogicalInfo(arrData[3]);
	    	if (arrData[4].equals("null"))
	    	{
	    		this.leftAdjacent = null;
	    	}
	    	else
	    	{
	    		this.leftAdjacent = new AdjacentNodeInfo(arrData[4]);
	    	}
	    	
	    	if (arrData[5].equals("null"))
	    	{
	    		this.rightAdjacent = null;
	    	}
	    	else
	    	{
	    		this.rightAdjacent = new AdjacentNodeInfo(arrData[5]);
	    	}
	    	
	    	this.content = new ContentInfo(arrData[6]);
	    	this.numOfExpectedRTReply = Integer.parseInt(arrData[7]);
	    	this.isFake = Boolean.valueOf(arrData[8]).booleanValue();
	    	this.direction = Boolean.valueOf(arrData[9]).booleanValue();
	    }
	    catch(Exception e)
	    {
	    	System.out.println("Incorrect serialize data at JoinAccept:" + serializeData);
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
	 * Get logical address of the new node
	 * 
	 * @return logical address of the new node
	 */
	public LogicalInfo getNewNodePosition()
	{
	    return this.newNodePosition;
	}

	/**
	 * Get information of the new node's left adjacent
	 * 
	 * @return information of the new node's left adjacent
	 */
	public AdjacentNodeInfo getLeftAdjacent()
	{
	    return this.leftAdjacent;
	}

	/**
	 * Get information of the new node's right adjacent
	 * 
	 * @return information of the new node's right adjacent
	 */
	public AdjacentNodeInfo getRightAdjacent()
	{
	    return this.rightAdjacent;
	}

	/**
	 * Get range of values and data the new peer is in charge
	 * 
	 * @return content information
	 */
	public ContentInfo getContent()
	{
	    return this.content;
	}

	/**
	 * Get number of expected routing table reply
	 * from new peer's neighbor nodes
	 * 
	 * @return number of expected routing table reply
	 * from new peer's neighbor nodes
	 */
	public int getNumOfExpectedRTReply()
	{
	    return this.numOfExpectedRTReply;
	}

	/**
	 * Get type of the new node
	 * 
	 * @return type of the new node: <code>true</code> it is 
	 * a fake node, <code>false</code> it is a real node
	 */
	public boolean getIsFake()
	{
	    return this.isFake;
	}

	/**
	 * Get direction for transfer the fake node. It only
	 * has meaning if the node is a fake node
	 * 
	 * @return direction for transfer the fake node
	 */
	public boolean getDirection()
	{
	    return this.direction;
	}

	/**
	 * Return a readable string for testing or writing in the log file 
	 * 
	 * @return a readable string
	 */
	public String getString()
	{
		String outMsg;

		outMsg = "JOINACCEPT";
		outMsg += "\n\t Physical Sender:" + physicalSender.toString();
		if (logicalSender == null)
		{
			outMsg += "\n\t Logical Sender:null";
		}
		else
		{
			outMsg += "\n\t Logical Sender:" + logicalSender.toString();
		}
		
		outMsg += "\n\t New Node Position:" + newNodePosition.toString();
		if (leftAdjacent == null)
		{
			outMsg += "\n\t Left Adjacent:null";
		}
		else
		{
			outMsg += "\n\t Left Adjacent:" + leftAdjacent.toString();
		}
		
		if (rightAdjacent == null)
		{
			outMsg += "\n\t Right Adjacent:null";
		}
		else
		{
			outMsg += "\n\t Right Adjacent:" + rightAdjacent.toString();
		}
		
		outMsg += "\n\t Content:" + content.toString();
		outMsg += "\n\t Number Of Expected RT Reply:" + numOfExpectedRTReply;
		outMsg += "\n\t Is Fake:" + String.valueOf(isFake);
		outMsg += "\n\t Direction:" + String.valueOf(direction);

	   return outMsg;
	}

	@Override
	public String toString()
	{
	    String outMsg;

	    outMsg = "JOINACCEPT";
	    outMsg += ":" + physicalSender.toString();
	    if (logicalSender == null)
	    {
	    	outMsg += ":" + "null";
	    }
	    else
	    {
	    	outMsg += ":" + logicalSender.toString();
	    }
	    
	    outMsg += ":" + newNodePosition.toString();
	    if (leftAdjacent == null)
	    {
	    	outMsg += ":" + "null";
	    }
	    else
	    {
	    	outMsg += ":" + leftAdjacent.toString();
	    }
	    
	    if (rightAdjacent == null)
	    {
	    	outMsg += ":" + "null";
	    }
	    else
	    {
	    	outMsg += ":" + rightAdjacent.toString();
	    }
	    
	    outMsg += ":" + content.toString();
	    outMsg += ":" + numOfExpectedRTReply;
	    outMsg += ":" + String.valueOf(isFake);
	    outMsg += ":" + String.valueOf(direction);

	    return outMsg;
	}
	
}
