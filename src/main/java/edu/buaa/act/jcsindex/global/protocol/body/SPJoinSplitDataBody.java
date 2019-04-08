/*
 * @(#) SPJoinSplitDataBody.java 1.0 2006-2-22
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.protocol.body;

import edu.buaa.act.jcsindex.global.peer.info.*;

import java.io.Serializable;

/**
 * Implement the message body used for splitting the range and values 
 * of an left adjacent super peer when a new node joins as a left child
 * of an existing node
 * 
 * @author Vu Quang Hieu
 * @version 1.0 2006-2-22
 */

public class SPJoinSplitDataBody extends Body implements Serializable
{

	// private members
	private static final long serialVersionUID = 3177441905733806610L;
	
	private PhysicalInfo physicalSender;
	private LogicalInfo logicalSender;
	private PhysicalInfo physicalNewNode;
	private LogicalInfo logicalNewNode;
	private int numberOfExpectedRTReply;
	private RoutingTableInfo leftRT, rightRT;
	private RoutingItemInfo parentNodeInfo;
	private ChildNodeInfo rightChildInfo;
	private LogicalInfo logicalDestination;

	/**
  	 * Construct the message body with specified parameters.
  	 * 
	 * @param physicalSender physical address of the sender
	 * @param logicalSender logical address of the sender
	 * @param physicalNewNode physical address of the new node
	 * @param logicalNewNode logical address of the new node
	 * @param numberOfExpectedRTReply number of expected routing table
	 * reply, which should be received by the new node 
	 * @param leftRT left routing table information of the new node
	 * @param rightRT right routing table information of the new node
	 * @param parentNodeInfo parent information of the new node
	 * @param logicalDestination logical address of the receiver
	 */
	public SPJoinSplitDataBody(PhysicalInfo physicalSender, LogicalInfo logicalSender,
								PhysicalInfo physicalNewNode, LogicalInfo logicalNewNode,
								int numberOfExpectedRTReply, RoutingTableInfo leftRT,
								RoutingTableInfo rightRT, RoutingItemInfo parentNodeInfo,
								ChildNodeInfo rightChildInfo, LogicalInfo logicalDestination) 
	{
	    this.physicalSender = physicalSender;
	    this.logicalSender = logicalSender;
	    this.physicalNewNode = physicalNewNode;
	    this.logicalNewNode = logicalNewNode;
	    this.numberOfExpectedRTReply = numberOfExpectedRTReply;
	    this.leftRT = leftRT;
	    this.rightRT = rightRT;
	    this.parentNodeInfo = parentNodeInfo;
	    this.rightChildInfo = rightChildInfo;
	    this.logicalDestination = logicalDestination;
	}

	/**
	 * Construct the message body with a string value.
	 * 
	 * @param serializeData the string value that contains 
	 * the serialized message body
	 */
	public SPJoinSplitDataBody(String serializeData)
	{
	    String[] arrData = serializeData.split(":");
	    try
	    {
	    	this.physicalSender = new PhysicalInfo(arrData[1]);
	    	this.logicalSender = new LogicalInfo(arrData[2]);
	    	this.physicalNewNode = new PhysicalInfo(arrData[3]);
	    	this.logicalNewNode = new LogicalInfo(arrData[4]);
	    	this.numberOfExpectedRTReply = Integer.parseInt(arrData[5]);
	    	this.leftRT = new RoutingTableInfo(arrData[6]);
	    	this.rightRT = new RoutingTableInfo(arrData[7]);
	    	this.parentNodeInfo = new RoutingItemInfo(arrData[8]);
	    	if (arrData[9].equals("null"))
	    		this.rightChildInfo = null;
	    	else
	    		this.rightChildInfo = new ChildNodeInfo(arrData[9]);
	    	this.logicalDestination = new LogicalInfo(arrData[10]);
	    }
	    catch(Exception e)
	    {
	      System.out.println("Incorrect serialize data at JoinSplitData:" + serializeData);
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
	 * Get physical address of the new node
	 * 
	 * @return physical address of the new node
	 */
	public PhysicalInfo getPhysicalNewNode()
	{
	    return this.physicalNewNode;
	}

	/**
	 * Get logical address of the new node
	 * 
	 * @return logical address of the new node
	 */
	public LogicalInfo getLogicalNewNode()
	{
	    return this.logicalNewNode;
	}

	/**
	 * Get number of expected routing table reply 
	 * 
	 * @return number of expected routing table reply,
	 * which should be received by the new node
	 */
	public int getNumberOfExpectedRTReply()
	{
	    return this.numberOfExpectedRTReply;
	}

	/**
	 * Get left routing table information
	 * 
	 * @return left routing table information
	 */
	public RoutingTableInfo getLeftRT()
	{
	    return this.leftRT;
	}

	/**
	 * Get right routing table information
	 * 
	 * @return right routing table information
	 */
	public RoutingTableInfo getRightRT()
	{
	    return this.rightRT;
	}

	/**
	 * Get parent information
	 * 
	 * @return parent information
	 */
	public RoutingItemInfo getParentNodeInfo()
	{
	    return this.parentNodeInfo;
	}

	/**
	 * Get right child information
	 * 
	 * @return
	 */
	public ChildNodeInfo getRightChildInfo()
	{
		return this.rightChildInfo;
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

	    outMsg = "JOINSPLITDATA";
	    outMsg += "\n\t Physical Sender:" + physicalSender.toString();
	    outMsg += "\n\t Logical Sender:" + logicalSender.toString();
	    outMsg += "\n\t Physical New Node:" + physicalNewNode.toString();
	    outMsg += "\n\t Logical New Node:" + logicalNewNode.toString();
	    outMsg += "\n\t Number Of Expected RT Reply:" + numberOfExpectedRTReply;
	    outMsg += "\n\t Left RT:" + leftRT.toString();
	    outMsg += "\n\t Right RT:" + rightRT.toString();
	    outMsg += "\n\t Parent Node Info:" + parentNodeInfo.toString();
	    if (this.rightChildInfo == null)
	    	outMsg += "\n\t Right Child Info: null";
	    else
	    	outMsg += "\n\t Right Child Info:" + this.rightChildInfo.toString();
	    outMsg += "\n\t Logical Destination:" + logicalDestination.toString();

	    return outMsg;
	}

	@Override
	public String toString()
	{
	    String outMsg;

	    outMsg = "JOINSPLITDATA";
	    outMsg += ":" + physicalSender.toString();
	    outMsg += ":" + logicalSender.toString();
	    outMsg += ":" + physicalNewNode.toString();
	    outMsg += ":" + logicalNewNode.toString();
	    outMsg += ":" + numberOfExpectedRTReply;
	    outMsg += ":" + leftRT.toString();
	    outMsg += ":" + rightRT.toString();
	    outMsg += ":" + parentNodeInfo.toString();
	    if (this.rightChildInfo == null)
	    	outMsg += ":null";
	    else
	    	outMsg += ":" + this.rightChildInfo.toString();
	    outMsg += ":" + logicalDestination.toString();

	    return outMsg;
	}
	
}
