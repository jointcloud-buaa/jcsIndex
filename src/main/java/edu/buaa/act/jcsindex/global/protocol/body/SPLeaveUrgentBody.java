/*
 * @(#) SPLeaveUrgentBody.java 1.0 2006-3-3
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.protocol.body;

import edu.buaa.act.jcsindex.global.peer.info.LogicalInfo;
import edu.buaa.act.jcsindex.global.peer.info.PhysicalInfo;
import edu.buaa.act.jcsindex.global.peer.info.TreeNode;

import java.io.Serializable;

/**
 * Implement the message body used for notify the parent node
 * when a child leaves the network 
 * 
 * @author Vu Quang Hieu
 * @version 1.0 2006-3-3
 */

public class SPLeaveUrgentBody extends Body implements Serializable 
{

	// private members	
	private static final long serialVersionUID = -1303567723851954713L;	
	
	private PhysicalInfo physicalSender;
	private LogicalInfo logicalSender;
	private TreeNode treeNode;
	private LogicalInfo logicalDestination;

	/**
	 * Construct the message body with specified parameters.
	 * 
	 * @param physicalSender physical address of the sender
	 * @param logicalSender logical address of the sender
	 * @param treeNode 
	 * @param logicalDestination logical address of the receiver
	 */
	public SPLeaveUrgentBody(PhysicalInfo physicalSender, LogicalInfo logicalSender,
							TreeNode treeNode, LogicalInfo logicalDestination) 
	{
		this.physicalSender = physicalSender;
		this.logicalSender = logicalSender;
		this.treeNode = treeNode;
		this.logicalDestination = logicalDestination;
	}

	/**
	 * Construct the message body with a string value.
	 * 
	 * @param serializeData the string value that contains 
	 * the serialized message body
	 */
	public SPLeaveUrgentBody(String serializeData)
	{
		String[] arrData = serializeData.split(":");
		try
		{
			this.physicalSender = new PhysicalInfo(arrData[1]);
			this.logicalSender = new LogicalInfo(arrData[2]);
			this.treeNode = new TreeNode(arrData[3]);
			this.logicalDestination = new LogicalInfo(arrData[4]);
		}
		catch(Exception e)
		{
			System.out.println("Incorrect serialize data at LeaveUrgent:" + serializeData);
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
	 * Get tree node information
	 * 
	 * @return tree node
	 */
	public TreeNode getTreeNode()
	{
		return this.treeNode;
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

		outMsg = "LEAVEURGENT";
		outMsg += "\n\t Physical Sender:" + physicalSender.toString();
		outMsg += "\n\t Logical Sender:" + logicalSender.toString();
		outMsg += "\n\t Tree Node:" + treeNode.toString();
		outMsg += "\n\t Logical Destination:" + logicalDestination.toString();

		return outMsg;
	}

	@Override
	public String toString()
	{
		String outMsg;

		outMsg = "LEAVEURGENT";
		outMsg += ":" + physicalSender.toString();
		outMsg += ":" + logicalSender.toString();
		outMsg += ":" + treeNode.toString();		
		outMsg += ":" + logicalDestination.toString();

		return outMsg;
	}
	
}
