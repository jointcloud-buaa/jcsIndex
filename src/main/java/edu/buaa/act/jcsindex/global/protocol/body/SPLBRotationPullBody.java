/*
 * @(#) SPLBRotationPullBody.java 1.0 2006-2-22
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
 * Implement the message body used for pulling a node to replace
 * its position due to network restructuring 
 * 
 * @author Vu Quang Hieu
 * @version 1.0 2006-2-22
 */

public class SPLBRotationPullBody extends Body implements Serializable 
{

	// private members	
	private static final long serialVersionUID = -1734143381489175106L;	

  	private PhysicalInfo physicalSender;
  	private LogicalInfo logicalSender;
  	private TreeNode treeNode;
  	private boolean direction;

  	/**
	 * Construct the message body with speicified parameters.
  	 * 
  	 * @param physicalSender physical address of the sender
  	 * @param logicalSender logical address of the sender
  	 * @param treeNode information of a tree node
  	 * @param direction direction of pulling
  	 */
  	public SPLBRotationPullBody(PhysicalInfo physicalSender, 
  			LogicalInfo logicalSender, TreeNode treeNode, 
  			boolean direction) 
  	{
  		this.physicalSender = physicalSender;
  		this.logicalSender = logicalSender;
  		this.treeNode = treeNode;
  		this.direction = direction;
  	}

	/**
	 * Construct the message body with a string value.
	 * 
	 * @param serializeData the string value that contains 
	 * the serialized message body
	 */
  	public SPLBRotationPullBody(String serializeData)
  	{
  		String[] arrData = serializeData.split(":");
  		try
  		{
  			this.physicalSender = new PhysicalInfo(arrData[1]);
  			this.logicalSender = new LogicalInfo(arrData[2]);
  			this.treeNode = new TreeNode(arrData[3]);
  			this.direction = Boolean.valueOf(arrData[4]).booleanValue();
  		}
  		catch(Exception e)
  		{
  			System.out.println("Incorrect serialize data at LBRotationPull:" + serializeData);
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
  	 * Get information of a tree node
  	 * 
  	 * @return information of a tree node
  	 */
  	public TreeNode getTreeNode()
  	{
  		return this.treeNode;
  	}

  	/**
  	 * Get direction of pulling
  	 * 
  	 * @return direction of pulling
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

  		outMsg = "LBROTATIONPULL";
  		outMsg += "\n\t Physical Sender:" + physicalSender.toString();
  		outMsg += "\n\t Logical Sender:" + logicalSender.toString();
  		outMsg += "\n\t Tree Node:" + treeNode.toString();
  		outMsg += "\n\t Direction:" + String.valueOf(direction);

  		return outMsg;
  	}

  	@Override
  	public String toString()
  	{
  		String outMsg;

  		outMsg = "LBROTATIONPULL";
  		outMsg += ":" + physicalSender.toString();
  		outMsg += ":" + logicalSender.toString();
  		outMsg += ":" + treeNode.toString();
  		outMsg += ":" + String.valueOf(direction);

  		return outMsg;
  	}
  	
}
