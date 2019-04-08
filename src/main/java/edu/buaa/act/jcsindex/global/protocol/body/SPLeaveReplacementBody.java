/*
 * @(#) SPLeaveReplacementBody.java 1.0 2006-2-22
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.protocol.body;

import edu.buaa.act.jcsindex.global.peer.info.ContentInfo;
import edu.buaa.act.jcsindex.global.peer.info.LogicalInfo;
import edu.buaa.act.jcsindex.global.peer.info.PhysicalInfo;
import edu.buaa.act.jcsindex.global.peer.info.TreeNode;

import java.io.Serializable;

/**
 * Implement the message body used for a leaving node to pass
 * it position to the replacement node
 * 
 * @author Vu Quang Hieu
 * @version 1.0 2006-2-22
 */

public class SPLeaveReplacementBody extends Body implements Serializable 
{

	// private members
	private static final long serialVersionUID = 7812421778746014901L;

	private PhysicalInfo physicalSender;
	private LogicalInfo logicalSender;
	private TreeNode treeNode;
	private ContentInfo content;

	/**
	 * Construct the message body with specified parameters.
	 * 
	 * @param physicalSender physical address of the sender
	 * @param logicalSender logical address of the sender
	 * @param treeNode logical node of the leaving node
	 * @param content stored data of the leaving node
	 */
	public SPLeaveReplacementBody(PhysicalInfo physicalSender, LogicalInfo logicalSender,
									TreeNode treeNode, ContentInfo content) 
	{
		this.physicalSender = physicalSender;
		this.logicalSender = logicalSender;
		this.treeNode = treeNode;
		this.content = content;
	}

	/**
	 * Construct the message body with a string value.
	 * 
	 * @param serializeData the string value that contains 
	 * the serialized message body
	 */
	public SPLeaveReplacementBody(String serializeData)
	{
		String[] arrData = serializeData.split(":");
		try
		{
			this.physicalSender = new PhysicalInfo(arrData[1]);
			this.logicalSender = new LogicalInfo(arrData[2]);
			this.treeNode = new TreeNode(arrData[3]);
			this.content = new ContentInfo(arrData[4]);
		}
		catch(Exception e)
		{
			System.out.println("Incorrect serialize data at LeaveReplacement:" + serializeData);
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
	 * Get logical node in tree tree of the leaving node
	 * 
	 * @return logical node information
	 */
	public TreeNode getTreeNode()
	{
		return this.treeNode;
	}

	/**
	 * Get stored data of the leaving node 
	 * 
	 * @return stored data of the leaving node
	 */
	public ContentInfo getContent()
	{
		return this.content;
	}

	/**
	 * Return a readable string for testing or writing in the log file 
	 * 
	 * @return a readable string
	 */
	public String getString()
	{
		String outMsg;

		outMsg = "LEAVEREPLACEMENT";
		outMsg += "\n\t Physical Sender:" + physicalSender.toString();
		outMsg += "\n\t Logical Sender:" + logicalSender.toString();
		outMsg += "\n\t Tree Node:" + treeNode.toString();
		outMsg += "\n\t Content:" + content.toString();

		return outMsg;
	}

	@Override
	public String toString()
	{
		String outMsg;

		outMsg = "LEAVEREPLACEMENT";
		outMsg += ":" + physicalSender.toString();
		outMsg += ":" + logicalSender.toString();
		outMsg += ":" + treeNode.toString();
		outMsg += ":" + content.toString();

		return outMsg;
	}
	
}
