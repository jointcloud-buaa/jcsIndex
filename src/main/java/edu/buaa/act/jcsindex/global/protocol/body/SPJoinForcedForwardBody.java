/*
 * @(#) SPJoinForceForwardBody.java 1.0 2006-2-22
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.protocol.body;

import edu.buaa.act.jcsindex.global.peer.info.ContentInfo;
import edu.buaa.act.jcsindex.global.peer.info.LogicalInfo;
import edu.buaa.act.jcsindex.global.peer.info.PhysicalInfo;

import java.io.Serializable;

/**
 * Implement the message body used for forwarding the JOINFORCED request
 *
 * @author Vu Quang Hieu
 * @version 1.0 2006-2-22
 */

public class SPJoinForcedForwardBody extends Body implements Serializable 
{

	// private members
  	private static final long serialVersionUID = -6734613428429987224L;
  	
  	private PhysicalInfo physicalSender;
  	private LogicalInfo logicalSender;
  	private PhysicalInfo physicalRequester;
  	private ContentInfo content;
  	private boolean direction;
  	private LogicalInfo logicalDestination;

  	/**
  	 * Construct the message body with specified parameters.
  	 * 
  	 * @param physicalSender physical address of the sender
  	 * @param logicalSender logical address of the sender
  	 * @param physicalRequester physical address of the forced-join node
  	 * @param content content information
  	 * @param direction <code>true</code>: node is forced to
	 * join as a right child of another node, <code>false</code>:
	 * node is forced to join as a left child
  	 * @param logicalDestination logical address of the receiver
  	 */
  	public SPJoinForcedForwardBody(PhysicalInfo physicalSender, LogicalInfo logicalSender,
  									PhysicalInfo physicalRequester, ContentInfo content,
  									boolean direction, LogicalInfo logicalDestination) 
  	{
  		this.physicalSender = physicalSender;
  		this.logicalSender = logicalSender;
  		this.physicalRequester = physicalRequester;
  		this.content = content;
  		this.direction = direction;
  		this.logicalDestination = logicalDestination;
  	}

  	/**
  	 * Construct the message body with a string value.
  	 * 
  	 * @param serializeData the string value that contains 
  	 * the serialized message body
  	 */
  	public SPJoinForcedForwardBody(String serializeData)
  	{
  		String[] arrData = serializeData.split(":");
  		try
  		{
  			this.physicalSender = new PhysicalInfo(arrData[1]);
  			this.logicalSender = new LogicalInfo(arrData[2]);
  			this.physicalRequester = new PhysicalInfo(arrData[3]);
  			this.content = new ContentInfo(arrData[4]);
  			this.direction = Boolean.valueOf(arrData[5]).booleanValue();
  			this.logicalDestination = new LogicalInfo(arrData[6]);
  		}
  		catch(Exception e)
  		{
  			System.out.println("Incorrect serialize data at JoinForcedForward:" + serializeData);
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
  	 * Get physical address of the forced-join node
  	 * 
  	 * @return physical address
  	 */
  	public PhysicalInfo getPhysicalRequester()
  	{
  		return this.physicalRequester;
  	}

  	/**
  	 * Get content information, which will be kept by the forced-join node
  	 * 
  	 * @return content information
  	 */
  	public ContentInfo getContent()
  	{
  		return this.content;
  	}

  	/**
  	 * Get direction
  	 * 
  	 * @return direction of searching
  	 */
  	public boolean getDirection()
  	{
  		return this.direction;
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

  		outMsg = "JOINFORCEDFORWARD";
  		outMsg += "\n\t Physical Sender:" + physicalSender.toString();
  		outMsg += "\n\t Logical Sender:" + logicalSender.toString();
  		outMsg += "\n\t Physical Requester:" + physicalRequester.toString();
  		outMsg += "\n\t Content:" + content.toString();
  		outMsg += "\n\t Direction:" + String.valueOf(direction);
  		outMsg += "\n\t Logical Destination:" + logicalDestination.toString();

  		return outMsg;
  	}

  	@Override
  	public String toString()
  	{
  		String outMsg;

  		outMsg = "JOINFORCEDFORWARD";
  		outMsg += ":" + physicalSender.toString();
  		outMsg += ":" + logicalSender.toString();
  		outMsg += ":" + physicalRequester.toString();
  		outMsg += ":" + content.toString();
  		outMsg += ":" + String.valueOf(direction);
  		outMsg += ":" + logicalDestination.toString();

  		return outMsg;
  	}
  	
}
