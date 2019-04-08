/*
 * @(#) SPLIAdjacentRootReplyBody.java 1.0 2006-12-13
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.protocol.body;

import edu.buaa.act.jcsindex.global.peer.info.AdjacentNodeInfo;
import edu.buaa.act.jcsindex.global.peer.info.BoundaryValue;
import edu.buaa.act.jcsindex.global.peer.info.LogicalInfo;
import edu.buaa.act.jcsindex.global.peer.info.PhysicalInfo;

import java.io.Serializable;

/**
 * Implement the message body used for replying adjacent info in case of node failure
 * 
 * @author Vu Quang Hieu
 * @version 1.0 2006-12-13
 */

public class SPLIAdjacentRootReplyBody extends Body implements Serializable
{
	
	// constant definition for direction
	public static final boolean FROM_LEFT_TO_RIGHT = false;
	public static final boolean FROM_RIGHT_TO_LEFT = true;
	
	private static final long serialVersionUID = 7851338115220694911L;

	private PhysicalInfo physicalSender;
  	private LogicalInfo logicalSender;
  	private AdjacentNodeInfo newAdjacent;
  	private BoundaryValue borderValue;
  	private boolean direction;  	
  	private LogicalInfo logicalDestination;

  	/**
  	 * Construct the message body with specified parameters.
  	 * 
  	 * @param physicalSender physical address of the sender
  	 * @param logicalSender logical address of the sender
  	 * @param newAdjacent new adjacent node info
  	 * @param direction direction of the new adjacent node
  	 * @param logicalDestination logical address of the receiver
  	 */
  	public SPLIAdjacentRootReplyBody(PhysicalInfo physicalSender, LogicalInfo logicalSender, 
  			AdjacentNodeInfo newAdjacent, BoundaryValue borderValue, boolean direction, LogicalInfo logicalDestination) 
  	{
  		this.physicalSender = physicalSender;
  		this.logicalSender = logicalSender;
  		this.newAdjacent = newAdjacent;
  		this.borderValue = borderValue;
  		this.direction = direction;
  		this.logicalDestination = logicalDestination;
  	}

  	/**
  	 * Construct the message body with a string value.
  	 * 
  	 * @param serializeData the string value that contains 
  	 * the serialized message body
  	 */
  	public SPLIAdjacentRootReplyBody(String serializeData)
  	{
  		String[] arrData = serializeData.split(":");
  		try
  		{
  			this.physicalSender = new PhysicalInfo(arrData[1]);
  			this.logicalSender = new LogicalInfo(arrData[2]);
  			this.newAdjacent = new AdjacentNodeInfo(arrData[3]);
  			this.borderValue = new BoundaryValue(arrData[4]);
  			this.direction = Boolean.valueOf(arrData[5]).booleanValue();
  			this.logicalDestination = new LogicalInfo(arrData[6]);
  		}
  		catch(Exception e)
  		{
  			System.out.println("Incorrect serialize data at LIAdjacentRootReply:" + serializeData);
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
  	 * Get information of the new adjacent node
  	 * 
  	 * @return information of the new adjacent node
  	 */
  	public AdjacentNodeInfo getNewAdjacent()
  	{
  		return this.newAdjacent;
  	}

  	/**
  	 * Get information of the border value
  	 * 
  	 * @return
  	 */
  	public BoundaryValue getBorderValue()
  	{
  		return this.borderValue;
  	}
  	
  	/**
  	 * Get direction of the new adjacent node
  	 * 
  	 * @return direction of the new adjacent node
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

  		outMsg = "LIADJACENTROOTREPLY";
  		outMsg += "\n\t Physical Sender:" + physicalSender.toString();
  		outMsg += "\n\t Logical Sender:" + logicalSender.toString();
  		outMsg += "\n\t New Adjacent:" + newAdjacent.toString();
  		outMsg += "\n\t Direction:" + String.valueOf(direction);
    	outMsg += "\n\t Logical Destination:" + logicalDestination.toString();

    	return outMsg;
  	}

  	@Override
  	public String toString()
  	{
  		String outMsg;

  		outMsg = "LIADJACENTROOTREPLY";
  		outMsg += ":" + physicalSender.toString();
  		outMsg += ":" + logicalSender.toString();
  		outMsg += ":" + newAdjacent.toString();
  		outMsg += ":" + direction;
  		outMsg += ":" + logicalDestination.toString();

  		return outMsg;
  	}
  	
}
