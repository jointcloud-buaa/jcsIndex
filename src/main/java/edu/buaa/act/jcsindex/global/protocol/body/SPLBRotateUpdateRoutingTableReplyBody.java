/*
 * @(#) SPLBRotateUpdateRoutingTableReplyBody.java 1.0 2006-2-22
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.protocol.body;

import edu.buaa.act.jcsindex.global.peer.info.LogicalInfo;
import edu.buaa.act.jcsindex.global.peer.info.PhysicalInfo;
import edu.buaa.act.jcsindex.global.peer.info.RoutingItemInfo;

import java.io.Serializable;

/**
 * Implement the message body used for replying SPLBRotateUpdateRoutingTable protocol 
 * 
 * @author Vu Quang Hieu
 * @version 1.0 2006-2-22
 */

public class SPLBRotateUpdateRoutingTableReplyBody extends Body implements Serializable 
{

	// private members
  	private static final long serialVersionUID = 1695521682674142757L;
  	
  	private PhysicalInfo physicalSender;
  	private LogicalInfo logicalSender;
  	private RoutingItemInfo infoRequester;
  	private int index;
  	private boolean direction;
  	private LogicalInfo logicalDestination;

  	/**
	 * Construct the message body with speicified parameters.
  	 * 
  	 * @param physicalSender physical address of the sender
  	 * @param logicalSender logical address of the sender
  	 * @param infoRequester information of a node, which is 
  	 * then put inside a routing table
  	 * @param index position of the node inside the routing table
  	 * @param direction direction of sending request
  	 * @param logicalDestination logical address of the receiver
  	 */
  	public SPLBRotateUpdateRoutingTableReplyBody(PhysicalInfo physicalSender, 
  			LogicalInfo logicalSender, RoutingItemInfo infoRequester, int index,
  			boolean direction, LogicalInfo logicalDestination)
  	{
  		this.physicalSender = physicalSender;
  		this.logicalSender = logicalSender;
  		this.infoRequester = infoRequester;
  		this.index = index;
  		this.direction = direction;
  		this.logicalDestination = logicalDestination;
  	}

	/**
	 * Construct the message body with a string value.
	 * 
	 * @param serializeData the string value that contains 
	 * the serialized message body
	 */
  	public SPLBRotateUpdateRoutingTableReplyBody(String serializeData)
  	{
  		String[] arrData = serializeData.split(":");	
  		try
  		{
  			this.physicalSender = new PhysicalInfo(arrData[1]);
  			this.logicalSender = new LogicalInfo(arrData[2]);
  			this.infoRequester = new RoutingItemInfo(arrData[3]);
  			this.index = Integer.parseInt(arrData[4]);
  			this.direction = Boolean.valueOf(arrData[5]).booleanValue();
  			this.logicalDestination = new LogicalInfo(arrData[6]);
  		}
  		catch(Exception e)
  		{
  			System.out.println("Incorrect serialize data at LBRotateUpdateRoutingTableReply:" + serializeData);
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
  	 * Get information of a node
  	 * 
  	 * @return information of the sender
  	 */
  	public RoutingItemInfo getInfoRequester()
  	{
  		return this.infoRequester;
  	}

  	/**
  	 * Get position of the node inside the routing table
  	 * 
  	 * @return position of the node inside the routing table
  	 */
  	public int getIndex()
  	{
  		return this.index;
  	}

  	/**
  	 * Get direction of sending request
  	 * 
  	 * @return direction of sending request
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

  		outMsg = "LBROTATEUPDATEROUTINGTABLEREPLY";
  		outMsg += "\n\t Physical Sender:" + physicalSender.toString();
  		outMsg += "\n\t Logical Sender:" + logicalSender.toString();
  		outMsg += "\n\t Info Requester:" + infoRequester.toString();
  		outMsg += "\n\t Index:" + index;
  		outMsg += "\n\t Direction:" + String.valueOf(direction);
  		outMsg += "\n\t Logical Destination:" + logicalDestination.toString();

  		return outMsg;
  	}

  	@Override
  	public String toString()
  	{
  		String outMsg;

  		outMsg = "LBROTATEUPDATEROUTINGTABLEREPLY";
  		outMsg += ":" + physicalSender.toString();
  		outMsg += ":" + logicalSender.toString();
  		outMsg += ":" + infoRequester.toString();
  		outMsg += ":" + index;
  		outMsg += ":" + String.valueOf(direction);
  		outMsg += ":" + logicalDestination.toString();

  		return outMsg;
  	}
  	
}
