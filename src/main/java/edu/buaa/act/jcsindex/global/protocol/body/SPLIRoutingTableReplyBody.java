/*
 * @(#) SPLIRoutingTableReplyBody 1.0 2006-12-03
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.protocol.body;

import edu.buaa.act.jcsindex.global.peer.info.BoundaryValue;
import edu.buaa.act.jcsindex.global.peer.info.LogicalInfo;
import edu.buaa.act.jcsindex.global.peer.info.PhysicalInfo;
import edu.buaa.act.jcsindex.global.peer.info.RoutingItemInfo;

import java.io.Serializable;

/**
 * Implement the message body used for replying SPLIRoutingTable protocol
 * 
 * @author Vu Quang Hieu
 * @version 1.0 2006-2-22
 */

public class SPLIRoutingTableReplyBody extends Body implements Serializable
{

	// constant definition for direction
	public static final boolean FROM_LEFT_TO_RIGHT = false;
	public static final boolean FROM_RIGHT_TO_LEFT = true;
	
	private static final long serialVersionUID = -4197576926482859908L;

	private PhysicalInfo physicalSender;
	private LogicalInfo logicalSender;
	private RoutingItemInfo infoRequester;
	private int index;
	private boolean direction;
	private BoundaryValue oldMin;
	private BoundaryValue oldMax;
	private LogicalInfo logicalDestination;

	/**
	 * Construct the message body with specified parameters.
	 * 
	 * @param physicalSender physical address of the sender
	 * @param logicalSender logical address of the sender
	 * @param infoRequester information of the sender
	 * @param index index of the sender in routing table
	 * @param direction direction of the sender
	 * @param logicalDestination logical address of the receiver
	 */
	public SPLIRoutingTableReplyBody(PhysicalInfo physicalSender, LogicalInfo logicalSender, 
			RoutingItemInfo infoRequester, int index, boolean direction, BoundaryValue oldMin,
			BoundaryValue oldMax, LogicalInfo logicalDestination)
	{
		this.physicalSender = physicalSender;
		this.logicalSender = logicalSender;
		this.infoRequester = infoRequester;
		this.index = index;
		this.direction = direction;
		this.oldMin = oldMin;
		this.oldMax = oldMax;
		this.logicalDestination = logicalDestination;
	}

	/**
	 * Construct the message body with a string value.
	 * 
	 * @param serializeData the string value that contains 
	 * the serialized message body
	 */
	public SPLIRoutingTableReplyBody(String serializeData)
	{
		String[] arrData = serializeData.split(":");
		try
		{
			this.physicalSender = new PhysicalInfo(arrData[1]);
			this.logicalSender = new LogicalInfo(arrData[2]);
			this.infoRequester = new RoutingItemInfo(arrData[3]);
			this.index = Integer.parseInt(arrData[4]);
			this.direction = Boolean.valueOf(arrData[5]).booleanValue();
			this.oldMin = new BoundaryValue(arrData[6]);
			this.oldMax = new BoundaryValue(arrData[7]);
			this.logicalDestination = new LogicalInfo(arrData[8]);
		}
		catch(Exception e)
		{
			System.out.println("Incorrect serialize data at UpdateRoutingTableReply:" + serializeData);
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
	 * Get information of the sender
	 * 
	 * @return information of the sender
	 */
	public RoutingItemInfo getInfoRequester()
	{
		return this.infoRequester;
	}

	/**
	 * Get index of the sender in routing table
	 * 
	 * @return index of the sender
	 */
	public int getIndex()
	{
		return this.index;
	}

	/**
	 * Get direction of the sender
	 * 
	 * @return direction of the sender
	 */
	public boolean getDirection()
	{
		return this.direction;
	}

	public BoundaryValue getOldMin(){
		return this.oldMin;
	}
	
	public BoundaryValue getOldMax(){
		return this.oldMax;
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

		outMsg = "LIROUTINGTABLEREPLY";
		outMsg += "\n\t Physical Sender:" + physicalSender.toString();
		outMsg += "\n\t Logical Sender:" + logicalSender.toString();
		outMsg += "\n\t Info Requester:" + infoRequester.toString();
		outMsg += "\n\t Index:" + index;
		outMsg += "\n\t Direction:" + String.valueOf(direction);
		outMsg += "\n\t Old Min:" + oldMin.toString();
		outMsg += "\n\t Old Max:" + oldMax.toString();
		outMsg += "\n\t Logical Destination:" + logicalDestination.toString();

		return outMsg;
	}

	@Override
	public String toString()
	{
		String outMsg;

		outMsg = "LIROUTINGTABLEREPLY";
		outMsg += ":" + physicalSender.toString();
		outMsg += ":" + logicalSender.toString();
		outMsg += ":" + infoRequester.toString();
		outMsg += ":" + index;
		outMsg += ":" + String.valueOf(direction);
		outMsg += ":" + oldMin.toString();
		outMsg += ":" + oldMax.toString();
		outMsg += ":" + logicalDestination.toString();

		return outMsg;
	}
	
}
