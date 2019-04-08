/*
 * @(#) SPUpdateRoutingTableIndirectlyBody.java 1.0 2006-2-22
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
 * Implement the message body used for updating the routing table 
 * indirectly between two neighbor nodes via their parents 
 * 
 * @author Vu Quang Hieu
 * @version 1.0 2006-2-22
 */

public class SPUpdateRoutingTableIndirectlyBody extends Body implements Serializable 
{

	// private members
  	private static final long serialVersionUID = 154951949891083635L;

  	private PhysicalInfo physicalSender;
  	private LogicalInfo logicalSender;
  	private RoutingItemInfo infoSender;
  	private RoutingItemInfo infoChild;
  	private int index;
  	private boolean direction;
  	private boolean special;
  	private LogicalInfo logicalDestination;

  	/**
  	 * Construct the message body with specified parameters.
  	 * 
  	 * @param physicalSender physical address of the sender
  	 * @param logicalSender logical address of the sender
  	 * @param infoSender information of the sender
  	 * @param infoChild information of the new node
  	 * @param index index position of the sender inside routing table
  	 * @param direction direction of the sender
  	 * @param special <code>false</code>: update directly,
  	 * <code>true</code> only update for the new node
  	 * @param logicalDestination logical address of the receiver 
  	 */
  	public SPUpdateRoutingTableIndirectlyBody(PhysicalInfo physicalSender, LogicalInfo logicalSender,
  												RoutingItemInfo infoSender, RoutingItemInfo infoChild,
  												int index, boolean direction, boolean special,
  												LogicalInfo logicalDestination)
  	{
  		this.physicalSender = physicalSender;
  		this.logicalSender = logicalSender;
  		this.infoSender = infoSender;
  		this.infoChild = infoChild;
  		this.index = index;
  		this.direction = direction;
  		this.special = special;
  		this.logicalDestination = logicalDestination;
  	}

  	/**
  	 * Construct the message body with a string value.
  	 * 
  	 * @param serializeData the string value that contains 
  	 * the serialized message body
  	 */
  	public SPUpdateRoutingTableIndirectlyBody(String serializeData)
  	{
  		String[] arrData = serializeData.split(":");
  		try
  		{
  			this.physicalSender = new PhysicalInfo(arrData[1]);
  			this.logicalSender = new LogicalInfo(arrData[2]);
  			this.infoSender = new RoutingItemInfo(arrData[3]);
  			this.infoChild = new RoutingItemInfo(arrData[4]);
  			this.index = Integer.parseInt(arrData[5]);
  			this.direction = Boolean.valueOf(arrData[6]).booleanValue();
  			this.special = Boolean.valueOf(arrData[7]).booleanValue();
  			this.logicalDestination = new LogicalInfo(arrData[8]);
  		}
  		catch(Exception e)
  		{
  			System.out.println("Incorrect serialize data at UpdateRoutingTableIndirectly:" + serializeData);
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
  	 * Get information of the sender node
  	 * 
  	 * @return information of the sender node
  	 */
  	public RoutingItemInfo getInfoSender()
  	{
  		return this.infoSender;
  	}

  	/**
  	 * Get information of the new node
  	 * 
  	 * @return information of the new node
  	 */
  	public RoutingItemInfo getInfoChild()
  	{
  		return this.infoChild;
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

  	/**
  	 * Get special flag
  	 * 
  	 * @return <code>false</code>: update directly,
  	 * <code>true</code> only update for the new node
  	 */
  	public boolean getSpecial()
  	{
  		return this.special;
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

  		outMsg = "UPDATEROUTINGTABLEINDIRECTLY";
  		outMsg += "\n\t Physical Sender:" + physicalSender.toString();
    	outMsg += "\n\t Logical Sender:" + logicalSender.toString();
    	outMsg += "\n\t Info Requester:" + infoSender.toString();
    	outMsg += "\n\t Info Child:" + infoChild.toString();
    	outMsg += "\n\t Index:" + index;
    	outMsg += "\n\t Direction:" + String.valueOf(direction);
    	outMsg += "\n\t Special:" + String.valueOf(special);
    	outMsg += "\n\t Logical Destination:" + logicalDestination.toString();

    	return outMsg;
  	}

  	@Override
  	public String toString()
  	{
  		String outMsg;

  		outMsg = "UPDATEROUTINGTABLEINDIRECTLY";
  		outMsg += ":" +  physicalSender.toString();
  		outMsg += ":" + logicalSender.toString();
  		outMsg += ":" + infoSender.toString();
  		outMsg += ":" + infoChild.toString();
  		outMsg += ":" + index;
  		outMsg += ":" + String.valueOf(direction);
  		outMsg += ":" + String.valueOf(special);
  		outMsg += ":" + logicalDestination.toString();

  		return outMsg;
  	}
  	
}
