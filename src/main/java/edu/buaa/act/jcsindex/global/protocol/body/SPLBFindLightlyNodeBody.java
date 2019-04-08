/*
 * @(#) SPLBFindLightlyNodeBody.java 1.0 2006-2-22
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.protocol.body;

import edu.buaa.act.jcsindex.global.peer.info.LogicalInfo;
import edu.buaa.act.jcsindex.global.peer.info.PhysicalInfo;

import java.io.Serializable;

/**
 * Implement the message body used for finding a lightly loaded
 * node for a heavily loaded node
 * 
 * @author Vu Quang Hieu
 * @version 1.0 2006-2-22
 */

public class SPLBFindLightlyNodeBody extends Body implements Serializable
{

	// private members
  	private static final long serialVersionUID = 4698557706502859621L;
  	
  	private PhysicalInfo physicalSender;
  	private LogicalInfo logicalSender;
  	private PhysicalInfo physicalOverloaded;
  	private LogicalInfo logicalOverloaded;
  	private int order;
  	private int nextIndex;
  	private boolean direction;
  	private int queryLoad;
  	private LogicalInfo logicalDestination;

  	/**
	 * Construct the message body with speicified parameters.
  	 * 
  	 * @param physicalSender physical address of the sender
  	 * @param logicalSender logical address of the sender
  	 * @param physicalOverloaded physical address of the overloaded node
  	 * @param logicalOverloaded logical address of the overloaded node
  	 * @param order order of the overloaded node
  	 * @param nextIndex the interval of searching, index of the next jump
  	 * @param direction <code>false</code> search towards the left
  	 * <code>true</code> search towards the right
  	 * @param queryLoad load of query messages at the overloaded node
  	 * @param logicalDestination logical address of the receiver
  	 */
  	public SPLBFindLightlyNodeBody(PhysicalInfo physicalSender, LogicalInfo logicalSender,
  								   PhysicalInfo physicalOverloaded, LogicalInfo logicalOverloaded,
  								   int order, int nextIndex, boolean direction, 
  								   int queryLoad, LogicalInfo logicalDestination)
  	{
  		this.physicalSender = physicalSender;
  		this.logicalSender = logicalSender;
  		this.physicalOverloaded = physicalOverloaded;
  		this.logicalOverloaded = logicalOverloaded;
  		this.order = order;
  		this.nextIndex = nextIndex;
  		this.direction = direction;
  		this.queryLoad = queryLoad;
  		this.logicalDestination = logicalDestination;
  	}

	/**
	 * Construct the message body with a string value.
	 * 
	 * @param serializeData the string value that contains 
	 * the serialized message body
	 */
  	public SPLBFindLightlyNodeBody(String serializeData)
  	{
  		String[] arrData = serializeData.split(":");
  		try
  		{
  			this.physicalSender = new PhysicalInfo(arrData[1]);
  			this.logicalSender = new LogicalInfo(arrData[2]);
  			this.physicalOverloaded = new PhysicalInfo(arrData[3]);
  			this.logicalOverloaded = new LogicalInfo(arrData[4]);
  			this.order = Integer.parseInt(arrData[5]);
  			this.nextIndex = Integer.parseInt(arrData[6]);
  			this.direction = Boolean.valueOf(arrData[7]).booleanValue();
  			this.queryLoad = Integer.parseInt(arrData[8]);
  			this.logicalDestination = new LogicalInfo(arrData[9]);
  		}
  		catch(Exception e)
  		{
  			System.out.println("Incorrect serialize data at LBFindLightlyNode:" + serializeData);
  			System.out.println(e.getMessage());
  		}
  	}

  	/**
  	 * Update physical address of the sender
  	 * 
  	 * @param physicalSender physical address of the sender
  	 */
  	public void setPhysicalSender(PhysicalInfo physicalSender)
  	{
  		this.physicalSender = physicalSender;	
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
  	 * Update logical address of the sender
  	 * 
  	 * @param logicalSender logical address of the sender
  	 */
  	public void setLogicalSender(LogicalInfo logicalSender)
  	{
  		this.logicalSender = logicalSender;
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
  	 * Get physical address of the overloaded node
  	 * 
  	 * @return physical address of the overloaded node
  	 */
  	public PhysicalInfo getPhysicalOverloaded()
  	{
  		return this.physicalOverloaded;
  	}

  	/**
  	 * Get logical address of the overloaded node
  	 * 
  	 * @return logical address of the overloaded node
  	 */
  	public LogicalInfo getLogicalOverloaded()
  	{
  		return this.logicalOverloaded;
  	}

  	/**
  	 * Get order of the overloaded node
  	 * 
  	 * @return order of the overloaded node
  	 */
  	public int getOrder()
  	{
  		return this.order;
  	}

  	/**
  	 * Set the next interval jumping
  	 * 
  	 * @param nextIndex tnext interval jumping
  	 */
  	public void setNextIndex(int nextIndex)
  	{
  		this.nextIndex = nextIndex;
  	}

  	/**
  	 * Get the next interval jumping
  	 * 
  	 * @return next interval jumping
  	 */
  	public int getNextIndex()
  	{
  		return this.nextIndex;
  	}

  	/**
  	 * Get direction of searching
  	 * 
  	 * @return direction of searching
  	 */
  	public boolean getDirection()
  	{
  		return this.direction;
  	}

  	/**
  	 * Get query load of the overloaded node
  	 * 
  	 * @return query load of the overloaded node
  	 */
  	public int getQueryLoad()
  	{
  		return this.queryLoad;
  	}

  	/**
  	 * Update logical address of the receiver
  	 * 
  	 * @param logicalDestination logical address of the receiver
  	 */
  	public void setLogicalDestination(LogicalInfo logicalDestination)
  	{
  		this.logicalDestination = logicalDestination;
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

    	outMsg = "LBFINDLIGHTLYNODE";
    	outMsg += "\n\t Physical Sender:" + physicalSender.toString();
    	outMsg += "\n\t Logical Sender:" + logicalSender.toString();
    	outMsg += "\n\t Physical Overloaded:" + physicalOverloaded.toString();
    	outMsg += "\n\t Logical Overloaded:" + logicalOverloaded.toString();
    	outMsg += "\n\t Order:" + order;
    	outMsg += "\n\t Next Index:" + nextIndex;
    	outMsg += "\n\t Direction:" + direction;
    	outMsg += "\n\t Query Load:" + queryLoad;
    	outMsg += "\n\t Logical Destination:" + logicalDestination.toString();

    	return outMsg;
    }

    @Override
    public String toString()
    {
    	String outMsg;

    	outMsg = "LBFINDLIGHTLYNODE";
    	outMsg += ":" + physicalSender.toString();
    	outMsg += ":" + logicalSender.toString();
    	outMsg += ":" + physicalOverloaded.toString();
    	outMsg += ":" + logicalOverloaded.toString();
    	outMsg += ":" + order;
    	outMsg += ":" + nextIndex;
    	outMsg += ":" + direction;
    	outMsg += ":" + queryLoad;
    	outMsg += ":" + logicalDestination.toString();

    	return outMsg;
    }
    
}
