/*
 * @(#) NodeInfo.java 1.0 2006-2-6
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.peer.info;

import java.io.Serializable;

/**
 * An abstract class used for define the data structure 
 * of each node in the BATON tree.
 * 
 * @author Xu Linhao
 * @version 1.0 2006-2-6
 */

public abstract class NodeInfo implements Serializable 
{
	
	// private members
	protected PhysicalInfo physicalInfo;
	protected LogicalInfo logicalInfo;

	/**
	 * Construct the node.
	 * 
	 * @param physicalInfo the physical information of the node
	 * @param logicalInfo the logical information of the node
	 */
	public NodeInfo(PhysicalInfo physicalInfo, LogicalInfo logicalInfo)
	{
	    this.physicalInfo = physicalInfo;
	    this.logicalInfo = logicalInfo;
	}

	/**
	 * Construct the node with a serialized string value.
	 * 
	 * @param serializeData the serialized string value
	 */
	public NodeInfo(String serializeData)
	{
	    String[] arrData = serializeData.split("%");
	    try
	    {
	    	this.physicalInfo = new PhysicalInfo(arrData[0]);
	    	this.logicalInfo  = new LogicalInfo(arrData[1]);
	    }
	    catch (Exception e)
	    {
	    	e.printStackTrace();
	    	System.out.println("Incorrect serialize data at NodeInfo:" + serializeData);
	    }
	}

	/**
	 * Set the physical information of the node.
	 * 
	 * @param physicalInfo the physical information of the node
	 */
	public void setPhysicalInfo(PhysicalInfo physicalInfo)
	{
	    this.physicalInfo = physicalInfo;
	}

	/**
	 * Get the physical information of the node.
	 * 
	 * @return the physical information of the node
	 */
	public PhysicalInfo getPhysicalInfo()
	{
	    return this.physicalInfo;
	}

	/**
	 * Set the logical information of the node.
	 * 
	 * @param logicalInfo the logical information of the node
	 */
	public void setLogicalInfo(LogicalInfo logicalInfo)
	{
	    this.logicalInfo = logicalInfo;
	}

	/**
	 * Get the logical information of the node.
	 * 
	 * @return the logical information of the node
	 */
	public LogicalInfo getLogicalInfo()
	{
	    return this.logicalInfo;
	}

	@Override
	public String toString()
	{
	    return this.physicalInfo.toString() + "%" + this.logicalInfo.toString();
	}

	public String formatedInfo() {
		return "IP " + this.physicalInfo.getIP() + " Port " + this.getPhysicalInfo().getPort() + " Level " + this.getLogicalInfo().getLevel() + " Number " + this.logicalInfo.getNumber();
	}

	/**
	 * Get the serialized string representation of the <code>NodeInfo</code>.
	 * 
	 * @return the serialized string representation of the <code>NodeInfo</code>
	 */
	public String serialize()
	{
	    return this.physicalInfo.serialize() + "," + this.logicalInfo.serialize();
	}
	
}
