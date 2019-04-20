/*
 * @(#) RoutingItemInfo.java 1.0 2006-2-6
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.peer.info;

import java.io.Serializable;

/**
 * Define the data structure of the routing item in the routing table. 
 * 
 * @author Vu Quang Hieu
 * @author (Modified by) Xu Linhao
 * @version 1.0 2006-2-6
 */

public class RoutingItemInfo implements Serializable 
{

	// private members
	private static final long serialVersionUID = -4880422189973227410L;

	private PhysicalInfo physicalInfo;
	private LogicalInfo logicalInfo;
	private ChildNodeInfo leftChild;
	private ChildNodeInfo rightChild;
	private BoundaryValue minValue;
	private BoundaryValue maxValue;

	/**
	 * Construct a routing item.
	 * 
	 * @param physicalInfo the physical information of the node
	 * @param logicalInfo the logical information of the node
	 * @param leftChild the left child of the node
	 * @param rightChild the right child of the node
	 * @param minValue the minimum index key
	 * @param maxValue the maximum index key
	 */
	public RoutingItemInfo(PhysicalInfo physicalInfo, LogicalInfo logicalInfo,
			ChildNodeInfo leftChild, ChildNodeInfo rightChild,
			BoundaryValue minValue, BoundaryValue maxValue)
	{
	    this.physicalInfo 	= physicalInfo;
	    this.logicalInfo	= logicalInfo;
	    this.leftChild 		= leftChild;
	    this.rightChild 	= rightChild;
	    this.minValue 		= minValue;
	    this.maxValue 		= maxValue;
	}

	/**
	 * Construct a routing item with a serialized string value.
	 * 
	 * @param serializeData the serialized string value
	 */
	public RoutingItemInfo(String serializeData)
	{
	    String[] arrData = serializeData.split("&");
	    try
	    {
	    	this.physicalInfo = new PhysicalInfo(arrData[0]);
	    	this.logicalInfo  = new LogicalInfo(arrData[1]);
	    	
	    	if (arrData[2].equals("null"))
	    		this.leftChild = null;
	    	else
	    		this.leftChild = new ChildNodeInfo(arrData[2]);
	    	
	    	if (arrData[3].equals("null"))
	    		this.rightChild = null;
	    	else
	    		this.rightChild = new ChildNodeInfo(arrData[3]);
	    	
	    	this.minValue = new BoundaryValue(arrData[4]);
	    	this.maxValue = new BoundaryValue(arrData[5]);
	    }
	    catch (Exception e)
	    {
	    	e.printStackTrace();
	    	System.out.println("Incorrect serialize data at RoutingTableNodeInfo:" + serializeData);
	    	/*
	    	 * commented by Xu Linhao on 2006-2-6
	    	 * System.out.println(e.getMessage());
	    	 */
	    }
	}

	/**
	 * Set the physical information of the node
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
	 * Set the right child of the current node.
	 * 
	 * @return the right child of the current node
	 */
	public LogicalInfo getLogicalInfo()
	{
	    return this.logicalInfo;
	}

	/**
	 * Set the left child of the current node.
	 * 
	 * @param leftChild the left child of the current node
	 */
	public void setLeftChild(ChildNodeInfo leftChild)
	{
	    this.leftChild = leftChild;
	}

	/**
	 * Get the left child of the current node.
	 * 
	 * @return the left child of the current node
	 */
	public ChildNodeInfo getLeftChild()
	{
	    return this.leftChild;
	}

	/**
	 * Set the right child of the current node.
	 * 
	 * @param rightChild the right child of the current node
	 */
	public void setRightChild(ChildNodeInfo rightChild)
	{
	    this.rightChild = rightChild;
	}

	/**
	 * Get the right child of the current node.
	 * 
	 * @return the right child of the current node
	 */
	public ChildNodeInfo getRightChild()
	{
	    return this.rightChild;
	}

	/**
	 * Get the minimum index key.
	 * 
	 * @return the minimum index key
	 */
	public BoundaryValue getMinValue()
	{
	    return this.minValue;
	}

	/**
	 * Get the maximum index key.
	 * 
	 * @return the maximum index key
	 */
	public BoundaryValue getMaxValue()
	{
	    return this.maxValue;
	}

	@Override
	public String toString()
	{
	    String outMsg;

	    outMsg = "(physicalInfo, " + physicalInfo.toString() + ") ";
	    outMsg += "(logicalInfo, " + logicalInfo.toString() + ") ";
	    outMsg += "(leftChild, ";
	    if (leftChild == null)
	    	outMsg += "null) ";
	    else
	    	outMsg += leftChild.toString() + ")";
	    outMsg += "(rightChild, ";
	    if (rightChild == null)
	    	outMsg += "null)";
	    else
	    	outMsg += rightChild.toString() + ")";
	    // TODO: 只输出数字
		outMsg += "(NodeRange, ";
	    outMsg += this.minValue.getLongValue() + " ";
	    outMsg += this.maxValue.getLongValue() + ")";

	    return outMsg;
	}

	/**
	 * Get the serialized string representation of the <code>RoutingItemInfo</code>.
	 * 
	 * @return the serialized string representation of the <code>RoutingItemInfo</code>
	 */
	public String serialize()
	{
	    return (physicalInfo.toString() + "," + logicalInfo.toString() + "," +
	    		leftChild.toString() + "," + rightChild.toString() + ",MinValue=" + 
	    		minValue.toString() + ",MaxValue=" + maxValue.toString());
	}
	
}
