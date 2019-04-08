/*
 * @(#) ChildNodeInfo.java 1.0 2006-2-6
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.peer.info;

/**
 * Define the structure of the child node
 * of each node in the BATON tree.
 * 
 * @author Vu Quang Hieu
 * @author (Modified by) Xu Linhao
 * @version 1.0 2006-2-6
 */

public class ChildNodeInfo extends NodeInfo
{
	
	// private members
	private static final long serialVersionUID = -2456707702722294767L;
	
	/**
	 * Construct the node.
	 * 
	 * @param physicalInfo the physical information of the node
	 * @param logicalInfo the logical information of the node
	 */
	public ChildNodeInfo(PhysicalInfo physicalInfo, LogicalInfo logicalInfo)
	{
		super(physicalInfo, logicalInfo);
	}

	/**
	 * Construct the node with a serialized string value.
	 * 
	 * @param serializeData the serialized string value
	 */
	public ChildNodeInfo(String serializeData)
	{
		super(serializeData);
	}

}
