/*
 * @(#) ParentNodeInfo.java 1.0 2006-2-6
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.peer.info;

/**
 * Define the data structure of the parent node
 * of each node in the BATON tree.
 * 
 * @author Vu Quang Hieu
 * @author (Modified by) Xu Linhao
 * @version 1.0 2006-2-7
 */

public class ParentNodeInfo extends NodeInfo
{

	// private members
	private static final long serialVersionUID = 8478429952933278866L;

	/**
	 * Construct the parent node of each node.
	 * 
	 * @param physicalInfo the physical information of the node
	 * @param logicalInfo the logical information of the node
	 */
	public ParentNodeInfo(PhysicalInfo physicalInfo, LogicalInfo logicalInfo)
	{
		super(physicalInfo, logicalInfo);
	}

	/**
	 * Construct the node with a serialized string value.
	 * 
	 * @param serializeData the serialized string value
	 */
	public ParentNodeInfo(String serializeData)
	{
		super(serializeData);
	}

}
