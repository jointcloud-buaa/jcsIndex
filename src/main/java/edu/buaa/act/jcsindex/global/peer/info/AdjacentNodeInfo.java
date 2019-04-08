/*
 * @(#) AdjacentNodeInfo.java 1.0 2006-2-7
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.peer.info;

/**
 * Define the data structure of the adjacent node
 * of each node in the BATON tree.
 * 
 * @author Vu Quang Hieu
 * @author (Modified by) Xu Linhao
 * @version 1.0 2006-2-7
 */ 

public class AdjacentNodeInfo extends NodeInfo 
{
	
	// private members
	private static final long serialVersionUID = 1052278269815622662L;
	
	/**
	 * Construct the adjacent node.
	 * 
	 * @param physicalInfo the physical information of the node
	 * @param logicalInfo the logical information of the node
	 */
	public AdjacentNodeInfo(PhysicalInfo physicalInfo, LogicalInfo logicalInfo)
	{
		super(physicalInfo, logicalInfo);
	}

	/**
	 * Construct the node with a serialized string value.
	 * 
	 * @param serializeData the serialized string value
	 */
	public AdjacentNodeInfo(String serializeData)
	{
		super(serializeData);
	}

}
