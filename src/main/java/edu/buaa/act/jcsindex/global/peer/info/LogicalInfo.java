/*
 * @(#) LogicalInfo.java 1.0 2006-2-6
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.peer.info;

import java.io.Serializable;

/**
 * Define the position of the node in the BATON tree, 
 * i.e., the logical identifier of the node 
 * with both number and level.
 * 
 * @author Vu Quang Hieu
 * @author (Modified by) Xu Linhao
 * @version 1.0 2006-2-6
 */

public class LogicalInfo implements Serializable
{

	// private members
	private static final long serialVersionUID = -1478714912822834878L;

	private int level;
	private int number;

	/**
	 * Construct the logical identifier of the node.
	 * 
	 * @param level the level of the node in the tree
	 * @param number the number of the node in the level
	 */
	public LogicalInfo(int level, int number) 
	{
		this.level = level;
	    this.number = number;
	}

	/**
	 * Construct the logical identifier of the node
	 * with a serialized string.
	 * 
	 * @param serializeData the serialized string used for constructing
	 * 						the <code>LogicalInfo</code>
	 */
	public LogicalInfo(String serializeData)
	{
	    String[] arrData = serializeData.split("_");
	    try
	    {
	    	this.level  = Integer.parseInt(arrData[0]);
	    	this.number = Integer.parseInt(arrData[1]);
	    }
	    catch (Exception e)
	    {
	    	e.printStackTrace();
	    	System.out.println("Incorrect serialize data at LogicalInfo:" + serializeData);
	    	/*
	    	 * commented by Xu Linhao on 2006-2-6
	    	 * System.out.println(e.getMessage());
	    	 */
	    }
	}

	/**
	 * Get the level of the node in the tree.
	 * 
	 * @return the level of the node in the tree
	 */
	public int getLevel()
	{
	    return this.level;
	}

	/**
	 * Get the position of the node in the level of the tree.
	 * 
	 * @return the position of the node in the level of the tree
	 */
	public int getNumber()
	{
	    return this.number;
	}

	/**
	 * Determine if two <code>LogicalInfo</code> is equal.
	 * 
	 * @return if equal, return <code>true</code>;
	 * 			otherwise, return <code>false</code>
	 */
	public boolean equals(Object comparedObject)
	{
	    if (comparedObject instanceof LogicalInfo)
	    {
	    	LogicalInfo comparedInfo = (LogicalInfo)comparedObject;
	    	return ((this.level == comparedInfo.level) 
	    			&& (this.number == comparedInfo.number));
	    }
    	/*
    	 * commented by Xu Linhao on 2006-2-6
    	 * return false;
    	 */
    	throw new IllegalArgumentException("Type mismatch");
	}

	@Override
	public String toString()
	{
	    return this.level + "_" + this.number;
	}

	/**
	 * Get the serialized string representation of the <code>LogicalInfo</code>.
	 * 
	 * @return the serialized string representation of the <code>LogicalInfo</code>
	 */
	public String serialize()
	{
	    return "Level=" + this.level + ",Number=" + this.number;
	}
	
}
