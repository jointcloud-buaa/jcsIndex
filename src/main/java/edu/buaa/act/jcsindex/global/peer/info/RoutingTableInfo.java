/*
 * @(#) RoutingTableInfo.java 1.0 2006-2-6
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.peer.info;

import java.io.Serializable;
import java.util.Vector;

/**
 * Implement a routing table that is composed of a set of
 * <code>RoutingItemInfo</code>.
 *  
 * @author Vu Quang Hieu
 * @author (Modified by) Xu Linhao
 * @version 1.0 2006-2-6
 */

public class RoutingTableInfo implements Serializable 
{

	// private members
	private static final long serialVersionUID = 5482422471695677556L;

	private Vector<RoutingItemInfo> nodeList;
	private int tableSize;

	/**
	 * Construct an empty routing table with specified table size.
	 * 
	 * @param tableSize the size of the routing table
	 */
	public RoutingTableInfo(int tableSize) 
	{
    	/*
    	 * commented by Xu Linhao on 2006-2-6
    	 * it is no use to do such initilization
    	 * 
	    if (tableSize > 0)
	    {
	    	nodeList = new Vector(tableSize);
	    	for (int i = 0; i < tableSize; i++)
	    		nodeList.add(null);
	    }
	    this.tableSize = tableSize;
    	*/
		this.tableSize = tableSize;
		nodeList = new Vector<RoutingItemInfo>(tableSize);
    	for (int i = 0; i < tableSize; i++)
    		nodeList.add(null);
	}

	/**
	 * Constructing a routing table with serialized data objects.
	 * 
	 * @param serializeData the serialized data objects
	 */
	public RoutingTableInfo(String serializeData)
	{
		/*
		 * personal comments by Xu Linhao on 2006-2-6:
		 * Such way to initilize a constructor is not recommended
		 * and so does passing data objects since the data structure
		 * is obscure. So, in the next version, all of such
		 * things should be removed and updated!
		 */
	    String[] arrData = serializeData.split("#");
	    try
	    {
	    	this.tableSize = Integer.parseInt(arrData[0]);
	    	if (this.tableSize > 0)
	    	{
	    		String[] arrNodeList = arrData[1].split("@");
	    		nodeList = new Vector<RoutingItemInfo>(this.tableSize);
	    		for (int i = 0; i < this.tableSize; i++)
	    		{
	    			if (arrNodeList[i].equals("null"))
	    				nodeList.add(null);
	    			else
	    				nodeList.add(new RoutingItemInfo(arrNodeList[i]));
	    		}
	    	}
	    }
	    catch (Exception e)
	    {
	    	System.out.println("Incorrect serialize data at RoutingTableInfo:" + serializeData);
	    	e.printStackTrace();
	    	/*
	    	 * commented by Xu Linhao on 2006-2-6
	    	 * System.out.println(e.getMessage());
	    	 */
	    }
	}

	/**
	 * @deprecated since <code>RoutingTableInfo(int)</code> is enough.
	 * 
	 * Construct an empty routing table with specified table size.
	 * 
	 * @param tableSize the size of the routing table
	 */
	public void resetRoutingTable(int tableSize)
	{
	    if (tableSize > 0)
	    {
	    	nodeList = new Vector<RoutingItemInfo>(tableSize);
	    	for (int i = 0; i < tableSize; i++)
	    		nodeList.add(null);
	    }
	    this.tableSize = tableSize;
	}

	/**
	 * @deprecated since this method is not safe.
	 * This method is replaced by <code>getRoutingTableNode(int)</code>.
	 * 
	 * @return the routing table
	 */
	public Vector getNodeList()
	{
	    return this.nodeList;
	}

	/**
	 * Get the size of the routing table.
	 * <p>
	 * Notice that some items in the routing table
	 * may be NULL.
	 * 
	 * @return the size of the routing table
	 */
	public int getTableSize()
	{
	    return this.tableSize;
	}
	
	/**
	 * Get a routing item at specified position. 
	 * 
	 * @param idx the position of the routing item
	 * @return an instance of <code>RoutingTableNodeInfo</code>
	 */
	public RoutingItemInfo getRoutingTableNode(int idx)
	{
		if ((idx < 0) || (idx > this.tableSize))
			throw new IllegalArgumentException("Out of index bound");
		
		return nodeList.get(idx);
	}
	
	/**
	 * Set a routing item into the routing table.
	 * 
	 * @param info the item of the routing table 
	 * @param idx the position that the item will be inserted
	 */
	public void setRoutingTableNode(RoutingItemInfo info, int idx)
	{
		nodeList.setElementAt(info, idx);
	}

	@Override
	public String toString()
	{
	    RoutingItemInfo tmpInfo;
	    String outMsg;
	    outMsg = String.valueOf(tableSize);
	    if (tableSize == 0)
	    	outMsg += "#null";
	    else
	    {
	    	outMsg += "#";
	    	for (int i = 0; i < tableSize - 1; i++)
	    	{
	    		tmpInfo = nodeList.get(i);
	        	if (tmpInfo == null)
	        		outMsg += "null@";
	        	else
	        		outMsg += tmpInfo.toString() + "@";
	    	}
	    	tmpInfo = nodeList.get(tableSize - 1);
	    	if (tmpInfo == null)
	    		outMsg += "null";
	    	else
	    		outMsg += tmpInfo.toString();
	    }
	    return outMsg;
	}

	/**
	 * Get the serialized string representation of the <code>RoutingTableInfo</code>.
	 * 
	 * @return the serialized string representation of the <code>RoutingTableInfo</code>
	 */
	public String serialize()
	{
	    RoutingItemInfo temptInfo;
	    String outMsg;
	    outMsg = "TableSize=" + tableSize;
	    if (tableSize == 0)
	    	outMsg += ",NodeList=null";
	    else
	    {
	    	outMsg += ",NodeList=";
	    	for (int i = 0; i < tableSize; i++)
	    	{
	    		temptInfo = nodeList.get(i);
	    		if (temptInfo == null)
	    			outMsg += "null:";
	    		outMsg += temptInfo.toString() + ":";
	    	}
	    	outMsg += nodeList.get(tableSize - 1).toString();
	    }
	    return outMsg;
	}
	
}
