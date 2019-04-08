/*
 * @(#) PhysicalInfo.java 1.0 2006-2-6
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.peer.info;

import java.io.Serializable;

/**
 * Keeping the physical information of the node,
 * i.e., IP address and port.
 * 
 * @author Vu Quang Hieu
 * @author (Modified by) Xu Linhao
 * @version 1.0 2006-2-6
 */

public class PhysicalInfo implements Serializable
{
	
	// private members
	private static final long serialVersionUID = 4670973360151950526L;

	private String ip;
	private int port;

	/**
	 * Construct the physical information of the node 
	 * with IP address and port.
	 * 
	 * @param ip the IP address of the node
	 * @param port the port
	 */
	public PhysicalInfo(String ip, int port) 
	{
	    this.ip = ip;
	    this.port = port;
	}

	/**
	 * Construct the physical information of the node
	 * with a serialized string value.
	 * 
	 * @param serializeData the serialized string value
	 */
	public PhysicalInfo(String serializeData)
	{
	    String[] arrData = serializeData.split("_");
	    try
	    {
	    	this.ip = arrData[0];
	    	this.port = Integer.parseInt(arrData[1]);
	    }
	    catch (Exception e)
	    {
	    	e.printStackTrace();
	    	System.out.println("Incorrect serialize data at PhysicalInfo:" + serializeData);
	    	/*
	    	 * commented by Xu Linhao on 2006-2-6
	    	 * System.out.println(e.getMessage());
	    	 */
	    }
	}

	/**
	 * Get the IP address of the node.
	 * 
	 * @return the IP address of the node
	 */
	public String getIP()
	{
	    return this.ip;
	}

	/**
	 * Get the port used for monitoring network events.
	 * 
	 * @return the port
	 */
	public int getPort()
	{
	    return this.port;
	}

	/**
	 * Determine whether two <code>PhysicalInfo</code> are equal.
	 * 
	 * @return if equal, return <code>true</code>;
	 * 			otherwise, return <code>false</code>
	 */
	public boolean equals(Object comparedObject)
	{
	    if (comparedObject instanceof PhysicalInfo)
	    {
	    	PhysicalInfo comparedInfo = (PhysicalInfo)comparedObject;
	    	return ((this.ip.equals(comparedInfo.ip)) && (this.port == comparedInfo.port));
	    }
    	/*
    	 * commented by Xu Linhao on 2006-2-6
    	 * return false;
    	 */
    	throw new IllegalArgumentException("Type mismatch");
	}

	@Override
	public String toString(){
	    return this.ip + "_" + this.port;
	}

	/**
	 * Get the serialized string representation of the <code>PhysicalInfo</code>.
	 * 
	 * @return the serialized string representation of the <code>PhysicalInfo</code>
	 */
	public String serialize(){
	    return "IP=" + this.ip + ",Port=" + this.port;
	}
	
}
