/*
 * @(#) FeedbackBody.java 1.0 2006-1-4
 * 
 * Copyright 2005, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.protocol.body;

import edu.buaa.act.jcsindex.global.peer.info.PeerInfo;

/**
 * Implement confirmation data type with online peer information.
 * <p>
 * The data value in confirmation body is empty.
 * 
 * @author Xu Linhao
 */
public class FeedbackBody extends Body
{
	/**
	 * The serialVersionUID is used for serializing and de-serializing
	 * this class and be SURE NOT CHANGE THIS VALUE!
	 */
	private static final long serialVersionUID = -79589139208560390L;

	private PeerInfo[] onlineSuperPeers;
	
	/**
	 * Construct an empty body.
	 */
	public FeedbackBody()
	{
		// do nothing
	}
	
	/**
	 * Construct a body with data objects.
	 * 
	 * @param data the data objects to be feedback
	 */
	public FeedbackBody(PeerInfo[] data)
	{
		this.onlineSuperPeers = data;
	}
	
	/**
	 * Set an array of online super peers to the message body.
	 * 
	 * @param data an array of online super peers
	 */
	public void setOnlineSuperPeers(PeerInfo[] data)
	{
		this.onlineSuperPeers = data;
	}
	
	/**
	 * Get all online super peers in message body.
	 * 
	 * @return all online super peers
	 */
	public PeerInfo[] getOnlineSuperPeers()
	{
		return onlineSuperPeers.clone();
	}
	
	/**
	 * Get the i-th online super peers.
	 * 
	 * @param i the index number
	 * @return the instance of <code>PeerInfo</code>
	 */
	public PeerInfo getOnlineSuperPeers(int i)
	{
		if (i < 0 || i >= onlineSuperPeers.length)
		{
			throw new IllegalArgumentException("Out of index bound");
		}
		return (PeerInfo)onlineSuperPeers[i].clone();
	}
	
	/**
	 * Override <code>toString()</code> function of <code>java.lang.Object</code>.
	 * 
	 * @return A string that describes the content of the body.
	 */
	public String toString()
	{
		String result = "";
		
		for (int i = 0; i < onlineSuperPeers.length; i++)
		{
			result += onlineSuperPeers[i].toString() + "\r\n";
		}
		
		return result;
	}
}