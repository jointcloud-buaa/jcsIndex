/*
 * @(#) PeerInfo.java 1.0 2006-1-24
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.peer.info;

import edu.buaa.act.jcsindex.global.utils.Tools;

import java.io.Serializable;

/**
 * This class is defined the format of the feedback information
 * to the peers who is registering or signing in the system.
 * According to such feedback information, then the peers can 
 * select a proper super peer to join the system.
 * 
 * @author Xu Linhao
 * @version 1.0 2006-1-14
 */

public class PeerInfo implements Cloneable, Serializable
{

	/**
	 * The serialVersionUID is used for serializing and de-serializing
	 * this class and be SURE NOT CHANGE THIS VALUE!
	 */
	private static final long serialVersionUID = 7238657095543061582L;
	
	private String peerID;
	private String ip;
	private int	   port;
	private String type;
	
	/**
	 * Wrap a peer's information that will be used by other peers
	 * to access this peer. 
	 * 
	 * @param peerID the identifier of the peer
	 * @param ip the IP address of the peer
	 * @param port the port used for monitoring network events
	 * @param type the peer type
	 */
	public PeerInfo(String peerID, String ip, int port, String type)
	{
		this.peerID = peerID;
		this.ip 	= ip;
		this.port 	= port;
		this.type	= type;
	}
	
	/**
	 * Clone self.
	 * 
	 * @return The instance of <code>PeerInfo</code>.
	 */
	public Object clone()
	{
		PeerInfo instance = null;
		try
		{
			instance = (PeerInfo)super.clone();
		}
		catch (CloneNotSupportedException e)
		{
			e.printStackTrace();
		}
		return instance;
	}
	
	/**
	 * Returns a 160-bits key by using SHA hash function.
	 * 
	 * @return a 160-bits key by using SHA hash function
	 */
	public String getKey()
	{
//		String key = ip + "@" + port + ":" + peerID;
		String key = ip + "@" + port;
		return Tools.getDigest(key.getBytes());
	}
	
	/**
	 * Get the peer identifier.
	 * 
	 * @return the peer identifier
	 */
	public String getPeerID()
	{
		return this.peerID;
	}
	
	/**
	 * Set the peer identifier.
	 * 
	 * @param id the peer identifier
	 */
	public void setPeerID(String id)
	{
		this.peerID = id;
	}
	
	/**
	 * Get the IP address.
	 * 
	 * @return the IP address
	 */
	public String getInetAddress()
	{
		return this.ip;
	}
	
	/**
	 * Set the IP address.
	 * 
	 * @param ip the IP address
	 */
	public void setInetAddress(String ip)
	{
		this.ip = ip;
	}
	
	/**
	 * Get the port used for monitoring network events.
	 * 
	 * @return the port used for monitoring network events
	 */
	public int getPort()
	{
		return this.port;
	}
	
	/**
	 * Set the port.
	 * 
	 * @param port the port
	 */
	public void setPort(int port)
	{
		this.port = port;
	}
	
	/**
	 * Get the peer type.
	 * 
	 * @return the peer type
	 */
	public String getPeerType()
	{
		return this.type;
	}
	
	/**
	 * Set the peer type.
	 * 
	 * @param type the peer type
	 */
	public void setPeerType(String type)
	{
		this.type = type;
	}
	
	/**
	 * Determine if two instances of the <code>PeerInfo</code> are equal.
	 * 
	 * @param p the instance of the <code>PeerInfo</code> to be compared
	 * @return if equal, return <code>true</code>;
	 * 			otherwise, return <code>false</code>
	 */
	public boolean equals(PeerInfo p)
	{
		if (this.peerID.equals(p.peerID) && this.ip.equals(p.ip) 
				&& (this.port == p.port) && this.type.equals(p.type))
			return true;
		return false;
	}
	
	/**
	 * Determine if two instances of <code>PeerInfo</code> are equal
	 * without considering both type and identifier.
	 * 
	 * @param p the instance of the <code>PeerInfo</code> to be compared
	 * @return if equal, return <code>true</code>
	 */
	public boolean equalsIgnoreType(PeerInfo p)
	{
		if (this.ip.equals(p.ip) && (this.port == p.port))
			return true;
		return false;
	}
	
	/**
	 * Get an array of all members.
	 * 
	 * @return an array of all members
	 */
	public Object[] toObjectArray()
	{
		Object[] result = new Object[4];
		
		result[0] = this.peerID;
		result[1] = this.type;
		result[2] = this.ip;
		result[3] = this.port;
		
		return result;
	}

	/**
	 * Get an array of all members without type.
	 * 
	 * @return an array of all members
	 */
	public Object[] toObjectArrayWithoutType()
	{
		Object[] result = new Object[3];
		
		result[0] = this.peerID;
		result[1] = this.ip;
		result[2] = this.port;
		
		return result;
	}

	/**
	 * Override <code>toString()</code> function of <code>java.lang.Object</code>.
	 * 
	 * @return A string that describes the content of the body.
	 */
	public String toString()
	{
		return "PeerID: " + this.peerID + " InetAddress: " + this.ip
						+ " Port: " + this.port + " Type: " + this.type;
	}
	
}
