/*
 * @(#) PeerType.java 1.0 2006-1-6
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.peer;

/**
 * This class defines the peer type.
 * 
 * @author Xu Linhao
 * @version 1.0 2006-1-6
 */

public enum PeerType 
{

	// define types
	
	/**
	 * the string identifier representing the certificate authority
	 */
	CERTAUTHORITY	("CERTAUTHORITY"),
	
	/**
	 * the string identifier representing the bootstrap server
	 */
	BOOTSTRAP 		("BOOTSTRAP"),
	
	/**
	 * the string identifier representing the super peer
	 */
	SUPERPEER		("SUPERPEER"),
	
	/**
	 * the string identifier respresenting the client peer
	 */
	CLIENTPEER		("CLIENTPEER");
	
	private String value;
	
	PeerType(String value)
	{
		this.value = value;
	}
	
	/**
	 * Get the peer type.
	 * 
	 * @return the peer type
	 */
	public String getValue()
	{
		return this.value;
	}
	
	/**
	 * Check if the peer type exists.
	 * 
	 * @param peerType the peer type
	 * @return <code>true</code> if exists; otherwise, <code>false</code>
	 */
	public static boolean checkValue(String peerType)
	{
		for (PeerType m: PeerType.values())
		{
			if (m.getValue().equals(peerType))
			{
				return true;
			}
		}
		return false;
	}

}
