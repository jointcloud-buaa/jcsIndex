/*
 * @(#) Inet.java 1.0 2006-2-1
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.utils;

import java.net.InetAddress;

/**
 * A utility used for obtaining IP-related information.
 * 
 * @author Xu Linhao
 * @version 1.0 2006-2-1
 */

public class Inet
{

	/**
	 * Returns the string representation of the IP address.
	 * 
	 * @return the string representation of the IP address; if an <code>Exception</code>
	 * 		   appears, then return a <code>null</code> value
	 */
	public static String getInetAddress()
	{
		try
		{
			InetAddress inet = InetAddress.getLocalHost();
			return inet.getHostAddress();
		}
		catch (Exception e)
		{ /* if error happens, return a null value */
			return null;
		}
	}
	
	/**
	 * Returns the <code>InetAddress</code>.
	 * 
	 * @return the <code>InetAddress</code>; if an <code>Exception</code>
	 * 		   appears, then return a <code>null</code> value
	 */
	public static InetAddress getInetAddress2()
	{
		try
		{
			return InetAddress.getLocalHost();
		}
		catch (Exception e)
		{ /* if error happens, return a null value */
			return null;
		}
	}
	
	/**
	 * Check if the IP address is valid.
	 * 
	 * @param ip the IP address represented by a <code>String</code>
	 * @return if the IP address is valid, return <code>true</code>; 
	 * 		   otherwise, return <code>false</code>
	 */
	public static boolean isValidInetAddress(String ip)
	{
	 	try
 		{
	 		byte[] octets = InetAddress.getByName(ip).getAddress();
	  		if(octets.length != 4)
	  		{
	  			return false;
	  		}
	  	}
	  	catch(Exception e)
	  	{ /* if any exception occurs then return false */
	  		return false;
	  	}
	  	/* no errors happen */
	  	return true;
  	}  
	
}