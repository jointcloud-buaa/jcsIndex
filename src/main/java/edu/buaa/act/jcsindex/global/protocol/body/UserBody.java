/*
 * @(#) UserBody.java 1.0 2006-1-6
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.protocol.body;

/**
 * This class is used to implement the user body which 
 * contains the user identifier and the password. The bootstrap
 * server or the CA will use this to judge whether the user is valid
 * to the system.
 * 
 * @author Xu Linhao
 * @version 1.0 2006-1-6
 */

public class UserBody extends Body
{
	
	// private members
	/**
	 * The serialVersionUID is used for serializing and de-serializing
	 * this class and be SURE NOT CHANGE THIS VALUE!
	 */
	private static final long serialVersionUID = 2083018310826233065L;
	
	private String userID;		// the user identifier
	private String password;	// the password
	private String email;		// the email address
	private String ip;			// the IP address
	private int	   port;		// the port of the server socket
	private String type;		// the peer type: superpeer or peer
	
	/**
	 * Construct a user body with both user identifier and
	 * password.
	 * 
	 * @param userID the user identifier
	 * @param password the password
	 * @param email the email address
	 * @param ip the IP address
	 * @param port the port used for monitoring network events
	 * @param type the peer type
	 */
	public UserBody(String userID, String password, String email, String ip, int port, String type)
	{
		this.userID   = userID;
		this.password = password;
		this.email    = email;
		this.ip		  = ip;
		this.port	  = port;
		this.type 	  = type;
	}

	/**
	 * Get the user identifier.
	 * 
	 * @return The user identifier.
	 */
	public String getUserID()
	{
		return userID;
	}

	/**
	 * Get the password.
	 * 
	 * @return The password.
	 */
	public String getPassword()
	{
		return password;
	}
	
	/**
	 * Get the email.
	 * 
	 * @return the email
	 */
	public String getEmail()
	{
		return email;
	}
	
	/**
	 * Get the request peer's IP address,
	 * for another peer to reply message.
	 * 
	 * @return the request peer's IP address
	 */
	public String getIP()
	{
		return ip;
	}
	
	/**
	 * Get the port of the server socket that 
	 * is used for providing service of monitoring
	 * incoming and outgoing socket connections.
	 * 
	 * @return the port of the server socket
	 */
	public int getPort()
	{
		return port;
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
	 * Override <code>toString()</code> function of <code>java.lang.Object</code>.
	 * 
	 * @return A string that describes the content of the body.
	 */
	public String toString()
	{
		String delim = ":";

		String result = "UserBody format:= userID:password:email:ip:port:type\r\n";
		result += userID + delim + password + delim + email + 
					delim + ip + delim + port + delim + type;
		
		return result;
	}
	
}