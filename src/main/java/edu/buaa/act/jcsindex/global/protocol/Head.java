/*
 * @(#) Head.java 1.0 2006-1-4
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.protocol;

import java.io.Serializable;

/**
 * Define the head of the message.
 * 
 * @author Xu Linhao
 * @version 1.0 2006-1-4
 */

public class Head implements Cloneable, Serializable
{

	/**
	 * The serialVersionUID is used for serializing and de-serializing
	 * this class and be SURE NOT CHANGE THIS VALUE!
	 */
	private static final long serialVersionUID = -8426349814055556532L;
	
	// private members
	private int msgType;
	
	/**
	 * Constructor.
	 */
	public Head()
	{
		/* set default message type */
		msgType = MsgType.DEFAULT.getValue();
	}
	
	/**
	 * Constructor.
	 * 
	 * @param msgType the message type
	 */
	public Head(int msgType)
	{
		if (!MsgType.checkValue(msgType))
		{
			throw new IllegalArgumentException("Unknown Message Type");
		}
		this.msgType = msgType;
	}
	
	/**
	 * Clone self.
	 * 
	 * @return The instance of <code>Head</code>.
	 */
	public Object clone()
	{
		Head instance = null;
		try
		{
			instance = (Head)super.clone();
		}
		catch (CloneNotSupportedException e)
		{
			e.printStackTrace();
		}
		return instance;
	}

	/**
	 * Set the message type of the head.
	 * 
	 * @param msgType the message type
	 */
	public void setMsgType(int msgType)
	{
		this.msgType = msgType;
	}
	
	/**
	 * Get the message type of the head.
	 * 
	 * @return the message type
	 */
	public int getMsgType()
	{
		return this.msgType;
	}
	
	/**
	 * Override <code>toString()</code> function of <code>java.lang.Object</code>.
	 * 
	 * @return A string that describes the content of the body.
	 */
	public String toString()
	{
		String delim = "\t";
		String result = "Head format: message type" + delim + msgType;

		return result;
	}
	
}