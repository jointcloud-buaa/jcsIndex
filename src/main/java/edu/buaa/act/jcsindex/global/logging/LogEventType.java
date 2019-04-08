/*
 * @(#) EventType.java 1.0 2006-6-22
 * 
 * Copyright 2006 National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.logging;

/**
 * Define the event type, namely <code>Error</code>, <code>Information</code>,
 * and <code>Warning</code>.
 * 
 * @author Xu Linhao
 * @version 1.0 2006-6-22
 */

public enum LogEventType
{

	// define event type bellow
	ERROR		("Error"),
	WARNING		("Warning"),
	INFORMATION	("Information");
	
	
	/* define message type bellow */
	private final String value;
	
	LogEventType(String value)
	{
		this.value = value;
	}
	
	/**
	 * Get the value of the event type.
	 * 
	 * @return the value of the event type
	 */
	public String getValue()
	{
		return this.value;
	}
	
	/**
	 * Get the value of the event type.
	 * 
	 * @param eventType the type of the event
	 * @return the value of the event type
	 */
	public static String getValue(String eventType)
	{
		for (LogEventType m: LogEventType.values())
		{
			if (m.getValue().equals(eventType))
			{
				return m.getValue();
			}
		}
		return null;
	}
	
	/**
	 * Check if the event type exists.
	 * 
	 * @param eventType the event type
	 * @return <code>true</code> if exists; otherwise, <code>false</code>
	 */
	public static boolean checkValue(String eventType)
	{
		for (LogEventType m: LogEventType.values())
		{
			if (m.getValue().equals(eventType))
			{
				return true;
			}
		}
		return false;
	}

}