/*
 * @(#) LogEventWatcher.java 1.0 2006-7-17
 * 
 * Copyright 2006 National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.logging;

/**
 * When a <code>LogEvent</code> happens, an <code>AbstractPeer</code> can call
 * addLogEvent(LogEvent event) in a <code>EventListener</code>.
 * 
 * @author Xu Linhao
 * @version 1.0 2006-7-17
 */

public interface LogEventWatcher
{

	/**
	 * Add a log event for displaying to users. The format of 
	 * the log event is defined by <code>LogEvent</code>.
	 * 
	 * @param type the event type
	 * @param desp the event description
	 * @param host the host name who sends requests
	 * @param src  the IP address of the requester
	 * @param dest the IP address of the receiver
	 */
	void log(String type, String desp, String host, String src, String dest);
	
}