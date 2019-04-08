/*
 * @(#) PooledSocketHanlder.java 1.0 2005-12-29
 * 
 * Copyright 2005, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.utils;

/**
 * This interface is responsible for processing any incoming socket request
 * passed from <code>PooledSocketServer</code>. 
 * 
 * <p>The <code>PooledSocketServer</code> is responsible for initiating 
 * <code>PooledSocketHandler</code>s, each of which will be in the status of sleep
 * when no socket request comes in and woke up for handling socket request when
 * a socket request arrives.
 *  
 * @author Xu Linhao
 * @version 1.0 2005-12-29
 * 
 * @see PooledSocketServer
 * @see AbstractPooledSocketHandler
 */

public interface PooledSocketHandler
{

	/**
	 * Process the incoming socket requests and reply with a response if necessary.
	 */
	void handleConnection();

	/**
	 * Stop the socket handler.
	 */
	void stop();
	
}