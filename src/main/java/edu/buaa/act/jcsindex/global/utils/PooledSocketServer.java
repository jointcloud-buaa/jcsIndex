/*
 * @(#) PooledSocketServer.java 1.0 2005-12-29
 * 
 * Copyright 2005, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.utils;

/**
 * This interface defines the methods used for initializing a set of <code>PooledSocketHandler</code>s 
 * that are reponsible for processing each incoming socket request, accepting any incoming
 * socket request by wrapping <code>java.net.ServerSocket</code>, and stopping the service
 * of <code>java.net.ServerSocket</code> and all running <code>PooledSocketHandler</code>s.
 * 
 * @author Xu Linhao
 * @version 1.0 2005-12-29
 * 
 * @see	PooledSocketHandler
 * @see AbstractPooledSocketServer
 */

public interface PooledSocketServer
{

	/**
	 * Initiate a set of <code>PooledSocketHandler</code>, which is responsible
	 * for processing each incoming socket request.
	 */
	void setupHandlers();
	
	/**
	 * Accept any incoming socket request from remote users and dispatch it
	 * to a sleeping <code>PooledSocketHanlder</code>. 
	 */
	void acceptConnections();
	
	/**
	 * Stop the service for accepting the incoming socke requests and clear
	 * all socket handlers at the same time.
	 */
	void stop();
	
}