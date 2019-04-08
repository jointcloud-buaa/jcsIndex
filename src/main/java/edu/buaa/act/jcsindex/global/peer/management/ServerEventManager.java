/*
 * @(#) ServerEventManager.java 1.0 2006-1-24
 * 
 * Copyright 2005, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.peer.management;

import edu.buaa.act.jcsindex.global.AbstractInstance;

import java.net.Socket;

/**
 * Implement the socket server at the bootstrap server,
 * that is responsbile for accepting the incoming socket connections.
 * 
 * @author Xu Linhao
 * @version 1.0 2005-12-30
 */

public class ServerEventManager extends AbstractEventManager
{

	/**
	 * Construct a pooled socket server at specified port 
	 * and with maximum number of incoming connections
	 *  
	 * @param port the port used for server socket
	 * @param maxConn the maximum connections that can be handled simultaneously
	 */
	public ServerEventManager(AbstractInstance instance, int port, int maxConn)
	{
		super(instance, port, maxConn);
	}

	@Override
	public void setupHandlers() 
	{
		this.eventHandlers = new ServerEventDispatcher[this.maxConn];
		for (int i = 0; i < maxConn; i++)
		{
			eventHandlers[i] = new ServerEventDispatcher(instance);
			eventHandlers[i].registerActionListeners();
			new Thread(eventHandlers[i], "Event Handler " + i).start();
		}
	}
	
	@Override
	protected void showConnections(Socket incomingConnection) 
	{
		//super.showConnections(incomingConnection);
	}

}