/*
 * @(#) BootstrapPnPReceiver.java 1.0 2006-10-9
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.peer.request;

import edu.buaa.act.jcsindex.global.AbstractInstance;
import edu.buaa.act.jcsindex.global.protocol.MsgType;

import java.net.DatagramSocket;
import java.net.SocketException;

/**
 * The <code>BootstrapPnPReceiver</code> is responsible for listening any incoming
 * UDP packets from remote peers. The received UDP packets have the message type of
 * <code>{@link MsgType#PONG}</code>.
 * 
 * <p>To process each received UDP packet, a set of handlers are pre-initialized in
 * the method <code>{@link #setupHandlers()}</code>. If the system-defined timeout
 * runs out, then the receiver sets timeout signal as <code>true</code> to notify
 * the sender to start a new round of UDP packets dissemination.  
 * 
 * @author Xu Linhao
 * @version 1.0 2006-10-9
 * 
 * @see BootstrapPnPServer
 * @see UDPReceiver
 */

public class BootstrapPnPReceiver extends UDPReceiver
{

	/**
	 * Construct the receiver with specified parameters.
	 * 
	 * @param instance the reference of the <code>AbstractInstance</code>
	 * @param ds the reference of the <code>DatagramSocket</code>
	 * @param maxConn the maximum number of handlers to be used for processing UDP packets
	 * @throws SocketException
	 */
	public BootstrapPnPReceiver(AbstractInstance instance, DatagramSocket ds, int maxConn) throws SocketException
	{
		super(instance, ds, maxConn);
	}

	@Override
	public void setupHandlers() 
	{
		handlers = new BootstrapPnPHandler[maxConn];
		for (int i = 0; i < maxConn; i++)
		{
			handlers[i] = new BootstrapPnPHandler(this.getDatagramSocket());
			new Thread(handlers[i], i + "-th UDP handler of Bootstrap").start();
		}
	}

	@Override
	public void stop() 
	{
		this.stop = true;
		// set stop signal of all handlers as true
		for (int i = 0; i < maxConn; i++)
			handlers[i].stop();
		// wake up all handlers to allow them destroy themselves
		BootstrapPnPHandler.stopAllHandlers(); 
	}
}