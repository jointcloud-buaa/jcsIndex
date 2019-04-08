/*
 * @(#) BootstrapPnPServer.java 1.0 2006-9-25
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.peer.request;

import edu.buaa.act.jcsindex.global.AbstractInstance;
import edu.buaa.act.jcsindex.global.protocol.MsgType;

import java.net.SocketException;
import java.util.Timer;

/**
 * The <code>BootstrapPnPServer</code> is used for sending and receiving UDP messages
 * between peers, which is composed of a <code>BootstrapPnPSender</code> and a 
 * <code>BootstrapPnPReceiver</code>. The <code>BootstrapPnPSender</code> is responsible for
 * sending <code>{@link MsgType#PING}</code> messages
 * to remote peers, while the <code>BootstrapPnPReceiver</code> for receiving 
 * <code>{@link MsgType#PONG}</code> messages from
 * remote peers.
 * 
 * <p>To start the <code>BootstrapPnPServer</code>, just implement the following codes
 * in the <code>{@link #run()}</code> method.
 * <pre>
 * public void run()
 * {
 * 	sender = new BootstrapPnPSender();
 * 	scheduler = new Timer("Ping Sender of Bootstrap");
 * 	scheduler.schedule(sender, 10000); // for 10 seconds
 * 
 * 	receiver = new BootstrapPnPReceiver(sender.getSocket(), 10);
 * 	Thread receiverThread = new Thread("Pong Receiver of Bootstrap", receiver);
 * 	receiverThread.start();
 * }
 * </pre>
 * 
 * @author Xu Linhao
 * @version 1.0 2006-9-25
 * 
 * @see UDPServer
 * @see BootstrapPnPSender
 * @see BootstrapPnPReceiver
 */

public class BootstrapPnPServer extends UDPServer
{

	// private members
	private Thread receiverThread;	// thread for UDP receiver
	
	/**
	 * Construct the <code>BootstrapPnPServer</code> with specified paramemters.
	 * 
	 * @param instance the reference of <code>AbstractInstance</code>
	 * @param port the port to be used for starting the server
	 * @param maxConn the maximum number of handlers used for processing UDP messages
	 * @param period the sitting up time used for initiating next round of UDP packets dissemination
	 * @throws SocketException if open socket on the port fails
	 */
	public BootstrapPnPServer(AbstractInstance instance, int port, int maxConn, long period) throws SocketException
	{
		super(instance, port, maxConn, period);
	}

	/**
	 * Start both sender and receiver for processing UDP messages.
	 */
	public void run() 
	{
		try 
		{
			// init sender
			sender = new BootstrapPnPSender(instance, port);
			scheduler = new Timer("Ping Sender of Bootstrap");

			// init receiver
			receiver = new BootstrapPnPReceiver(instance, sender.getDatagramSocket(), maxConn);
			receiver.addUDPListener((BootstrapPnPSender) sender);
			receiverThread = new Thread(receiver, "Pong Receiver of Bootstrap");

			// start both sender and receiver
			scheduler.scheduleAtFixedRate(sender, 100, period);
			receiverThread.start();
		} 
		catch (SocketException e) 
		{
			System.err.println("Cannot start UDP service");
			System.exit(1);
		}
	}
	
}