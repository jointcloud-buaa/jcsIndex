/*
 * @(#) UDPServer.java 1.0 2006-9-22
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.peer.request;

import edu.buaa.act.jcsindex.global.AbstractInstance;

import java.net.DatagramSocket;
import java.util.Timer;

/**
 * The <code>UDPServer</code> is composed of a <code>UDPSender</code> 
 * and a <code>UDPReceiver</code>, where the sender is responsible for 
 * sending UDP packets to remote peers and the receiver for receiving 
 * UDP packets from remote peers.
 * 
 * <p>Each concrete <code>UDPServer</code> should extend this class 
 * by using customized <code>UDPSender</code>, <code>UDPReceiver</code> 
 * and <code>UDPHandler</code>.
 * 
 * <p>The following example demonstrate how to use <code>UDPSender</code> 
 * and <code>UDPReceiver</code>:
 * <pre>
 * public void run()
 * {
 * 	scheduler = new Timer("UDP Sender");
 * 	sender = new UDPSender();
 * 	scheduler.scheduleAtFixedRate(sender, 100, 10000); // 10 seconds
 * 	receiver = new UDPReceiver(sender.getSocket(), 10);
 * 	Thread receiverThread = new Thread("UDP Receiver", receiver);
 * 	receiverThread.start();
 * }
 * </pre>
 * 
 * @author Xu Linhao
 * @version 1.0 2006-9-22
 * 
 * @see BootstrapPnPServer
 * @see UDPSender
 * @see UDPReceiver
 */

public abstract class UDPServer implements Runnable
{
	
	// protected members
	/**
	 * The main frame that has the control of <code>UDPServer</code>.
	 */
	protected AbstractInstance instance;
	
	/**
	 * A Timer used to schedule UDPSender at regular time interval.
	 */
	protected Timer scheduler;
	
	/**
	 * An UDPSender used for sending UDP packets to remote peers.
	 */
	protected UDPSender sender;
	
	/**
	 * The time out used for initiating next round of UDP packets dissemination.
	 */
	protected long period;
	
	/**
	 * An UDPReceiver used for receiving UDP packets from remote peers.
	 */
	protected UDPReceiver receiver;
	
	/**
	 * The port used for initializing <code>UDPsender</code> and <code>UDPReceiver</code>.
	 */
	protected int port;
	
	/**
	 * The maximum number of UDP packets that can be handled simultaneously. 
	 */
	protected int maxConn;

	/**
	 * The signal used to determine whether stops the server itself.
	 */
	protected volatile boolean stop;

	/**
	 * Construct <code>UDPServer</code> by specifying the maximum number of UDP packets 
	 * that can be handled simultaneously.
	 * 
	 * @param instance the main frame that has the control of <code>UDPServer</code>
	 * @param port the port used for initializing <code>UDPsender</code> and <code>UDPReceiver</code>
	 * @param maxConn the maximum number of UDP packets that can be handled simultaneously
	 * @param period the sitting up time used for initiating next round of UDP packets dissemination
	 */
	public UDPServer(AbstractInstance instance, int port, int maxConn, long period)
	{
		this.instance = instance;
		this.period = period;
		this.port = port;
		this.maxConn = maxConn;
		this.stop = false;
	}
	
	/**
	 * Returns the datagram socket.
	 * 
	 * @return the datagram socket
	 */
	public DatagramSocket getDatagramSocket()
	{
		return this.sender.datagramSocket;
	}
	
	/**
	 * Returns the port used for running <code>DatagramSocket</code>.
	 * 
	 * @return if the instance of <code>UDPSender</code> is not null
	 * 		   and alive, return the port used for running <code>DatagramSocket</code>;
	 * 		   otherwise, return -1 
	 */
	public int getLocalPort()
	{
		if ((sender != null) && (sender.isAlive()))
			return sender.getLocalPort();
		return -1;
	}
	
	/**
	 * Returns <code>true</code> if <code>UDPServer</code> is alive by checking the 
	 * status of both <code>UDPReceiver</code> and <code>UDPSender</code>.
	 * 
	 * @return <code>true</code> if alive; otherwise, return <code>false</code>
	 */
	public boolean isAlive()
	{
		return sender.isAlive() && receiver.isAlive();
	}
	
	/**
	 * Reschedule the UDP Sender with a new time interval.
	 * 
	 * @param period the new delay
	 */
	public void scheduleUDPSender(long period)
	{
		this.period = period;
		this.scheduler.cancel();
		if (sender.isAlive())
			this.scheduler.scheduleAtFixedRate(sender, 100, period);
	}
	
	/**
	 * Stop both UDPSender and UDPReceiver.
	 */
	public void stop()
	{
		// clear the state of both sender and receiver
		this.scheduler.cancel();
		this.receiver.stop();

		// close datagram socket
		DatagramSocket socket = this.sender.getDatagramSocket();
		if (!socket.isClosed())
			socket.close();
	}
	
}