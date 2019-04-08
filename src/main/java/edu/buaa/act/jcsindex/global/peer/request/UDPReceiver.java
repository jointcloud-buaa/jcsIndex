/*
 * @(#) UDPReceiver.java 1.0 2006-9-22
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.peer.request;

import edu.buaa.act.jcsindex.global.AbstractInstance;
import edu.buaa.act.jcsindex.global.logging.LogManager;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;

/**
 * The <code>UDPReceiver</code> is responsible for listening and receiving UDP packets 
 * from remote sites by wrapping <code>DatagramSocket</code>.
 * 
 * <p>All concrete receivers should extend the <code>UDPReceiver</code> with a set of
 * <code>UDPHandler</code>, each of which is responsible for processing the received
 * UDP packets.
 * 
 * <p>To handle UDP packets, a callback pattern is used by <code>UDPHandler</code>. To
 * pass <code>UDPListener</code>s to <code>UDPHandler</code>, the <code>UDPReceiver</code>
 * provides the methods <code>{@link #addUDPListener(UDPListener)}</code> and
 * <code>{@link #removeUDPListener(UDPListener)}</code> to register and remove
 * a <code>UDPListener</code> to its <code>UDPHandler</code>s.
 * 
 * @author Xu Linhao
 * @version 1.0 2006-9-22
 * 
 * @see BootstrapPnPReceiver
 * @see UDPSender
 */

public abstract class UDPReceiver implements Runnable
{
	
	// protected members
	/**
	 * The default buffer size for receiving UDP packet.
	 */
	protected final static int buffersize = 1024;
	
	/**
	 * The main frame that has the control of <code>UDPServer</code>.
	 */
	protected AbstractInstance instance;

	/**
	 * The maximum number of UDP packets that can be handled simultaneously. 
	 */
	protected int maxConn;
	
	/**
	 * A set of handlers used for processing received UDP packets.
	 */
	protected UDPHandler[] handlers;
	
	/**
	 * The signal used to determine whether stops the service of accepting the incoming UDP packets.
	 */
	protected volatile boolean stop;
	
	/**
	 * The instance of <code>DatagramSocket</code>.
	 */
	protected DatagramSocket datagramSocket;
	
	/**
	 * Construct the <code>UDPReceiver</code> with specified <code>DatagramSocket</code>
	 * and the maximum number of UDP packets that can be handled simultaneously.
	 * 
	 * @param instance the main frame
	 * @param ds the instance of a <code>DatagramSocket</code>
	 * @param maxConn the maximum number of UDP packets that can be handled simultaneously
	 * @throws SocketException
	 */
	public UDPReceiver(AbstractInstance instance, DatagramSocket ds, int maxConn) throws SocketException
	{
		this.instance = instance;
		this.datagramSocket = ds;
		this.maxConn = maxConn;
		this.stop = false;
		this.setupHandlers();
	}
	
	// ------------ methods for event listeners -----------
	
	/**
	 * Add an <code>UDPListener</code> to <code>UDPHandler</code>.
	 * 
	 * @param l the <code>UDPListener</code> that is responsible for processing UDP message
	 */
	public void addUDPListener(UDPListener l)
	{
		for (int i = 0; i < maxConn; i++)
			handlers[i].addUDPListener(l);
	}
	
	/**
	 * Remove an <code>UDPListener</code> to <code>UDPHandler</code>.
	 * 
	 * @param l the <code>UDPListener</code> that is responsible for processing	UDP message
	 */
	public void removeUDPListener(UDPListener l)
	{
		for (int i = 0; i < maxConn; i++)
			handlers[i].removeUDPListener(l);
	}
	
	// -------------- end of methods for event listeners --------------
	
	/**
	 * Returns the instance of the <code>DatagramSocket</code>.
	 * 
	 * @return the instance of the <code>DatagramSocket</code>
	 */
	public DatagramSocket getDatagramSocket()
	{
		return this.datagramSocket;
	}
	
	/**
	 * Returns the port used for running <code>DatagramSocket</code>.
	 * 
	 * @return the port used for running <code>DatagramSocket</code>
	 */
	public int getLocalPort()
	{
		return this.datagramSocket.getLocalPort();
	}
	
	/**
	 * Initiate a set of <code>UDPHandler</code>s and the maximum number of handlers 
	 * is specified by the parameter <code>maxConn</code>. 
	 */
	public abstract void setupHandlers(); 
	
	/**
	 * Listening UDP packets from remote servers and if any UDP packet is received,
	 * then the packet will be passed to the handlers in order to process it.
	 */
	protected void receive()
	{
		byte[] buffer = new byte[buffersize];
		while (!stop)
		{
			try 
			{
				DatagramPacket incoming = new DatagramPacket(buffer, 0, buffer.length);
				this.datagramSocket.receive(incoming);
				this.handleUDPPacket(incoming);

				if (stop) return;
			} 
			catch (Exception e) 
			{
				if (!stop) // if stop flag is set as true, just ignore exception
				{
//					instance.log(LogEventType.ERROR.getValue(), "Exception occurs because:\r\n" + Tools.getException(e), datagramSocket.getLocalAddress().getHostName(), datagramSocket.getLocalAddress().getHostAddress() + ":" + datagramSocket.getLocalPort(), System.getProperty("user.name"));
					LogManager.getLogger(instance.peer()).error("Exception ocurrs in receive of UDPReceiver", e);
				}
			}
		}
	}
	
	/**
	 * Accept an incoming UDP packet and then pass it to 
	 * the <code>UDPHandler</code>s for processing.
	 * 
	 * @param request the incoming UDP packet
	 */
	protected void handleUDPPacket(DatagramPacket request)
	{
		UDPHandler.processRequest(request);
	}
	
	/**
	 * Returns <code>true</code> if the <code>UDPReceiver</code> is still alive.
	 * 
	 * @return <code>true</code> if alive; otherwise, return <code>false</code>
	 */
	public boolean isAlive()
	{
		if (this.datagramSocket == null)
			return false;
		return !this.datagramSocket.isClosed();
	}
	
	/**
	 * Stop all <code>UDPHandler</code>s and do necessary clearup job.
	 */
	public abstract void stop();
	
	/**
	 * Setup all <code>UDPHandler</code>s and then be prepared for
	 * receiving the incoming UDP packets.
	 */
	public void run()
	{
		this.receive();
	}
	
}