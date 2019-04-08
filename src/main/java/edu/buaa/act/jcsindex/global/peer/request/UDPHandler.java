/*
 * @(#) UDPHandler.java 1.0 2006-9-22
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.peer.request;

import edu.buaa.act.jcsindex.global.protocol.MsgType;
import edu.buaa.act.jcsindex.global.utils.Tools;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;

/**
 * The <code>UDPHandler</code> is responsible for processing UDP packets.
 * 
 * <p>All <code>UDPHandler</code>s will be blocked when there is no UDP packet 
 * in the pool; and will be woke up when any UDP request comes in and is inserted 
 * into the pool. To stop <code>UDPHandler</code> just set the stop signal 
 * as <code>true</code>.
 * 
 * <p>The <code>UDPReceiver</code> is responsible for listening UDP requests
 * from remote users and then passing the UDP packets to the <code>UDPHandler</code>.
 * 
 * <p>To process the UDP packets, <code>UDPListener</code> should be registered
 * to each <code>UDPHandler</code> by invoking the method 
 * <code>{@link #addUDPListener(UDPListener)}</code>.
 * The <code>UDPListener</code> defines the callback interface that is used for 
 * triggering a proper actions when a desired UDP packet is received. The method
 * <code>{@link #handlePacket()}</code> is responsible for invoking the
 * callback method of the <code>UDPListener</code>.
 * 
 * <p>To define customized <code>UDPHandler</code>, just create your own handler by
 * extending this class and implement the method <code>{@link #handlePacket()}</code>.
 * 
 * <p>An example to show how to use <code>UDPListener</code>:
 * <pre>
 * public void handlePacket()
 * {
 * 	Object[] list;
 * 	synchronized (this)
 * 	{
 * 		list = listeners.toArray().clone();
 * 	}
 * 	UDPListener listener;
 * 	for (int i = 0; i < list.length; i++)
 * 	{
 * 		listener = (UDPListener) list[i];
 * 		...
 * 	}
 * }
 * </pre>
 * 
 * @author Xu Linhao
 * @version 1.0 2006-9-22
 * 
 * @see UDPReceiver
 * @see BootstrapPnPHandler
 */

public abstract class UDPHandler implements Runnable
{
	
	// protected members
	protected final static boolean debug = false;
	/**
	 * A list that keeps all incoming UDP packets, each of which is waiting for being processed by the <code>UDPHandler</code>.
	 */
	protected static List<DatagramPacket> pool = Collections.checkedList(new LinkedList<DatagramPacket>(), DatagramPacket.class);

	/**
	 * A list that keeps all <code>UDPListener</code>s that is responsible for processing the incoming UDP packets.
	 */
	protected List<UDPListener> listeners;
	
	/**
	 * The signal used to determine whether stops the handler itself.
	 */
	protected boolean stop;
	
	/**
	 * The instance of <code>DatagramSocket</code>.
	 */
	protected DatagramSocket datagramSocket;
	
	/**
	 * The instance of datagram packet to be processed.
	 */
	protected DatagramPacket request;
	
	/**
	 * Construct the <code>UDPHandler</code>.
	 */
	public UDPHandler(DatagramSocket ds)
	{
		listeners = Collections.checkedList(new Vector<UDPListener>(), UDPListener.class);
		this.datagramSocket = ds;
		this.stop = false;
	}
	
	// ------------ manage event listeners ---------------
	
	/**
	 * Add an <code>UDPListener</code> to <code>UDPHandler</code>.
	 * 
	 * @param l the <code>UDPListener</code> that is responsible for processing UDP message
	 */
	public synchronized void addUDPListener(UDPListener l)
	{
		listeners.add(l);
	}
	
	/**
	 * Remove an <code>UDPListener</code> to <code>UDPHandler</code>.
	 * 
	 * @param l the <code>UDPListener</code> that is responsible for processing UDP message
	 */
	public synchronized void removeUDPListener(UDPListener l)
	{
		listeners.remove(l);
	}

	// ------------- end of managing event listeners -----------------
	
	/**
	 * Accepting the UDP packet to be handled from the <code>UDPReceiver</code> 
	 * and then put the packet into a pool for further processing.
	 * 
	 * @param request the incoming UDP packet to be processed
	 */
	public static void processRequest(DatagramPacket request)
	{
		synchronized (pool)
		{
			pool.add(pool.size(), request);
			pool.notifyAll();
		}
	}
	
	/**
	 * Clear the UDP packets in the pool and notify all <code>UDPHandler</code>s.
	 */
	public static void stopAllHandlers()
	{
		synchronized (pool)
		{
			pool.clear();
			pool.notifyAll();
		}
	}

	/**
	 * Stop the handler by setting the flag as <code>false</code..
	 */
	public void stop()
	{
		this.stop = true;
	}
	
	/**
	 * Each concrete <code>UDPHandler</code> must implement this method
	 * to provide detailed algorithm for processing the incoming UDP packets.
	 */
	protected abstract void handlePacket();
	
	/**
	 * Returns the message type of the packet.
	 * 
	 * @param buf the byte array to be parsed
	 * @param start the start position of the byte array to be parsed
	 * @return the message type of the packet
	 */
	protected synchronized int parseType(final byte[] buf, int start)
	{
		return Tools.byteArrayToInt(buf, start);
	}
	
	/**
	 * Returns a pong packet that only includes a 4-bytes message 
	 * type without specifying the IP address and port of the 
	 * destination peer.
	 * 
	 * <p>The packet format is:
	 * <pre>
	 * 0-3: the int value of PONG  
	 * </pre>
	 * 
	 * @return a <code>DatagramPacket</code> with pong message type
	 */
	protected synchronized DatagramPacket pong()
	{
		byte[] buf = new byte[4];
		int val  = MsgType.PONG.getValue();
		Tools.intToByteArray(val, buf, 0);
		return new DatagramPacket(buf, 0, buf.length);
	}
	
	/**
	 * Returns a pong packet that only includes a 4-bytes message 
	 * type with specified IP address and port of the destination peer. 
	 * 
	 * <p>The packet format is:
	 * <pre>
	 * 0-3: the int value of PONG  
	 * </pre>
	 *   
	 * @param ip the IP address of the destination peer
	 * @param port the port that the datagram socket is running
	 * @return a <code>DatagramPacket</code> with ping message type
	 */
	protected synchronized DatagramPacket pong(InetAddress ip, int port)
	{
		DatagramPacket packet = pong();
		packet.setAddress(ip);
		packet.setPort(port);
		return packet;
	}
	
	/**
	 * Start the <code>UDPHandler</code> and wait for any incoming UDP packet.
	 */
	public void run()
	{
		while (!stop)
		{
			synchronized (pool)
			{
				while (pool.isEmpty())
				{
					try
					{
						pool.wait();
					}
					catch (InterruptedException e)
					{
						/* ignore it */
					}
					if (stop) return;
				}
				/* get the first datagram to process */
				request = pool.remove(0);
			}
			/* process data */
			this.handlePacket();
		}
	}
	
}