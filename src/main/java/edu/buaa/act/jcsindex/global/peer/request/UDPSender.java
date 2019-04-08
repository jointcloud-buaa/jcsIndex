/*
 * @(#) UDPSender.java 1.0 2006-9-22
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.peer.request;

import edu.buaa.act.jcsindex.global.AbstractInstance;
import edu.buaa.act.jcsindex.global.protocol.MsgType;
import edu.buaa.act.jcsindex.global.utils.Tools;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.TimerTask;

/**
 * The <code>UDPSender</code> is responsible for send UDP packets to remote users.
 * 
 * <p>All concrete senders should extend <code>UDPSender</code> by implementing 
 * the method <code>{@link #sendPacket()}</code>.
 * 
 * @author Xu Linhao
 * @version 1.0 2006-9-22
 * 
 * @see BootstrapPnPSender
 * @see UDPReceiver
 */

public abstract class UDPSender extends TimerTask
{
	
	// protected member
	/**
	 * For each upd packet, the sender tries to send it out to remote site
	 * for several times for the purpose of lessing the packet missing problem.
	 */
	protected final static int TRY_TIME = 2;
	protected final static boolean debug = false;

	/**
	 * The main frame that has the control of <code>UDPServer</code>.
	 */
	protected AbstractInstance instance;
	
	/**
	 * The instance of datagram socket used for sending UDP packets.
	 */
	protected DatagramSocket datagramSocket;
	
	/**
	 * Construct <code>UDPSender</code> by initializing <code>DatagramSocket</code>
	 * at the specified port, the server list and the stop flag as <code>false</code>.
	 * 
	 * @param instance the main frame
	 * @param port the port that the <code>DatagramSocket</code> will be initialized
	 * @throws SocketException
	 */
	public UDPSender(AbstractInstance instance, int port) throws SocketException
	{
		this.instance = instance;
		this.datagramSocket = new DatagramSocket(port);
	}
	
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
	 * Returns <code>true</code> if the <code>UDPSender</code> is still alive.
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
	 * Creates and sends UDP packets to remote site. All concrete <code>UDPSender</code>
	 * should implement this method.
	 */
	protected abstract void sendPacket(); 
	
	/**
	 * Returns a ping packet that only includes a 4-bytes message 
	 * type without specifying the IP address and port of the 
	 * destination peer.
	 * 
	 * <p>The packet format is:
	 * <pre>
	 * 0-3: the int value of PING  
	 * </pre>
	 * 
	 * @return a <code>DatagramPacket</code> with ping message type
	 */
	protected synchronized DatagramPacket ping()
	{
		byte[] buf = new byte[4];
		int val  = MsgType.PING.getValue();
		Tools.intToByteArray(val, buf, 0);
		return new DatagramPacket(buf, 0, buf.length);
	}
	
	/**
	 * Returns a ping packet that only includes a 4-bytes message 
	 * type with specified IP address and port of the destination peer. 
	 * 
	 * <p>The packet format is:
	 * <pre>
	 * 0-3: the int value of PING  
	 * </pre>
	 *   
	 * @param ip the IP address of the destination peer
	 * @param port the port that the datagram socket is running
	 * @return a <code>DatagramPacket</code> with ping message type
	 */
	protected synchronized DatagramPacket ping(InetAddress ip, int port)
	{
		DatagramPacket packet = ping();
		packet.setAddress(ip);
		packet.setPort(port);
		return packet;
	}
	
	/**
	 * Start the service of <code>UDPSender</code>.
	 */
	public void run()
	{
		this.sendPacket();
	}
	
}