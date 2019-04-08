/*
 * @(#) AbstractPeer.java 1.0 2006-1-27
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.peer;

import edu.buaa.act.jcsindex.global.AbstractInstance;
import edu.buaa.act.jcsindex.global.peer.info.PhysicalInfo;
import edu.buaa.act.jcsindex.global.peer.management.AbstractEventManager;
import edu.buaa.act.jcsindex.global.peer.management.PeerMaintainer;
import edu.buaa.act.jcsindex.global.peer.request.UDPServer;
import edu.buaa.act.jcsindex.global.protocol.Message;
import edu.buaa.act.jcsindex.global.protocol.MsgType;
import edu.buaa.act.jcsindex.global.utils.Inet;
import edu.buaa.act.jcsindex.global.utils.Tools;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * An abstract class that defines all necessary interfaces
 * for communicating between peers.
 * <p>
 * The philosophy is twofolds: (1) wrap all necessary functionalities
 * that has no relationship with instance in a separated class; (2) provide
 * the interfaces to construct a peer in a simulation environment.
 * 
 * @author Xu Linhao
 * @version 1.0 2006-1-27
 */

public abstract class AbstractPeer
{

	// public members
	/**
	 * The local port used for monitoring network events, i.e., tcp and udp services
	 * will be initialized on it at the same time.
	 */
	public static int LOCAL_SERVER_PORT;
	
	/**
	 * The maximal number of threads used for handling network events simultaneously.
	 */
	public static int CAPACITY;

	/**
	 * The main instance that contains the peer.
	 */
	protected AbstractInstance instance;
	
	/**
	 * The type of a peer.
	 * @see PeerType
	 */
	protected String peerType;
	
	/**
	 * The network event manager who is responsible for monitoring and 
	 * processing all incoming network events, while at the same time,
	 * replying feedback if necessary.
	 */
	protected AbstractEventManager eventManager;
	
	/**
	 * The UDP server is responsible for monitoring and processing all
	 * incoming datagram packets and replying udp packets if necessary.
	 */
	protected UDPServer udpServer;

	/**
	 * Maintain the online peers who has already registered to the system.
	 */
	protected PeerMaintainer peerMaintainer;
	
	/**
	 * How many times a troubleshoot packet is sent out.
	 */
	protected final static boolean debug = true;
	protected final static int TRY_TIME = 2;
	
	/**
	 * Construct an empty peer with <code>PeerType</code> only.
	 * 
	 * @param peerType the type of the peer
	 */
	public AbstractPeer(AbstractInstance instance, String peerType)
	{
		this.instance = instance;
		this.peerType = peerType;
		this.eventManager = null;
	}
	
	/**
	 * Get the handler of the <code>AbstractInstance</code>.
	 * 
	 * @return the handler of the <code>AbstractInstance</code>
	 */
	public AbstractInstance getInstance()
	{
		return this.instance;
	}
	
	/**
	 * Get the peer type.
	 * 
	 * @return the type of the peer
	 */
	public String getPeerType()
	{
		return peerType;
	}
	
	// ------------- for tcp service ----------------
	
	/**
	 * Determine whether the instance of 
	 * <code>EventManager</code> is null.
	 * 
	 * @return if null, return <code>false</code>;
	 * 			otherwise, return <code>true</code>
	 */
	public boolean isEventManagerAlive()
	{
		if (eventManager == null)
			return false;
		return eventManager.isAlive();
	}
	
	/**
	 * Start the network monitoring service.
	 * 
	 * @param port the port used for monitoring network events
	 * @param capacity the maximum capacity that serves the incoming
	 * 					socket connections at the same time
	 * @return if success, return <code>true</code>; 
	 * 			otherwise, return <code>false</code>
	 */
	public abstract boolean startEventManager(int port, int capacity);
	
	/**
	 * Stop the network monitoring server.
	 * 
	 * @return if success, return <code>true</code>; 
	 * 			otherwise, return <code>false</code>
	 */
	public boolean stopEventManager()
	{
		if (isEventManagerAlive())
		{
			try
			{
				eventManager.stop();
				eventManager = null;
				return true;
			}
			catch (Exception e)
			{
				e.printStackTrace();
				return false;
			}
		}
		return true;
	}

	// --------------- for udp service -----------------
	
	/**
	 * Returns <code>true</code> if the server is alive.
	 * 
	 * @return <code>true</code> if alive; otherwise, return <code>false</code>
	 */
	public boolean isUDPServerAlive()
	{
		if (udpServer == null)
			return false;
		return udpServer.isAlive();
	}
	
	/**
	 * Starts the UDP service by specify the port and the number of
	 * threads that can handle incoming and outgoing UDP packets 
	 * at the same time. 
	 * 
	 * @param port the port to be used for starting the UDP service
	 * @param capacity the number of threads that can handle incoming
	 * 				   and outgoing UDP packets at the same time
	 * @return <code>true</code> if the UDP service is started; otherwise,
	 * 		   return <code>false</code>
	 */
	public abstract boolean startUDPServer(int port, int capacity, long period);
	
	/**
	 * Re-schedule the UDP Sender with a new time interval.
	 * 
	 * @param period the new period for disseminating UDP packets
	 */
	public void scheduleUDPSender(long period)
	{
		this.udpServer.scheduleUDPSender(period);
	}
	
	/**
	 * Stops the UDP service.
	 * 
	 * @return <code>true</code> if the UDP service is stoped; otherwise,
	 * 		   return <code>false</code>
	 */
	public boolean stopUDPServer()
	{
		if (this.isUDPServerAlive())
		{
			try
			{
				udpServer.stop();
				udpServer = null;
				return true;
			}
			catch (Exception e)
			{
				e.printStackTrace();
				return false;
			}
		}
		return true;
	}
	
	/**
	 * Force a peer out the network with its IP address and port.
	 * 
	 * @param ip the IP address of the peer to be forced out the network
	 * @param port the port where the network service is running
	 */
	public abstract void forceOut(String ip, int port);
	
	/**
	 * When the peer exits the system for some reason, it will broadcast 
	 * a TROUBLESHOOT message to all relevant peers for its leave.
	 * 
	 * @param toBoot if <code>true</code>, send messages to bootstrapper
	 * @param toServer if <code>true</code>, send messages to server peers
	 * @param toClient if <code>true</code>, send messages to client peers
	 */
	public abstract void troubleshoot(boolean toBoot, boolean toServer, boolean toClient);
	
	/**
	 * Returns a troubleshoot packet to indicate the peer who sends the packet
	 * will leave the system due to hardware or system errors.
	 * 
	 * <p>The packet format is:
	 * <pre>
	 * 0-3: the int value of TROUBLESHOOT
	 * 4-7: the number of bytes used for peer type, denoted as "tsize"
	 * 8-(tsize+7): the bytes array representing peer type 
	 * (tsize+8)-(tsize+11): the number of bytes used for peer identifier, denoted as "psize"
	 * (tsize+12)-(tsize+psize+11): the bytes array representing peer identifier   
	 * </pre>
	 * 
	 * @param type the peer type
	 * @param pid the peer identifier
	 * @return a <code>DatagramPacket</code> with troubleshoot message type
	 */
	protected synchronized DatagramPacket troubleshoot(PeerType type, String pid)
	{
		byte[] buf = new byte[type.getValue().length() + pid.length() + 12];
		
		// get message type
		int start = 0;
		int val  = MsgType.TROUBLESHOOT.getValue();
		Tools.intToByteArray(val, buf, start);

		// get the size of peer type
		start = 4;
		byte[] buf1 = type.getValue().getBytes();
		int size = buf1.length;
		Tools.intToByteArray(size, buf, start);
		
		// put peer type into byte array
		start = 8;
		for (int i = 0; i < size; i++)
		{
			buf[i + start] = buf1[i];
		}
		
		// get the size of the peer identifier
		start += size;
		byte[] buf2 = pid.getBytes();
		size = buf2.length;
		Tools.intToByteArray(size, buf, start);
		
		// put peer identifier into byte array
		start += 4;
		for (int i = 0; i < size; i++)
		{
			buf[i + start] = buf2[i];
		}
		
		// return packet
		start += size;
		return new DatagramPacket(buf, 0, start);
	}
	
	// ---------------- end of udp service -----------------
	
	/**
	 * Check if there exists an IP address in the BOOTSTRAP_SERVER_LIST
	 * whose value is equal to the current IP address of the peer.
	 * 
	 * @param ip the IP address list 
	 * @return if the current IP address is in the BOOTSTRAP_SERVER_LIST,
	 * 			return <code>true</code>; otherwise, return <code>false</code>
	 */
	public static boolean checkInet(String ip)
	{
		String inet = Inet.getInetAddress();
		String[] ipArray = ip.split(":");
		for (int i = 0; i < ipArray.length; i++)
		{
			if (ipArray[i].equalsIgnoreCase(inet))
			{
				return true;
			}
		}
		return false;
	}
	
	/**
	 * Get the instance of <code>PhysicalInfo</code>, which
	 * includes the physical information of the IP address
	 * and the port used for monitoring network events.
	 * 
	 * @return an instance of <code>PhysicalInfo</code>
	 * @throws UnknownHostException if cannot obtain a valid IP address
	 */
	public PhysicalInfo getPhysicalInfo() throws UnknownHostException
	{
		String ip = Inet.getInetAddress();
		if (ip == null)
		{
			throw new UnknownHostException("Cannot obtain IP address");
		}
		return (new PhysicalInfo(ip, LOCAL_SERVER_PORT));
	}
	
	/**
	 * Send a message to a peer.
	 * 
	 * @param dest the peer to which the message will be sent
	 * @param message the message to be sent
	 * @throws UnknownHostException, IOException
	 */
	public void sendMessage(PhysicalInfo dest, Message message) throws UnknownHostException, IOException
	{
		try
		{
			Socket socket = new Socket(dest.getIP(), dest.getPort());
			ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
			
			oos.writeObject(message);
			
			oos.close();
			socket.close();
			
			if (peerType.equals(PeerType.BOOTSTRAP.getValue()))
			{
//				if (BootstrapInstance.outputLog != null)
//				{
//					BootstrapInstance.outputLog.WriteLog(message.toString());
//				}
			}
			else if (peerType.equals(PeerType.SUPERPEER.getValue()))
			{
//				if (ServerInstance.outputLog != null)
//				{
//					ServerInstance.outputLog.WriteLog(message.toString());
//				}
			}
		}
		catch (Exception e)
		{
			System.out.println("Cannot send out the message to the destination node. Destination node is out");
		}
	}
}