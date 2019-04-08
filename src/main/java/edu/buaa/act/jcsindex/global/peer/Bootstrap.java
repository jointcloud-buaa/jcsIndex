/*
 * @(#) Bootstrap.java 1.0 2006-2-1
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.peer;

import edu.buaa.act.jcsindex.global.AbstractInstance;
import edu.buaa.act.jcsindex.global.logging.LogManager;
import edu.buaa.act.jcsindex.global.peer.info.PeerInfo;
import edu.buaa.act.jcsindex.global.peer.management.BootstrapEventManager;
import edu.buaa.act.jcsindex.global.peer.management.PeerMaintainer;
import edu.buaa.act.jcsindex.global.peer.request.BootstrapPnPServer;
import edu.buaa.act.jcsindex.global.protocol.Head;
import edu.buaa.act.jcsindex.global.protocol.Message;
import edu.buaa.act.jcsindex.global.protocol.MsgType;
import edu.buaa.act.jcsindex.global.protocol.body.ForceOutBody;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.*;

/**
 * Implement a bootstrap.
 * 
 * @author Xu Linhao
 * @version 1.0 2006-2-1
 */

public class Bootstrap extends AbstractPeer
{
	/**
	 * Load system-defined value.
	 */
	public static void load()
	{
		// TODO: 暂时这么写
		LOCAL_SERVER_PORT = 30000;
		CAPACITY = 10;
	}
	
	/**
	 * Write user-defined values to file. Notice that this function must be
	 * called after user applies the change.
	 */
	public static void write()
	{
	}

	public Bootstrap(AbstractInstance instance, String peerType)
	{
		super(instance, peerType);
		this.peerMaintainer = PeerMaintainer.getInstance();	/* init online peer manager now */
	}

	// ----------- for tcp service -----------------
	
	@Override
	public boolean startEventManager(int port, int capacity) 
	{
		if (!isEventManagerAlive())
		{
			try
			{
				eventManager = new BootstrapEventManager(instance, port, capacity);
				new Thread(eventManager, "TCP Server").start();
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
	
	// ------------ for udp service -------------
	
	@Override
	public boolean startUDPServer(int port, int capacity, long period) 
	{
		if (!this.isUDPServerAlive())
		{
			try
			{
				udpServer = new BootstrapPnPServer(instance, port, capacity, period);
				new Thread(udpServer, "UDP Server").start();
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

	public void troubleshoot(boolean toBoot, boolean toServer, boolean toClient)
	{
		if (toServer && this.peerMaintainer.hasServer())
		{
			PeerInfo info = null;
			PeerInfo[] peerList = null;

			// init datagram packets
			PeerType type = PeerType.BOOTSTRAP;
			String pid = type.getValue();
			DatagramPacket trouble = this.troubleshoot(type, pid);
			DatagramSocket socket = this.udpServer.getDatagramSocket();
			try 
			{
				peerList = this.peerMaintainer.getServers();
				int size = peerList.length;
				for (int i = 0; i < size; i++)
				{
					info = peerList[i];
					trouble.setAddress(InetAddress.getByName(info.getInetAddress()));
					trouble.setPort(info.getPort());
					for (int j = 0; j < TRY_TIME; j++)
					{
						socket.send(trouble);
						if (debug)
							System.out.println("case 7: send troubleshoot out to " + trouble.getAddress().getHostAddress() + " : " + trouble.getPort());
					}
				} 
			}
			catch (UnknownHostException e) 
			{ /* ignore it */
			}
			catch (SocketException e) 
			{ /* ignore it */
			}
			catch (IOException e)
			{ /* ignore it */
			}
		}
	}
	
	@Override
	public void forceOut(String ip, int port) 
	{
		try
		{
			/* init socket connection with super peer */
			Socket socket = new Socket(ip, port);

			Head head = new Head();
			head.setMsgType(MsgType.FORCE_OUT.getValue());
			Message message = new Message(head, new ForceOutBody());

			/* send force out message to super peer */
			ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
			oos.writeObject(message);
			
			/* add log event */
//			((BootstrapInstance) instance).log(LogEventType.INFORMATION.getValue(), MsgType.description(head.getMsgType()) + " request is sent out...",	socket.getLocalAddress().getHostName(),	socket.getLocalAddress().getHostAddress() + ":" + socket.getLocalPort(), System.getProperty("user.name"));
			LogManager.getLogger(this).info(MsgType.description(head.getMsgType()) + " request is sent out from " + socket.getLocalAddress().getHostAddress() + ":" + socket.getLocalPort());
			
			/* close stream */
			oos.close();
			socket.close();
		}
		catch (Exception e)
		{
			// record exception event
//			((BootstrapInstance) instance).log(LogEventType.WARNING.getValue(), "Fail to open connection with super peer because:\r\n" + Tools.getException(e), Inet.getInetAddress2().getHostName(), Inet.getInetAddress(), System.getProperty("user.name"));
			LogManager.getLogger(this).error("Fail to open connection to super peer", e);
		}
	}
}