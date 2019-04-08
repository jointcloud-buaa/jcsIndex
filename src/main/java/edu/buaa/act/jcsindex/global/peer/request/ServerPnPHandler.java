/*
 * @(#) ServerPnPHandler.java 1.0 2006-10-16
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.peer.request;

import edu.buaa.act.jcsindex.global.peer.info.PeerInfo;
import edu.buaa.act.jcsindex.global.protocol.MsgType;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

//import buaa.act.jointcloudstorage.peer.*;

/**
 * A particular <code>UDPHandler</code> is responsible for processing
 * <code>{@link MsgType#PING}</code>,
 * <code>{@link MsgType#PONG}</code>, and
 * <code>{@link MsgType#TROUBLESHOOT}</code> messages
 * from bootstrapper, server peers and client peers.
 * 
 * @author Xu Linhao
 * @version 1.0 2006-9-26
 * 
 * @see UDPHandler
 * @see ServerPnPReceiver
 */

public class ServerPnPHandler extends UDPHandler
{

	/**
	 * Construct the handler with a datagram socket.
	 * 
	 * @param ds the datagram socket
	 */
	public ServerPnPHandler(DatagramSocket ds) 
	{
		super(ds);
	}

	/**
	 * To handle UDP packets, four cases should be considered.
	 * <ul>
	 * <li>case 1: if receive a ping message from a peer (bootstrapper, server peer 
	 * or client peer), then return a pong message to the request peer.</li>
	 * <li>case 2: if receive a pong message from a peer (bootstrapper, server peer
	 * or client peer), then notify the UDPServer that the peer is still online.</li>
	 * <li>case 3: if receive a troubleshoot message from bootstrapper, then the server peer
	 * broadcasts a troubleshoot message to all server peers in its routing table and 
	 * all client peers and then leaves the system.</li> 
	 * <li>case 4: if receive a troubleshoot message from a server peer, then the corresponding
	 * BATON network maintenance operation should be triggered.</li>
	 * </ul>
	 */
	protected void handlePacket() 
	{
		// information used for replying 
		InetAddress iaddr = request.getAddress();
		String ipstr  	  = iaddr.getHostAddress();
		int port   		  = request.getPort();
		byte[] buf 		  = request.getData();
		PeerInfo info 	  = null;
		int debugMsgType  = 0;
		
		// parse message type
		int msgtype = this.parseType(buf, 0);
		
		/* case 1 */
		if (msgtype == MsgType.PING.getValue())
		{
			try 
			{
//				Session session = Session.getInstance();
//				DatagramPacket pong = this.pong(PeerType.SUPERPEER, session.getUserID());
				DatagramPacket pong = this.pong();
				pong.setAddress(iaddr);
				pong.setPort(port);
				
				if (debug)
				{
					byte[] debug = pong.getData();
					debugMsgType = this.parseType(debug, 0);
					System.out.println("case 5: pong message " + debugMsgType);
				}

				this.datagramSocket.send(pong);

				if (debug)
					System.out.println("case 6: response pong to " + pong.getAddress().getHostAddress() + " : " + pong.getPort());
			} 
			catch (IOException e) 
			{ /* ignore it */ }
		}
		/* case 2 */
		else if (msgtype == MsgType.PONG.getValue())
		{
//			info = this.parsePong(buf, 4);
			info = new PeerInfo(null, null, 0, null);
			info.setInetAddress(ipstr);
			info.setPort(port);
			
			// get a copy of listeners
			Object[] list;
			synchronized (this)
			{
				list = listeners.toArray().clone();
			}
			
			// for each listener, invoke it
			PnPListener listener;
			int size = list.length;
			for (int i = 0; i < size; i++)
			{
				listener = (PnPListener) list[i];
				listener.nodeAlive(info);
			}
		}
		/* case 3 and 4 */
		else if (msgtype == MsgType.TROUBLESHOOT.getValue())
		{
//			info = this.parseTroubleshoot(buf, 4);
			info = new PeerInfo(null, null, 0, null);
			info.setInetAddress(ipstr);
			info.setPort(port);
			
			// get a copy of listeners
			Object[] list;
			synchronized (this)
			{
				list = listeners.toArray().clone();
			}
			
			// for each listener, invoke it
			PnPListener listener;
			int size = list.length;
			for (int i = 0; i < size; i++)
			{
				listener = (PnPListener) list[i];
				listener.nodeFailure(info);
			}
		}
		else
		{
			throw new IllegalArgumentException("Unknown UDP message type: " + msgtype);
		}
	}

}