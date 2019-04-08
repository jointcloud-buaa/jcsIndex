/*
 * @(#) JoinListener.java 1.0 2006-2-4
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.peer.event;

import edu.buaa.act.jcsindex.global.AbstractInstance;
import edu.buaa.act.jcsindex.global.peer.info.PeerInfo;
import edu.buaa.act.jcsindex.global.peer.management.EventHandleException;
import edu.buaa.act.jcsindex.global.peer.management.PeerMaintainer;
import edu.buaa.act.jcsindex.global.protocol.Message;
import edu.buaa.act.jcsindex.global.protocol.MsgType;
import edu.buaa.act.jcsindex.global.protocol.body.UserBody;

import java.io.ObjectOutputStream;

/**
 * Implements a listener for processing JOIN_SUCCESS or JOIN_FAILURE messages.
 * Both events occurs when a peer selects a super peer to join either the
 * super network or a super peer.
 * 
 * @author Xu Linhao
 * @version 1.0 2006-2-4
 */

public class JoinListener extends ActionAdapter
{

	public JoinListener(AbstractInstance instance)
	{
		super(instance);
	}

	public void actionPerformed(ObjectOutputStream oos, Message msg) throws EventHandleException
	{
		PeerMaintainer maintainer = PeerMaintainer.getInstance();
		if (msg.getHead().getMsgType() == MsgType.JOIN_SUCCESS.getValue())
		{
			try
			{
				UserBody ub = (UserBody) msg.getBody();
				String user = ub.getUserID();
				String ip   = ub.getIP();
				int port	= ub.getPort();
				String type = ub.getPeerType();

				PeerInfo peerInfo = new PeerInfo(user, ip, port, type);
				maintainer.put(peerInfo);
			}
			catch (Exception e)
			{
				e.printStackTrace();
				throw new EventHandleException("Peer join operation failure", e);
			}
		}
		else if (msg.getHead().getMsgType() == MsgType.JOIN_FAILURE.getValue())
		{
			try
			{
				UserBody ub = (UserBody) msg.getBody();
				String user = ub.getUserID();
				String ip   = ub.getIP();
				int port	= ub.getPort();
				String type = ub.getPeerType();
				
				PeerInfo peerInfo = new PeerInfo(user, ip, port, type);
				/* if have online super peers */
				if (maintainer.hasServer()) 
					maintainer.remove(peerInfo);
			}
			catch (Exception e)
			{
				e.printStackTrace();
				throw new EventHandleException("Peer join operation failure", e);
			}
		}
	}

	public boolean isConsumed(Message msg) throws EventHandleException 
	{
		if (msg.getHead().getMsgType() == MsgType.JOIN_SUCCESS.getValue())
		{
			return true;
		}
		else if (msg.getHead().getMsgType() == MsgType.JOIN_FAILURE.getValue())
		{
			return true;
		}
		return false;
	}
	
}