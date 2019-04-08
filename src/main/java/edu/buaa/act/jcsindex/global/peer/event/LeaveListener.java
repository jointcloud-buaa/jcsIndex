/*
 * @(#) LeaveListener.java 1.0 2006-2-4
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
 * Implement a listener when a peer leaves the network 
 * or detaches from a super peer.
 * 
 * @author Xu Linhao
 * @version 1.0 2006-2-4
 */

public class LeaveListener extends ActionAdapter
{

	public LeaveListener(AbstractInstance instance)
	{
		super(instance);
	}

	public void actionPerformed(ObjectOutputStream oos, Message msg) throws EventHandleException
	{
		try
		{
			UserBody ub = (UserBody) msg.getBody();
			String user = ub.getUserID();
			String ip   = ub.getIP();
			int port	= ub.getPort();
			String type = ub.getPeerType();
			
			PeerInfo peerInfo = new PeerInfo(user, ip, port, type);

			/* judge whether exist online super peers */
			PeerMaintainer maintainer = PeerMaintainer.getInstance();
			
			/*
			 * if have online super peers
			 */
			if (maintainer.hasServer()) 
			{
				maintainer.remove(peerInfo);
			}
			else if (maintainer.hasClient())
			{
				maintainer.remove(peerInfo);
				//buaa.act.jointcloudstorage.instance.server.Pane pane = ((Serverinstance) instance).getPane();
				//pane.firePeerTableRowRemoved(peerInfo);
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new EventHandleException("Peer departure operation failure", e);
		}
	}

	public boolean isConsumed(Message msg) throws EventHandleException 
	{
		if (msg.getHead().getMsgType() == MsgType.I_WILL_LEAVE.getValue())
			return true;
		return false;
	}
	
}