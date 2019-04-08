/*
 * @(#) AttachListener.java 1.0 2006-2-1
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.peer.event;

import edu.buaa.act.jcsindex.global.AbstractInstance;
import edu.buaa.act.jcsindex.global.peer.ServerPeer;
import edu.buaa.act.jcsindex.global.peer.info.PeerInfo;
import edu.buaa.act.jcsindex.global.peer.info.PhysicalInfo;
import edu.buaa.act.jcsindex.global.peer.management.EventHandleException;
import edu.buaa.act.jcsindex.global.peer.management.PeerMaintainer;
import edu.buaa.act.jcsindex.global.protocol.Head;
import edu.buaa.act.jcsindex.global.protocol.Message;
import edu.buaa.act.jcsindex.global.protocol.MsgType;
import edu.buaa.act.jcsindex.global.protocol.body.Body;
import edu.buaa.act.jcsindex.global.protocol.body.ConfirmBody;
import edu.buaa.act.jcsindex.global.protocol.body.UserBody;

import java.io.ObjectOutputStream;

/**
 * Implement a listener for processing ATTACH_REQUEST message.
 * 
 * @author Xu Linhao
 * @version 1.0 2006-2-1
 */

public class AttachListener extends ActionAdapter
{

	public AttachListener(AbstractInstance instance)
	{
		super(instance);
	}

	public void actionPerformed(ObjectOutputStream oos, Message msg) throws EventHandleException
	{
    	Message result = null;
    	Head thead = null;
    	
    	PeerInfo peerInfo = null;
    	
    	boolean checkpoint1 = false;
    	boolean checkpoint2 = false;
    	
    	PeerMaintainer maintainer = PeerMaintainer.getInstance();
		try
		{
			UserBody ub = (UserBody) msg.getBody();
			String user = ub.getUserID();
			String ip   = ub.getIP();
			int port	= ub.getPort();
			String type = ub.getPeerType();

			peerInfo = new PeerInfo(user, ip, port, type);
			maintainer.put(peerInfo);
			
			checkpoint1 = true;
			
			/* update table component */
			//Pane pane = ((Serverinstance) instance).getPane();
			//pane.firePeerTableRowInserted(pane.getPeerTableRowCount(), peerInfo.toObjectArray());
			
			checkpoint2 = true;
			
			/* response message */
			thead = new Head();
			thead.setMsgType(MsgType.ATTACH_SUCCESS.getValue());
			
			ServerPeer serverpeer = (ServerPeer) instance.peer();
			PhysicalInfo info = serverpeer.getPhysicalInfo();
		}
		catch (Exception e)
		{
			if (checkpoint1)
			{
				/* remove the peer from online peer manager */
				maintainer.remove(peerInfo);
				if (checkpoint2)
				{
					//Pane pane = ((Serverinstance) instance).getPane();
					//pane.firePeerTableRowRemoved(peerInfo);
				}
			}

			/* create reply message */
			thead = new Head();
			thead.setMsgType(MsgType.ATTACH_FAILURE.getValue());
			Body tbody = new ConfirmBody();
			result = new Message(thead, tbody);
		}
		
		/* write result to the request peer */
		result.serialize(oos);
	}

	public boolean isConsumed(Message msg) throws EventHandleException 
	{
		if (msg.getHead().getMsgType() == MsgType.ATTACH_REQUEST.getValue())
			return true;
		return false;
	}

}