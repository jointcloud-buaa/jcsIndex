/*
 * @(#) SPPassClientListener.java 1.0 2006-3-5
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
import edu.buaa.act.jcsindex.global.protocol.body.SPPassClientBody;

import java.io.ObjectOutputStream;

/**
 * Implement a listener for processing SP_PASS_CLIENT message.
 * 
 * @author Vu Quang Hieu  
 * @version 1.0 2006-3-5
 */

public class SPPassClientListener extends ActionAdapter
{

	public SPPassClientListener(AbstractInstance instance)
	{
		super(instance);
	}

	public void actionPerformed(ObjectOutputStream oos, Message msg) throws EventHandleException
	{
		try
		{	
			/* get the message body */
			SPPassClientBody body = (SPPassClientBody) msg.getBody();
			
			PeerInfo[] attachedPeers  = body.getAttachedPeers();
			PeerMaintainer maintainer = PeerMaintainer.getInstance();
			
			/* update table component */
//			Pane pane = ((Serverinstance) instance).getPane();
//			int size = attachedPeers.length;
//			for (int i = 0; i < size; i++){
//				PeerInfo peerInfo = attachedPeers[i];
//				maintainer.put(peerInfo);
//				pane.firePeerTableRowInserted(pane.getPeerTableRowCount(), peerInfo.toObjectArray());
//			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new EventHandleException("SP_PASS_CLIENT operation failure", e);
		}
	}

	public boolean isConsumed(Message msg) throws EventHandleException 
	{
		if (msg.getHead().getMsgType() == MsgType.SP_PASS_CLIENT.getValue())
			return true;
		return false;
	}

}