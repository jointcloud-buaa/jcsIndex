/*
 * @(#) SPSearchExactBundleListener.java 1.0 2006-3-6
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.peer.event;

import edu.buaa.act.jcsindex.global.AbstractInstance;
import edu.buaa.act.jcsindex.global.peer.ServerPeer;
import edu.buaa.act.jcsindex.global.peer.info.IndexValue;
import edu.buaa.act.jcsindex.global.peer.info.PhysicalInfo;
import edu.buaa.act.jcsindex.global.peer.management.EventHandleException;
import edu.buaa.act.jcsindex.global.protocol.Head;
import edu.buaa.act.jcsindex.global.protocol.Message;
import edu.buaa.act.jcsindex.global.protocol.MsgType;
import edu.buaa.act.jcsindex.global.protocol.body.Body;
import edu.buaa.act.jcsindex.global.protocol.body.SPSearchExactBody;
import edu.buaa.act.jcsindex.global.protocol.body.SPSearchExactBundleBody;

import java.io.ObjectOutputStream;
import java.util.Vector;

/**
 * Implement a listener for processing SP_SEARCH_EXACT_BUNDLE message.
 * 
 * @author Vu Quang Hieu 
 * @version 1.0 2006-3-6
 */

public class SPSearchExactBundleListener extends ActionAdapter
{

	public SPSearchExactBundleListener(AbstractInstance instance)
	{
		super(instance);
	}

	public void actionPerformed(ObjectOutputStream oos, Message msg) throws EventHandleException
	{
    	Message result = null;
    	Head head = new Head();
    	Body searchBody;
    	
		try
		{
			/* get the handler of the ServerPeer */
			ServerPeer serverpeer = (ServerPeer) instance.peer();
			PhysicalInfo physicalInfo = serverpeer.getPhysicalInfo();
			
			/* get the message body */
			SPSearchExactBundleBody body = (SPSearchExactBundleBody) msg.getBody();
			Vector<IndexValue> searchedData = body.getData();
			
			head.setMsgType(MsgType.SP_SEARCH_EXACT.getValue());
			for (int i = 0; i < searchedData.size(); i++)
			{
				searchBody = new SPSearchExactBody(physicalInfo, null,
						body.getPhysicalRequester(), body.getLogicalRequester(),
						searchedData.get(i), null);
				result = new Message(head, searchBody);
				serverpeer.sendMessage(physicalInfo, result);
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new EventHandleException("Super peer locates index range for exact search failure", e);
		}
	}

	public boolean isConsumed(Message msg) throws EventHandleException 
	{
		if (msg.getHead().getMsgType() == MsgType.SP_SEARCH_EXACT_BUNDLE.getValue())
			return true;
		return false;
	}

}