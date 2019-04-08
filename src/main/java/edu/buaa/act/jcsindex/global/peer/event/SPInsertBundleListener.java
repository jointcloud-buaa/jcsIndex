/*
 * @(#) SPInsertBundleListener.java 1.0 2006-3-6
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.peer.event;

import edu.buaa.act.jcsindex.global.AbstractInstance;
import edu.buaa.act.jcsindex.global.peer.ServerPeer;
import edu.buaa.act.jcsindex.global.peer.info.IndexInfo;
import edu.buaa.act.jcsindex.global.peer.info.IndexPair;
import edu.buaa.act.jcsindex.global.peer.info.IndexValue;
import edu.buaa.act.jcsindex.global.peer.info.PhysicalInfo;
import edu.buaa.act.jcsindex.global.peer.management.EventHandleException;
import edu.buaa.act.jcsindex.global.protocol.Head;
import edu.buaa.act.jcsindex.global.protocol.Message;
import edu.buaa.act.jcsindex.global.protocol.MsgType;
import edu.buaa.act.jcsindex.global.protocol.body.Body;
import edu.buaa.act.jcsindex.global.protocol.body.SPInsertBody;
import edu.buaa.act.jcsindex.global.protocol.body.SPInsertBundleBody;

import java.io.ObjectOutputStream;
import java.util.Vector;

/**
 * Implement a listener for processing SP_INSERT_BUNDLE message.
 * 
 * @author Vu Quang Hieu 
 * @version 1.0 2006-3-6
 */

public class SPInsertBundleListener extends ActionAdapter
{

	public SPInsertBundleListener(AbstractInstance instance)
	{
		super(instance);
	}

	public void actionPerformed(ObjectOutputStream oos, Message msg) throws EventHandleException
	{
		super.actionPerformed(oos, msg);
		
    	Message result = null;
    	Head head = new Head();
    	Body insertBody;
    	
		try
		{
			/* get the handler of the ServerPeer */
			ServerPeer serverpeer = (ServerPeer) instance.peer();
			PhysicalInfo physicalInfo = serverpeer.getPhysicalInfo();
			
			/* get the message body */
			SPInsertBundleBody body = (SPInsertBundleBody) msg.getBody();
			Vector<IndexPair> insertedData = body.getData();
			PhysicalInfo peerID = body.getPhysicalSender();
			String docID = body.getDocID();			
			
			head.setMsgType(MsgType.SP_INSERT.getValue());
			for (int i = 0; i < insertedData.size(); i++)
			{
				IndexPair indexPair = (IndexPair) insertedData.get(i);				
				IndexInfo indexInfo = new IndexInfo(docID);
				IndexValue indexValue = new IndexValue(IndexValue.STRING_TYPE, indexPair.getKeyword(), indexInfo);
				insertBody = new SPInsertBody(physicalInfo, null, indexValue, null);
				result = new Message(head, insertBody);
				serverpeer.sendMessage(physicalInfo, result);
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new EventHandleException("Super peer bundle insertion failure", e);
		}
	}

	public boolean isConsumed(Message msg) throws EventHandleException 
	{
		if (msg.getHead().getMsgType() == MsgType.SP_INSERT_BUNDLE.getValue())
			return true;
		return false;
	}

}