/*
 * @(#) SPLBGetLoadInfoListener.java 1.0 2006-2-22
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.peer.event;

import edu.buaa.act.jcsindex.global.AbstractInstance;
import edu.buaa.act.jcsindex.global.peer.ServerPeer;
import edu.buaa.act.jcsindex.global.peer.info.TreeNode;
import edu.buaa.act.jcsindex.global.peer.management.EventHandleException;
import edu.buaa.act.jcsindex.global.protocol.Head;
import edu.buaa.act.jcsindex.global.protocol.Message;
import edu.buaa.act.jcsindex.global.protocol.MsgType;
import edu.buaa.act.jcsindex.global.protocol.body.Body;
import edu.buaa.act.jcsindex.global.protocol.body.SPLBGetLoadInfoBody;
import edu.buaa.act.jcsindex.global.protocol.body.SPLBGetLoadInfoReplyBody;

import java.io.ObjectOutputStream;

/**
 * Implement a listener for processing SP_LB_GET_LOAD_INFO message.
 * 
 * @author Vu Quang Hieu
 * @author (Modified by) Xu Linhao
 * @version 1.0 2006-2-22
 */

public class SPLBGetLoadInfoListener extends ActionAdapter
{

	public SPLBGetLoadInfoListener(AbstractInstance instance)
	{
		super(instance);
	}

	public void actionPerformed(ObjectOutputStream oos, Message msg) throws EventHandleException
	{
		Message result = null;
    	Head thead = new Head();
    	Body tbody = null;
    	
		try
		{
			/* get the handler of the ServerPeer */
			ServerPeer serverpeer = (ServerPeer) instance.peer();
			
			/* get the message body */
			SPLBGetLoadInfoBody body = (SPLBGetLoadInfoBody) msg.getBody();
			
			TreeNode treeNode = serverpeer.getTreeNode(body.getLogicalDestination());
			if (treeNode == null)
			{
				System.out.println("SP_LB_GET_LOAD_INFO: Tree node is null, do not process the message");
				return;				
			}
			
			int numOfElement = treeNode.getContent().getData().size();
			boolean direction = body.getDirection();

			//special case, send to its adjacent which then leaves and transfers worklist back to itself
			if (treeNode.getLogicalInfo().equals(body.getLogicalSender()))
			{
				if (!direction)
				{
					treeNode.setRNElement(-1);
					if (treeNode.getRNElement() != -2)
					{
						new SPLoadBalance(serverpeer, treeNode).balanceLoad();
					}
				}
				else
				{
					treeNode.setRNElement(-1);
					if (treeNode.getRNElement() != -2)
					{
						new SPLoadBalance(serverpeer, treeNode).balanceLoad();
					}
				}
				return;
			}

			if (!body.getDirection())
			{
				tbody =	new SPLBGetLoadInfoReplyBody(serverpeer.getPhysicalInfo(), treeNode.getLogicalInfo(),
													numOfElement, treeNode.getContent().getOrder(), true,
													body.getLogicalSender());

				thead.setMsgType(MsgType.SP_LB_GET_LOAD_INFO_REPLY.getValue());
				result = new Message(thead, tbody);
				serverpeer.sendMessage(body.getPhysicalSender(), result);
	    	}
			else
			{
				tbody = new SPLBGetLoadInfoReplyBody(serverpeer.getPhysicalInfo(), treeNode.getLogicalInfo(),
	      	    									numOfElement, treeNode.getContent().getOrder(), false,
	      	    									body.getLogicalSender());

				thead.setMsgType(MsgType.SP_LB_GET_LOAD_INFO_REPLY.getValue());
				result = new Message(thead, tbody);
				serverpeer.sendMessage(body.getPhysicalSender(), result);
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new EventHandleException("SP_LB_GET_LOAD_INFO operation failure", e);
		}
	}

	public boolean isConsumed(Message msg) throws EventHandleException 
	{
		if (msg.getHead().getMsgType() == MsgType.SP_LB_GET_LOAD_INFO.getValue())
			return true;
		return false;
	}

}