/*
 * @(#) SPLBGetLoadInfoReplyListener.java 1.0 2006-2-22
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.peer.event;

import edu.buaa.act.jcsindex.global.AbstractInstance;
import edu.buaa.act.jcsindex.global.peer.ServerPeer;
import edu.buaa.act.jcsindex.global.peer.info.TreeNode;
import edu.buaa.act.jcsindex.global.peer.management.EventHandleException;
import edu.buaa.act.jcsindex.global.protocol.Message;
import edu.buaa.act.jcsindex.global.protocol.MsgType;
import edu.buaa.act.jcsindex.global.protocol.body.SPLBGetLoadInfoReplyBody;

import java.io.ObjectOutputStream;

/**
 * Implement a listener for processing SP_LB_GET_LOAD_INFO_REPLY message.
 * 
 * @author Vu Quang Hieu
 * @author (Modified by) Xu Linhao
 * @version 1.0 2006-2-22
 */

public class SPLBGetLoadInfoReplyListener extends ActionAdapter
{

	public SPLBGetLoadInfoReplyListener(AbstractInstance instance)
	{
		super(instance);
	}

	public void actionPerformed(ObjectOutputStream oos, Message msg) throws EventHandleException
	{
		try
		{
			/* get the handler of the ServerPeer */
			ServerPeer serverpeer = (ServerPeer) instance.peer();
			
			/* get the message body */
			SPLBGetLoadInfoReplyBody body = (SPLBGetLoadInfoReplyBody) msg.getBody();
			
			TreeNode treeNode = serverpeer.getTreeNode(body.getLogicalDestination());
			if (treeNode == null)
			{
				System.out.println("SP_LB_GET_LOAD_INFO_REPLY: Tree node is null, do not process the message");
				return;				
			}
			
			if (!body.getDirection())
			{
				treeNode.setLNElement(body.getNumOfElement());
				treeNode.setLNOrder(body.getOrder());
				if (treeNode.getRNElement() != -2) 
				{
					new SPLoadBalance(serverpeer, treeNode).balanceLoad();
				}
			}
			else
			{
				treeNode.setRNElement(body.getNumOfElement());
				treeNode.setRNOrder(body.getOrder());
				if (treeNode.getRNElement() != -2) 
				{
					new SPLoadBalance(serverpeer, treeNode).balanceLoad();
				}
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new EventHandleException("SP_LB_GET_LOAD_INFO_REPLY operation failure", e);
		}
	}

	public boolean isConsumed(Message msg) throws EventHandleException 
	{
		if (msg.getHead().getMsgType() == MsgType.SP_LB_GET_LOAD_INFO_REPLY.getValue())
			return true;
		return false;
	}

}