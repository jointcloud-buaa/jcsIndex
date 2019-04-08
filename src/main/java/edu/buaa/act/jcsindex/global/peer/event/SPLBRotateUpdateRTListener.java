/*
 * @(#) SPLBRotateUpdateRTListener.java 1.0 2006-2-22
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.peer.event;

import edu.buaa.act.jcsindex.global.AbstractInstance;
import edu.buaa.act.jcsindex.global.peer.ServerPeer;
import edu.buaa.act.jcsindex.global.peer.info.RoutingItemInfo;
import edu.buaa.act.jcsindex.global.peer.info.TreeNode;
import edu.buaa.act.jcsindex.global.peer.management.EventHandleException;
import edu.buaa.act.jcsindex.global.protocol.Head;
import edu.buaa.act.jcsindex.global.protocol.Message;
import edu.buaa.act.jcsindex.global.protocol.MsgType;
import edu.buaa.act.jcsindex.global.protocol.body.Body;
import edu.buaa.act.jcsindex.global.protocol.body.SPLBRotateUpdateRoutingTableBody;
import edu.buaa.act.jcsindex.global.protocol.body.SPLBRotateUpdateRoutingTableReplyBody;

import java.io.ObjectOutputStream;

/**
 * Implement a listener for processing SP_LB_ROTATE_UPDATE_ROUTING_TABLE message.
 * 
 * @author Vu Quang Hieu
 * @author (Modified by) Xu Linhao
 * @version 1.0 2006-2-22
 */

public class SPLBRotateUpdateRTListener extends ActionAdapter
{

	public SPLBRotateUpdateRTListener(AbstractInstance instance)
	{
		super(instance);
	}

	public void actionPerformed(ObjectOutputStream oos, Message msg) throws EventHandleException
	{
		super.actionPerformed(oos, msg);
		
    	Head thead = new Head();
    	Body tbody = null;
    	
		try
		{
			/* get the handler of the ServerPeer */
			ServerPeer serverpeer = (ServerPeer) instance.peer();
			
			/* get the message body */
			SPLBRotateUpdateRoutingTableBody body = (SPLBRotateUpdateRoutingTableBody) msg.getBody();
			
			TreeNode treeNode = serverpeer.getTreeNode(body.getLogicalDestination());
			if (treeNode == null)
			{
				System.out.println("SP_LB_ROTATE_UPDATE_ROUTING_TABLE: Tree node is null, do not process the message");
				return;				
			}
			
			//1. update routing table
			int index = body.getIndex();
			RoutingItemInfo updatedNodeInfo = body.getInfoRequester();

			RoutingItemInfo tempInfo;
			boolean direction = body.getDirection();
			if (!direction)
			{
				tempInfo = treeNode.getLeftRoutingTable().getRoutingTableNode(index);
				if (tempInfo == null) 
				{
					return;
				}
				treeNode.getLeftRoutingTable().setRoutingTableNode(updatedNodeInfo, index);
			}
			else
			{
				tempInfo = treeNode.getRightRoutingTable().getRoutingTableNode(index);
				if (tempInfo == null) 
				{
					return;
				}
				treeNode.getRightRoutingTable().setRoutingTableNode(updatedNodeInfo, index);
			}

			//check existing knowledge

			RoutingItemInfo nodeInfo = new RoutingItemInfo(serverpeer.getPhysicalInfo(),
														treeNode.getLogicalInfo(),
														treeNode.getLeftChild(),
														treeNode.getRightChild(),
														treeNode.getContent().getMinValue(),
														treeNode.getContent().getMaxValue());
			
			tbody = new SPLBRotateUpdateRoutingTableReplyBody(serverpeer.getPhysicalInfo(),
														treeNode.getLogicalInfo(),
														nodeInfo, index, !direction,
														updatedNodeInfo.getLogicalInfo());

			thead.setMsgType(MsgType.SP_LB_ROTATE_UPDATE_ROUTING_TABLE_REPLY.getValue());
			Message message = new Message(thead, tbody);
			serverpeer.sendMessage(updatedNodeInfo.getPhysicalInfo(), message);
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new EventHandleException("SP_LB_ROTATE_UPDATE_ROUTING_TABLE operation failure", e);
		}
	}

	public boolean isConsumed(Message msg) throws EventHandleException 
	{
		if (msg.getHead().getMsgType() == MsgType.SP_LB_ROTATE_UPDATE_ROUTING_TABLE.getValue())
			return true;
		return false;
	}

}