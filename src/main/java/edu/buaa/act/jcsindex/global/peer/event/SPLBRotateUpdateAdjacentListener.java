/*
 * @(#) SPLBRotateUpdateAdjacentListener.java 1.0 2006-2-22
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.peer.event;

import edu.buaa.act.jcsindex.global.AbstractInstance;
import edu.buaa.act.jcsindex.global.peer.ServerPeer;
import edu.buaa.act.jcsindex.global.peer.info.AdjacentNodeInfo;
import edu.buaa.act.jcsindex.global.peer.info.TreeNode;
import edu.buaa.act.jcsindex.global.peer.management.EventHandleException;
import edu.buaa.act.jcsindex.global.protocol.Head;
import edu.buaa.act.jcsindex.global.protocol.Message;
import edu.buaa.act.jcsindex.global.protocol.MsgType;
import edu.buaa.act.jcsindex.global.protocol.body.Body;
import edu.buaa.act.jcsindex.global.protocol.body.SPLBRotateUpdateAdjacentBody;
import edu.buaa.act.jcsindex.global.protocol.body.SPLBRotateUpdateAdjacentReplyBody;

import java.io.ObjectOutputStream;

/**
 * Implement a listener for processing SP_LB_ROTATE_UPDATE_ADJACENT message.
 * 
 * @author Vu Quang Hieu
 * @author (Modified by) Xu Linhao
 * @version 1.0 2006-2-22
 */

public class SPLBRotateUpdateAdjacentListener extends ActionAdapter
{

	public SPLBRotateUpdateAdjacentListener(AbstractInstance instance)
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
			SPLBRotateUpdateAdjacentBody body = (SPLBRotateUpdateAdjacentBody) msg.getBody();
			
			TreeNode treeNode = serverpeer.getTreeNode(body.getLogicalDestination());
			if (treeNode == null)
			{
				System.out.println("SP_LB_ROTATE_UPDATE_ADJACENT: Tree node is null, do not process the message");
				return;				
			}
			
			AdjacentNodeInfo newAdjacent = new AdjacentNodeInfo(body.getPhysicalSender(), body.getLogicalSender());

			boolean direction = body.getDirection();
			if (!direction)
			{
				if (treeNode.getLeftAdjacentNode().getLogicalInfo().equals(newAdjacent.getLogicalInfo()))
					treeNode.setLeftAdjacentNode(newAdjacent);
			}
			else
			{
				if (treeNode.getRightAdjacentNode().getLogicalInfo().equals(newAdjacent.getLogicalInfo()))
					treeNode.setRightAdjacentNode(newAdjacent);
			}

			tbody = new SPLBRotateUpdateAdjacentReplyBody(serverpeer.getPhysicalInfo(), treeNode.getLogicalInfo(),
														!direction, body.getLogicalSender());

			thead.setMsgType(MsgType.SP_LB_ROTATE_UPDATE_ADJACENT_REPLY.getValue());
			Message message = new Message(thead, tbody);
			serverpeer.sendMessage(body.getPhysicalSender(), message);
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new EventHandleException("SP_LB_ROTATE_UPDATE_ADJACENT operation failure", e);
		}
	}

	public boolean isConsumed(Message msg) throws EventHandleException 
	{
		if (msg.getHead().getMsgType() == MsgType.SP_LB_ROTATE_UPDATE_ADJACENT.getValue())
			return true;
		return false;
	}

}
