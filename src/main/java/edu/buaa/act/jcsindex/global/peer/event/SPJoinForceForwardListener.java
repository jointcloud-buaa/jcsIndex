/*
 * @(#) SPJoinForceForwardListener.java 1.0 2006-2-22
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.peer.event;

import edu.buaa.act.jcsindex.global.AbstractInstance;
import edu.buaa.act.jcsindex.global.peer.ServerPeer;
import edu.buaa.act.jcsindex.global.peer.info.*;
import edu.buaa.act.jcsindex.global.peer.management.EventHandleException;
import edu.buaa.act.jcsindex.global.protocol.Head;
import edu.buaa.act.jcsindex.global.protocol.Message;
import edu.buaa.act.jcsindex.global.protocol.MsgType;
import edu.buaa.act.jcsindex.global.protocol.body.SPJoinForcedForwardBody;
import edu.buaa.act.jcsindex.global.utils.PeerMath;

import java.io.ObjectOutputStream;

/**
 * Implement a listener for processing SP_JOIN_FORCED_FORWARD message.
 * 
 * @author Vu Quang Hieu
 * @author (Modified by) Xu Linhao
 * @version 1.0 2006-2-22
 */

public class SPJoinForceForwardListener extends ActionAdapter
{

	public SPJoinForceForwardListener(AbstractInstance instance)
	{
		super(instance);
	}

	public void actionPerformed(ObjectOutputStream oos, Message msg) throws EventHandleException
	{
		super.actionPerformed(oos, msg);
		
    	Message result = null;
    	Head thead = new Head();
    	
		try
		{
			/* get the handler of the ServerPeer */
			ServerPeer serverpeer = (ServerPeer) instance.peer();
			
			/* get the message body */
			SPJoinForcedForwardBody body = (SPJoinForcedForwardBody) msg.getBody();
			
			TreeNode treeNode = serverpeer.getTreeNode(body.getLogicalDestination());
			if (treeNode == null)
			{
				System.out.println("SP_JOIN_FORCED_FORWARD: Tree node is null, do not process the message");
				return;				
			}
			
			boolean direction = body.getDirection();

			if (!direction)
			{
				if (!treeNode.getRightAdjacentNode().getLogicalInfo().equals(body.getLogicalSender())) 
				{
					thead.setMsgType(MsgType.SP_JOIN_FORCED_FORWARD.getValue());
					result = new Message(thead, body);
					serverpeer.sendMessage(treeNode.getRightAdjacentNode().getPhysicalInfo(), result);
					return;
				}
			}
			else
			{
				if (!treeNode.getLeftAdjacentNode().getLogicalInfo().equals(body.getLogicalSender()))
				{
					thead.setMsgType(MsgType.SP_JOIN_FORCED_FORWARD.getValue());
					result = new Message(thead, body);
					serverpeer.sendMessage(treeNode.getLeftAdjacentNode().getPhysicalInfo(), result);
					return;
				}
			}

			ContentInfo content;
			ChildNodeInfo childNode;
			int childLevel;
			int childNumber;
			int numOfExpectedRTReply;
			if (treeNode.getLeftChild() == null)
			{
				childLevel = treeNode.getLogicalInfo().getLevel() + 1;
				childNumber = treeNode.getLogicalInfo().getNumber() * 2 - 1;
				childNode = new ChildNodeInfo(body.getPhysicalRequester(), new LogicalInfo(childLevel, childNumber));
				treeNode.setLeftChild(childNode);
				numOfExpectedRTReply = PeerMath.getNumberOfExpectedRTReply(true,
														treeNode.getRightChild() != null,
														treeNode.getLeftRoutingTable(),
														treeNode.getLeftRoutingTable());
				
				content = SPGeneralAction.splitData(treeNode, false, serverpeer.getOrder());
			}
			else
			{
				childLevel = treeNode.getLogicalInfo().getLevel() + 1;
				childNumber = treeNode.getLogicalInfo().getNumber() * 2;
				childNode = new ChildNodeInfo(body.getPhysicalRequester(), new LogicalInfo(childLevel, childNumber));
				treeNode.setRightChild(childNode);
				numOfExpectedRTReply = PeerMath.getNumberOfExpectedRTReply(false, false,
														treeNode.getLeftRoutingTable(),
														treeNode.getLeftRoutingTable());
				content = SPGeneralAction.splitData(treeNode, true, serverpeer.getOrder());
			}
			RoutingItemInfo nodeInfo = SPGeneralAction.doAccept(serverpeer, treeNode, body.getPhysicalRequester(),
														new LogicalInfo(childLevel, childNumber),
														content, numOfExpectedRTReply, false, false);
	    	
			SPGeneralAction.updateRoutingTable(serverpeer, treeNode, nodeInfo);
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new EventHandleException("SP_JOIN_FORCED_FORWARD operation failure", e);
		}
	}

	public boolean isConsumed(Message msg) throws EventHandleException 
	{
		if (msg.getHead().getMsgType() == MsgType.SP_JOIN_FORCED_FORWARD.getValue())
			return true;
		return false;
	}

}