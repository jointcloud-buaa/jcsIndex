/*
 * @(#) SPUpdateRoutetableDirectlyListener.java 1.0 2006-2-22
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
import edu.buaa.act.jcsindex.global.protocol.body.SPUpdateRoutingTableDirectlyBody;
import edu.buaa.act.jcsindex.global.protocol.body.SPUpdateRoutingTableReplyBody;

import java.io.ObjectOutputStream;

/**
 * Implement a listener for processing SP_UPDATE_ROUTING_TABLE_DIRECTLY message.
 * 
 * @author Vu Quang Hieu
 * @author (Modified by) Xu Linhao
 * @version 1.0 2006-2-22
 */

public class SPUpdateRouteTableDirectlyListener extends ActionAdapter
{

	public SPUpdateRouteTableDirectlyListener(AbstractInstance instance)
	{
		super(instance);
	}

	public void actionPerformed(ObjectOutputStream oos, Message msg) throws EventHandleException
	{
		super.actionPerformed(oos, msg);
		
    	Message result = null;
    	Head thead = new Head();
    	Body tbody = null;
    	
		try
		{
			/* get the handler of the ServerPeer */
			ServerPeer serverpeer = (ServerPeer) instance.peer();
			
			/* get the message body */
			SPUpdateRoutingTableDirectlyBody body = (SPUpdateRoutingTableDirectlyBody) msg.getBody();
			
			TreeNode treeNode = serverpeer.getTreeNode(body.getLogicalDestination());
			if (treeNode == null)
			{
				System.out.println("SP_UPDATE_ROUTING_TABLE_DIRECTLY: Tree node is null, do not process the message");
				return;
			}
			
			/* update routing table */
			int index = body.getIndex();
			boolean direction = body.getDirection();
			RoutingItemInfo updatedNodeInfo = body.getInfoRequester();
			if (!direction)
			{
				treeNode.getLeftRoutingTable().setRoutingTableNode(updatedNodeInfo, index);
			}
			else
			{
				treeNode.getRightRoutingTable().setRoutingTableNode(updatedNodeInfo, index);
			}

	    	/* reply if necessary */
			if (updatedNodeInfo != null)
			{
				RoutingItemInfo nodeInfo = new RoutingItemInfo(serverpeer.getPhysicalInfo(),
															treeNode.getLogicalInfo(),
															treeNode.getLeftChild(),
															treeNode.getRightChild(),
															treeNode.getContent().getMinValue(),
															treeNode.getContent().getMaxValue());
				
				tbody = new SPUpdateRoutingTableReplyBody(serverpeer.getPhysicalInfo(),
														treeNode.getLogicalInfo(),
														nodeInfo, index, !direction,
														updatedNodeInfo.getLogicalInfo());
				
				thead.setMsgType(MsgType.SP_UPDATE_ROUTING_TABLE_REPLY.getValue());
				result = new Message(thead, tbody);			
				serverpeer.sendMessage(updatedNodeInfo.getPhysicalInfo(), result);

				if (treeNode.isNotifyImbalance() && (updatedNodeInfo.getLogicalInfo().equals(treeNode.getMissingNode())))
				{
					treeNode.notifyImbalance(false);
					treeNode.setMissingNode(null);
				}
			}

		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new EventHandleException("Super peer directly updates routing table failure", e);
		}
	}

	public boolean isConsumed(Message msg) throws EventHandleException 
	{
		if (msg.getHead().getMsgType() == MsgType.SP_UPDATE_ROUTING_TABLE_DIRECTLY.getValue())
			return true;
		return false;
	}

}