/*
 * @(#) SPUpdateRouteTableIndirectlyListener.java 1.0 2006-2-22
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.peer.event;

import edu.buaa.act.jcsindex.global.AbstractInstance;
import edu.buaa.act.jcsindex.global.peer.ServerPeer;
import edu.buaa.act.jcsindex.global.peer.info.ChildNodeInfo;
import edu.buaa.act.jcsindex.global.peer.info.RoutingItemInfo;
import edu.buaa.act.jcsindex.global.peer.info.TreeNode;
import edu.buaa.act.jcsindex.global.peer.management.EventHandleException;
import edu.buaa.act.jcsindex.global.protocol.Head;
import edu.buaa.act.jcsindex.global.protocol.Message;
import edu.buaa.act.jcsindex.global.protocol.MsgType;
import edu.buaa.act.jcsindex.global.protocol.body.Body;
import edu.buaa.act.jcsindex.global.protocol.body.SPUpdateRoutingTableDirectlyBody;
import edu.buaa.act.jcsindex.global.protocol.body.SPUpdateRoutingTableIndirectlyBody;

import java.io.ObjectOutputStream;

/**
 * Implement a listener for processing SP_UPDATE_ROUTING_TABLE_INDIRECTLY message.
 * 
 * @author Vu Quang Hieu
 * @author (Modified by) Xu Linhao
 * @version 1.0 2006-2-22
 */

public class SPUpdateRouteTableIndirectlyListener extends ActionAdapter
{

	public SPUpdateRouteTableIndirectlyListener(AbstractInstance instance)
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
			SPUpdateRoutingTableIndirectlyBody body = (SPUpdateRoutingTableIndirectlyBody) msg.getBody();
			
			TreeNode treeNode = serverpeer.getTreeNode(body.getLogicalDestination());
			if (treeNode == null)
			{
				System.out.println("SP_UPDATE_ROUTING_TABLE_INDIRECTLY: Tree node is null, do not process the message");
				return;				
			}
			
			int index = body.getIndex();
			
			boolean direction = body.getDirection();
			boolean special   = body.getSpecial();
			
			RoutingItemInfo senderNodeInfo = body.getInfoSender();
			RoutingItemInfo newNodeInfo    = body.getInfoChild();
			
			if (!direction)
			{
				if (!special)
				{
					treeNode.getLeftRoutingTable().setRoutingTableNode(senderNodeInfo, index);
				}
				else
				{
					ChildNodeInfo newLeftChild = body.getInfoSender().getLeftChild();
					
					RoutingItemInfo updateNodeInfo = (RoutingItemInfo) treeNode.getLeftRoutingTable().getRoutingTableNode(index);

					// 此处有问题，先看是哪一个出现了问题
					System.out.println("HELLO SAM: " + updateNodeInfo == null);
					updateNodeInfo.setLeftChild(newLeftChild);
					treeNode.getLeftRoutingTable().setRoutingTableNode(updateNodeInfo, index);
				}
			}
			else
			{
				if (!special)
				{
					treeNode.getRightRoutingTable().setRoutingTableNode(senderNodeInfo, index);
				}
				else
				{
					ChildNodeInfo newLeftChild = body.getInfoSender().getLeftChild();
					
					RoutingItemInfo updateNodeInfo = (RoutingItemInfo)
					treeNode.getRightRoutingTable().getRoutingTableNode(index);
					
					updateNodeInfo.setLeftChild(newLeftChild);
					treeNode.getRightRoutingTable().setRoutingTableNode(updateNodeInfo, index);
				}
			}

			//forward to its children
			if (((newNodeInfo.getLogicalInfo().getNumber() % 2) == 0) && (treeNode.getRightChild() != null))
			{
				tbody = new SPUpdateRoutingTableDirectlyBody(serverpeer.getPhysicalInfo(),
															treeNode.getLogicalInfo(),
															newNodeInfo, index + 1, direction,
															treeNode.getRightChild().getLogicalInfo());

				thead.setMsgType(MsgType.SP_UPDATE_ROUTING_TABLE_DIRECTLY.getValue());
				result = new Message(thead, tbody);
				serverpeer.sendMessage(treeNode.getRightChild().getPhysicalInfo(), result);
			}
			
			if (((newNodeInfo.getLogicalInfo().getNumber() % 2) == 1) && (treeNode.getLeftChild() != null))
			{
				tbody = new SPUpdateRoutingTableDirectlyBody(serverpeer.getPhysicalInfo(),
															treeNode.getLogicalInfo(),
															newNodeInfo, index + 1, direction,
															treeNode.getLeftChild().getLogicalInfo());

				thead.setMsgType(MsgType.SP_UPDATE_ROUTING_TABLE_DIRECTLY.getValue());
				result = new Message(thead, tbody);
				serverpeer.sendMessage(treeNode.getLeftChild().getPhysicalInfo(), result);
			}
			
			//special cases for two adjacent nodes
			if ((!direction) && (index == 0) && ((newNodeInfo.getLogicalInfo().getNumber() % 2) == 0) 
					&& (treeNode.getLeftChild() != null))
			{
				tbody = new SPUpdateRoutingTableDirectlyBody(serverpeer.getPhysicalInfo(),
															treeNode.getLogicalInfo(),
															newNodeInfo, 0, direction,
															treeNode.getLeftChild().getLogicalInfo());

				thead.setMsgType(MsgType.SP_UPDATE_ROUTING_TABLE_DIRECTLY.getValue());
				result = new Message(thead, tbody);
				serverpeer.sendMessage(treeNode.getLeftChild().getPhysicalInfo(), result);
			}
			
			if ((direction) && (index == 0) && ((newNodeInfo.getLogicalInfo().getNumber() % 2) == 1) 
					&& (treeNode.getRightChild() != null))
			{
				tbody = new SPUpdateRoutingTableDirectlyBody(serverpeer.getPhysicalInfo(),
															treeNode.getLogicalInfo(),
															newNodeInfo, 0, direction,
															treeNode.getRightChild().getLogicalInfo());

				thead.setMsgType(MsgType.SP_UPDATE_ROUTING_TABLE_DIRECTLY.getValue());
				result = new Message(thead, tbody);
				serverpeer.sendMessage(treeNode.getRightChild().getPhysicalInfo(), result);
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new EventHandleException("Super peer indirectly updates routing table failure", e);
		}
	}

	public boolean isConsumed(Message msg) throws EventHandleException 
	{
		if (msg.getHead().getMsgType() == MsgType.SP_UPDATE_ROUTING_TABLE_INDIRECTLY.getValue())
			return true;
		return false;
	}

}