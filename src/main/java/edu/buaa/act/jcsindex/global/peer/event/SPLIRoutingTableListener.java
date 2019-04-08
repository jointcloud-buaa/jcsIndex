/*
 * @(#) SPLIRoutingTableListener.java 1.0 2006-12-04
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.peer.event;

import edu.buaa.act.jcsindex.global.AbstractInstance;
import edu.buaa.act.jcsindex.global.peer.ServerPeer;
import edu.buaa.act.jcsindex.global.peer.info.LogicalInfo;
import edu.buaa.act.jcsindex.global.peer.info.RoutingItemInfo;
import edu.buaa.act.jcsindex.global.peer.info.TreeNode;
import edu.buaa.act.jcsindex.global.peer.management.EventHandleException;
import edu.buaa.act.jcsindex.global.protocol.Head;
import edu.buaa.act.jcsindex.global.protocol.Message;
import edu.buaa.act.jcsindex.global.protocol.MsgType;
import edu.buaa.act.jcsindex.global.protocol.body.*;

import java.io.ObjectOutputStream;

/**
 * Implement a listener for processing SP_LI_ROUTING_TABLE message.
 * 
 * @author Vu Quang Hieu
 * @version 1.0 2006-12-07
 */

public class SPLIRoutingTableListener extends ActionAdapter
{

	public SPLIRoutingTableListener(AbstractInstance instance)
	{
		super(instance);
	}

	public void actionPerformed(ObjectOutputStream oos, Message msg) throws EventHandleException
	{
		super.actionPerformed(oos, msg);
		
		try
		{
			/* get the handler of the ServerPeer */
			ServerPeer serverpeer = (ServerPeer) instance.peer();
			
			/* get the message body */
			SPLIRoutingTableBody body = (SPLIRoutingTableBody) msg.getBody();
			
			/* get the correspondent tree node*/
			TreeNode treeNode = serverpeer.getTreeNode(body.getLogicalDestination());
			if (treeNode == null)
			{
				System.out.println("SP_LI_ROUTING_TABLE: Tree node is null, do not process the message");
				return;				
			}
			
			LogicalInfo logicalFailedNode = body.getLogicalFailedNode();
			boolean direction = body.getDirection();
			int index = body.getIndex();
			
			if (logicalFailedNode.getLevel() == treeNode.getLogicalInfo().getLevel())
			{								
				RoutingItemInfo updateInfo = null;
				RoutingItemInfo nodeInfo = 
					new RoutingItemInfo(serverpeer.getPhysicalInfo(),
						treeNode.getLogicalInfo(), treeNode.getLeftChild(),
						treeNode.getRightChild(), treeNode.getContent().getMinValue(),
						treeNode.getContent().getMaxValue());
				
				if (direction == SPLIRoutingTableBody.FROM_LEFT_TO_RIGHT)
				{
					updateInfo = treeNode.getLeftRoutingTable().getRoutingTableNode(index);
				}
				else
				{
					updateInfo = treeNode.getRightRoutingTable().getRoutingTableNode(index);										
				}
				
				if (updateInfo != null)
				{
					updateInfo.setPhysicalInfo(body.getPhysicalRequester());
				}
				
				Body tbody = new SPLIRoutingTableReplyBody(serverpeer.getPhysicalInfo(),
						treeNode.getLogicalInfo(), nodeInfo, index, !direction,
						updateInfo.getMinValue(), updateInfo.getMaxValue(), 
						body.getLogicalRequester());
				Head thead = new Head();
				thead.setMsgType(MsgType.SP_LI_ROUTING_TABLE_REPLY.getValue());
				Message result = new Message(thead, tbody);			
				serverpeer.sendMessage(body.getPhysicalRequester(), result);
				
				if (index == 0)
				{
					if ((logicalFailedNode.getNumber() % 2 == 0) &&
						(direction == SPLIRoutingTableBody.FROM_RIGHT_TO_LEFT))
					{
						RoutingItemInfo routingItem = treeNode.getRightRoutingTable().getRoutingTableNode(0);
						Body tbody2 = new SPLIChildReplyBody(serverpeer.getPhysicalInfo(),
								treeNode.getLogicalInfo(), routingItem.getLeftChild(),
								routingItem.getRightChild(), body.getLogicalRequester());
						Head thead2 = new Head();
						thead2.setMsgType(MsgType.SP_LI_CHILD_REPLY.getValue());
						Message result2 = new Message(thead2, tbody2);
						serverpeer.sendMessage(body.getPhysicalRequester(), result2);
						
						if (routingItem.getLeftChild() != null)
						{
							Body tbody3 = new SPLIUpdateParentBody(serverpeer.getPhysicalInfo(),
									treeNode.getLogicalInfo(), body.getPhysicalRequester(),
									routingItem.getLeftChild().getLogicalInfo());
							Head thead3 = new Head();
							thead3.setMsgType(MsgType.SP_LI_UPDATE_PARENT.getValue());
							Message result3 = new Message(thead3, tbody3);
							serverpeer.sendMessage(routingItem.getLeftChild().getPhysicalInfo(), result3);
						}
						
						if (routingItem.getRightChild() != null)
						{
							Body tbody3 = new SPLIUpdateParentBody(serverpeer.getPhysicalInfo(),
									treeNode.getLogicalInfo(), body.getPhysicalRequester(),
									routingItem.getRightChild().getLogicalInfo());
							Head thead3 = new Head();
							thead3.setMsgType(MsgType.SP_LI_UPDATE_PARENT.getValue());
							Message result3 = new Message(thead3, tbody3);
							serverpeer.sendMessage(routingItem.getRightChild().getPhysicalInfo(), result3);
						}						
					}
					
					if ((logicalFailedNode.getNumber() % 2 == 1) &&
						(direction == SPLIRoutingTableBody.FROM_LEFT_TO_RIGHT))
					{
						RoutingItemInfo routingItem = treeNode.getLeftRoutingTable().getRoutingTableNode(0);
						Body tbody2 = new SPLIChildReplyBody(serverpeer.getPhysicalInfo(),
								treeNode.getLogicalInfo(), routingItem.getLeftChild(),
								routingItem.getRightChild(), body.getLogicalRequester());
						Head thead2 = new Head();
						thead2.setMsgType(MsgType.SP_LI_CHILD_REPLY.getValue());
						Message result2 = new Message(thead2, tbody2);
						serverpeer.sendMessage(body.getPhysicalRequester(), result2);
						
						if (routingItem.getLeftChild() != null)
						{
							Body tbody3 = new SPLIUpdateParentBody(serverpeer.getPhysicalInfo(),
									treeNode.getLogicalInfo(), body.getPhysicalRequester(),
									routingItem.getLeftChild().getLogicalInfo());
							Head thead3 = new Head();
							thead3.setMsgType(MsgType.SP_LI_UPDATE_PARENT.getValue());
							Message result3 = new Message(thead3, tbody3);
							serverpeer.sendMessage(routingItem.getLeftChild().getPhysicalInfo(), result3);
						}
						
						if (routingItem.getRightChild() != null)
						{
							Body tbody3 = new SPLIUpdateParentBody(serverpeer.getPhysicalInfo(),
									treeNode.getLogicalInfo(), body.getPhysicalRequester(),
									routingItem.getRightChild().getLogicalInfo());
							Head thead3 = new Head();
							thead3.setMsgType(MsgType.SP_LI_UPDATE_PARENT.getValue());
							Message result3 = new Message(thead3, tbody3);
							serverpeer.sendMessage(routingItem.getRightChild().getPhysicalInfo(), result3);
						}
					}
				}
			}
			else{				
				body.setPhysicalSender(serverpeer.getPhysicalInfo());
				body.setLogicalSender(treeNode.getLogicalInfo());
				
				//adjust index
				if (direction == SPLIRoutingTableBody.FROM_LEFT_TO_RIGHT)
				{
					if (logicalFailedNode.getNumber() % 2 == 1)
					{
						body.setIndex(index + 1);						
					}					
				}
				else
				{
					if (logicalFailedNode.getNumber() % 2 == 0)
					{
						body.setIndex(index + 1);
					}					
				}
				
				if (logicalFailedNode.getNumber() % 2 == 1)
				{
					if (treeNode.getLeftChild() != null)
					{
						body.setLogicalDestination(treeNode.getLeftChild().getLogicalInfo());
						
						Head thead = new Head();
						thead.setMsgType(MsgType.SP_LI_ROUTING_TABLE.getValue());
						Message result = new Message(thead, body);
						
						serverpeer.sendMessage(treeNode.getLeftChild().getPhysicalInfo(), result);
					}
				}
				else
				{
					if (treeNode.getRightChild() != null)
					{
						body.setLogicalDestination(treeNode.getRightChild().getLogicalInfo());
						
						Head thead = new Head();
						thead.setMsgType(MsgType.SP_LI_ROUTING_TABLE.getValue());
						Message result = new Message(thead, body);
						
						serverpeer.sendMessage(treeNode.getRightChild().getPhysicalInfo(), result);
					}
				}
			}			
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new EventHandleException("Message processing fails", e);
		}
	}

	public boolean isConsumed(Message msg) throws EventHandleException 
	{
		if (msg.getHead().getMsgType() == MsgType.SP_LI_ROUTING_TABLE.getValue())
			return true;
		return false;
	}

}