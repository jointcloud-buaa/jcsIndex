/*
 * @(#) SPNotifyImbalanceListener.java 1.0 2006-2-22
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
import edu.buaa.act.jcsindex.global.protocol.body.Body;
import edu.buaa.act.jcsindex.global.protocol.body.SPLBRotationPullBody;
import edu.buaa.act.jcsindex.global.protocol.body.SPNotifyImbalanceBody;
import edu.buaa.act.jcsindex.global.utils.PeerMath;

import java.io.ObjectOutputStream;
import java.util.Vector;

/**
 * Implement a listener for processing SP_NOTIFY_IMBALANCE message.
 * 
 * @author Vu Quang Hieu
 * @author (Modified by) Xu Linhao
 * @version 1.0 2006-2-22
 */

public class SPNotifyImbalanceListener extends ActionAdapter
{

	public SPNotifyImbalanceListener(AbstractInstance instance)
	{
		super(instance);
	}

	public void actionPerformed(ObjectOutputStream oos, Message msg) throws EventHandleException
	{
    	Message result = null;
		Head head = new Head();
    	
		try
		{
			/* get the handler of the ServerPeer */
			ServerPeer serverpeer = (ServerPeer) instance.peer();
			
			/* get the message body */
			SPNotifyImbalanceBody body = (SPNotifyImbalanceBody) msg.getBody();
			
			TreeNode treeNode = serverpeer.getTreeNode(body.getLogicalDestination());
			if (treeNode == null)
			{
				System.out.println("SP_NOTIFY_IMBALANCE: Tree node is null, do not process the message");
				return;				
			}
			
			LogicalInfo missingNode = body.getMissingNode();
			boolean direction = body.getDirection();
			
			RoutingItemInfo nodeInfo;
			int numOfExpectedRTReply;

			if (isMissingNodeChild(treeNode.getLogicalInfo(), missingNode))
			{
				if (((missingNode.getNumber() % 2) == 0) && (treeNode.getRightChild() == null))
				{
					//get number of expected routing table reply
					numOfExpectedRTReply = PeerMath.getNumberOfExpectedRTReply(false,
															treeNode.getLeftChild() != null,
															treeNode.getLeftRoutingTable(),
															treeNode.getRightRoutingTable());
					if (!direction)
					{
						//create a new node with its content
						nodeInfo = SPGeneralAction.doAccept(serverpeer, treeNode, serverpeer.getPhysicalInfo(),
															missingNode, treeNode.getContent(),
															numOfExpectedRTReply, false, false);
						
						treeNode.setRightChild(new ChildNodeInfo(serverpeer.getPhysicalInfo(), missingNode));
						treeNode.getContent().deleteAll();
						treeNode.getContent().setMaxValue(treeNode.getContent().getMinValue());
						
						SPGeneralAction.updateRoutingTable(serverpeer, treeNode, nodeInfo);
						this.pullNode(serverpeer, treeNode, true);
					}
					else
					{
						//create a new node with empty content
						ContentInfo emptyContent = new ContentInfo(treeNode.getContent().getMaxValue(),
															treeNode.getContent().getMaxValue(),
															serverpeer.getOrder(), new Vector<IndexValue>());
						
						nodeInfo = SPGeneralAction.doAccept(serverpeer, treeNode, treeNode.getRightAdjacentNode().getPhysicalInfo(),
															missingNode, emptyContent, numOfExpectedRTReply, true, false);
						treeNode.setRightChild(new ChildNodeInfo(treeNode.getRightAdjacentNode().getPhysicalInfo(), missingNode));
						
						//will update later, no need
						//SPGeneralAction.updateRoutingTable(peerNode, treeNode, nodeInfo);
					}
				}
				else if (((missingNode.getNumber() % 2) == 1) && (treeNode.getLeftChild() == null))
				{
					numOfExpectedRTReply = PeerMath.getNumberOfExpectedRTReply(true, treeNode.getRightChild() != null,
															treeNode.getLeftRoutingTable(),
															treeNode.getRightRoutingTable());
					
					if (!direction)
					{
						//create a new node with empty content
						ContentInfo emptyContent = new ContentInfo(treeNode.getContent().getMinValue(),
															treeNode.getContent().getMinValue(),
															serverpeer.getOrder(), new Vector<IndexValue>());

						nodeInfo = SPGeneralAction.doAccept(serverpeer, treeNode, treeNode.getLeftAdjacentNode().getPhysicalInfo(),
															missingNode, emptyContent,
															numOfExpectedRTReply, true, true);
						
						treeNode.setLeftChild(new ChildNodeInfo(treeNode.getLeftAdjacentNode().getPhysicalInfo(), missingNode));

						//will update later, no need
						//ProcessGeneral.updateRoutingTable(peerNode, treeNode, nodeInfo);
					}
					else
					{
						nodeInfo = SPGeneralAction.doAccept(serverpeer, treeNode, serverpeer.getPhysicalInfo(),
															missingNode, treeNode.getContent(),
															numOfExpectedRTReply, false, false);
						
						treeNode.setLeftChild(new ChildNodeInfo(serverpeer.getPhysicalInfo(), missingNode));
						treeNode.getContent().deleteAll();
						treeNode.getContent().setMinValue(treeNode.getContent().getMaxValue());
						
						SPGeneralAction.updateRoutingTable(serverpeer, treeNode, nodeInfo);
						this.pullNode(serverpeer, treeNode, false);
					}
				}
			}
			else
			{
				RoutingTableInfo leftRoutingTable = treeNode.getLeftRoutingTable();
				for (int i = 0; i < leftRoutingTable.getTableSize(); i++)
				{
					nodeInfo = leftRoutingTable.getRoutingTableNode(i);
					if (nodeInfo != null)
					{
						if (isMissingNodeChild(nodeInfo.getLogicalInfo(), missingNode))
						{
							body.setPhysicalSender(serverpeer.getPhysicalInfo());
							body.setLogicalSender(treeNode.getLogicalInfo());
							body.setLogicalDestination(nodeInfo.getLogicalInfo());
							
							head.setMsgType(MsgType.SP_NOTIFY_IMBALANCE.getValue());
							result = new Message(head, body);
							serverpeer.sendMessage(nodeInfo.getPhysicalInfo(), result);
							return;
						}
					}
				}
				
				RoutingTableInfo rightRoutingTable = treeNode.getRightRoutingTable();
				for (int i = 0; i < rightRoutingTable.getTableSize(); i++)
				{
					nodeInfo = rightRoutingTable.getRoutingTableNode(i);
					if (nodeInfo != null)
					{
						if (isMissingNodeChild(nodeInfo.getLogicalInfo(), missingNode))
						{
							body.setPhysicalSender(serverpeer.getPhysicalInfo());
							body.setLogicalSender(treeNode.getLogicalInfo());
							body.setLogicalDestination(nodeInfo.getLogicalInfo());
						
							head.setMsgType(MsgType.SP_NOTIFY_IMBALANCE.getValue());
							result = new Message(head, body);
							serverpeer.sendMessage(nodeInfo.getPhysicalInfo(), result);
							return;
						}
					}
				}
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new EventHandleException("Super peer notifies imbalance status to other peers failure", e);
		}
	}

	/**
	 * Return <code>true</code> if a child node is missed.
	 * 
	 * @param checkNode
	 * @param missingNode
	 * @return if a child node is missed, return <code>true</code>
	 */
	private boolean isMissingNodeChild(LogicalInfo checkNode, LogicalInfo missingNode)
	{
		if (checkNode.getLevel() == (missingNode.getLevel() - 1))
		{
			if (((checkNode.getNumber() * 2) == missingNode.getNumber()) 
				|| ((checkNode.getNumber() * 2 - 1) == missingNode.getNumber()))
			{
				return true;
			}
		}
		return false;
	}

	/**
	 * 
	 * 
	 * @param serverpeer
	 * @param treeNode
	 * @param direction
	 */
	private void pullNode(ServerPeer serverpeer, TreeNode treeNode, boolean direction) throws EventHandleException
	{
		Message result = null;
		Head head = new Head();
		Body body = null;

		try
		{
			if (serverpeer.getActivateStablePosition() != null)
			{
				TreeNode activateTreeNode = serverpeer.getActivateStablePosition().getTreeNode();
				if (treeNode.getLogicalInfo().equals(activateTreeNode.getLogicalInfo()))
				{
					serverpeer.stopActivateStablePosition();
				}
			}
	
			treeNode.setRole(0);
	
			if (!direction)
			{
				body = new SPLBRotationPullBody(serverpeer.getPhysicalInfo(), treeNode.getLogicalInfo(),
												treeNode, false);
				
				head.setMsgType(MsgType.SP_LB_ROTATION_PULL.getValue());
				result = new Message(head, body);
				serverpeer.sendMessage(treeNode.getRightAdjacentNode().getPhysicalInfo(), result);
				treeNode.clearCoOwnerList();
				treeNode.addCoOwnerList(treeNode.getRightAdjacentNode().getPhysicalInfo());
			}
			else
			{
				body = new SPLBRotationPullBody(serverpeer.getPhysicalInfo(), treeNode.getLogicalInfo(),
												treeNode, true);
	
				head.setMsgType(MsgType.SP_LB_ROTATION_PULL.getValue());
				result = new Message(head, body);
				serverpeer.sendMessage(treeNode.getLeftAdjacentNode().getPhysicalInfo(), result);
				treeNode.clearCoOwnerList();
				treeNode.addCoOwnerList(treeNode.getLeftAdjacentNode().getPhysicalInfo());
			}
			treeNode.deleteAllWork();
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new EventHandleException(e);
		}
	}
	
	public boolean isConsumed(Message msg) throws EventHandleException 
	{
		if (msg.getHead().getMsgType() == MsgType.SP_NOTIFY_IMBALANCE.getValue())
			return true;
		return false;
	}

}