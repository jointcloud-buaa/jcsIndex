/*
 * @(#) SPLeaveFindReplaceListener.java 1.0 2006-2-22
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.peer.event;

import edu.buaa.act.jcsindex.global.AbstractInstance;
import edu.buaa.act.jcsindex.global.peer.ServerPeer;
import edu.buaa.act.jcsindex.global.peer.info.AdjacentNodeInfo;
import edu.buaa.act.jcsindex.global.peer.info.RoutingItemInfo;
import edu.buaa.act.jcsindex.global.peer.info.RoutingTableInfo;
import edu.buaa.act.jcsindex.global.peer.info.TreeNode;
import edu.buaa.act.jcsindex.global.peer.management.EventHandleException;
import edu.buaa.act.jcsindex.global.protocol.Head;
import edu.buaa.act.jcsindex.global.protocol.Message;
import edu.buaa.act.jcsindex.global.protocol.MsgType;
import edu.buaa.act.jcsindex.global.protocol.body.*;

import java.io.ObjectOutputStream;
import java.util.Vector;

/**
 * Implement a listener for processing SP_LEAVE_FIND_REPLACEMENT_NODE message.
 * 
 * @author Vu Quang Hieu
 * @author (Modified by) Xu Linhao
 * @version 1.0 2006-2-22
 */
 
public class SPLeaveFindReplaceListener extends ActionAdapter
{

	public SPLeaveFindReplaceListener(AbstractInstance instance)
	{
		super(instance);
	}

	public void actionPerformed(ObjectOutputStream oos, Message msg) throws EventHandleException
	{
    	Message result = null;
    	Head thead = new Head();
    	
		try
		{
			/* get the handler of the ServerPeer */
			ServerPeer serverpeer = (ServerPeer) instance.peer();
			
			/* get the message body */
			SPLeaveFindReplacementNodeBody body = (SPLeaveFindReplacementNodeBody) msg.getBody();
			
			TreeNode treeNode = serverpeer.getTreeNode(body.getLogicalDestination());
			if (treeNode == null)
			{
				System.out.println("SP_LEAVE_FIND_REPLACEMENT_NODE: Tree node is null, do not process the message");
				return;				
			}
			
			RoutingTableInfo leftRoutingTable  = treeNode.getLeftRoutingTable();
			RoutingTableInfo rightRoutingTable = treeNode.getRightRoutingTable();
			RoutingItemInfo temptInfo, transferInfo = null;
			
			int i;

			if (treeNode.getLeftChild() != null)
			{
				// if it has left child
				body.setPhysicalSender(serverpeer.getPhysicalInfo());
				body.setLogicalSender(treeNode.getLogicalInfo());
				body.setLogicalDestination(treeNode.getLeftChild().getLogicalInfo());

				thead.setMsgType(MsgType.SP_LEAVE_FIND_REPLACEMENT_NODE.getValue());
				result = new Message(thead, body);
				serverpeer.sendMessage(treeNode.getLeftChild().getPhysicalInfo(), result);
	    	}
			else if(treeNode.getRightChild() != null)
			{
				// if it has right child
				body.setPhysicalSender(serverpeer.getPhysicalInfo());
				body.setLogicalSender(treeNode.getLogicalInfo());
				body.setLogicalDestination(treeNode.getRightChild().getLogicalInfo());

				thead.setMsgType(MsgType.SP_LEAVE_FIND_REPLACEMENT_NODE.getValue());
				result = new Message(thead, body);
				serverpeer.sendMessage(treeNode.getRightChild().getPhysicalInfo(), result);
	    	}
			else
			{
				// if it has no child, check routing table
				i = 0;
				while ((i < leftRoutingTable.getTableSize()) && (transferInfo == null))
				{
					temptInfo = leftRoutingTable.getRoutingTableNode(i);
					if (temptInfo != null)
					{
						if ( (temptInfo.getLeftChild() != null) || (temptInfo.getRightChild() != null))
						{
							transferInfo = temptInfo;
						}
					}
					i++;
				}

				i = 0;
				while ((i < rightRoutingTable.getTableSize()) && (transferInfo == null))
				{
					temptInfo = rightRoutingTable.getRoutingTableNode(i);
					if (temptInfo != null)
					{
						if ( (temptInfo.getLeftChild() != null) || (temptInfo.getRightChild() != null))
						{
							transferInfo = temptInfo;
						}
					}
					i++;
				}
				
				if (transferInfo != null)
				{
					body.setPhysicalSender(serverpeer.getPhysicalInfo());
					body.setLogicalSender(treeNode.getLogicalInfo());
					if (transferInfo.getLeftChild() != null)
					{
						body.setLogicalDestination(transferInfo.getLeftChild().getLogicalInfo());

						thead.setMsgType(MsgType.SP_LEAVE_FIND_REPLACEMENT_NODE.getValue());
						result = new Message(thead, body);
						serverpeer.sendMessage(transferInfo.getLeftChild().getPhysicalInfo(), result);
					}
					else
					{
						body.setLogicalDestination(transferInfo.getRightChild().getLogicalInfo());

						thead.setMsgType(MsgType.SP_LEAVE_FIND_REPLACEMENT_NODE.getValue());
						result = new Message(thead, body);
						serverpeer.sendMessage(transferInfo.getRightChild().getPhysicalInfo(), result);
					}
				}
				else
				{
					doLeaveAndReplace(serverpeer, treeNode, body);
				}
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new EventHandleException("Super peer finds a replacement peer failure when it leaves network", e);
		}
	}

	private void doLeaveAndReplace(ServerPeer serverpeer, TreeNode treeNode, SPLeaveFindReplacementNodeBody body)
	{
		Message result = null;
		Head thead = new Head();
		Body tbody = null, tbodyLeave = null;
		
		try
		{
			AdjacentNodeInfo adjacentInfo;
			Vector<String> transferWorkList;
			
			int i;

			adjacentInfo = new AdjacentNodeInfo(treeNode.getParentNode().getPhysicalInfo(),	treeNode.getParentNode().getLogicalInfo());
			transferWorkList = SPGeneralAction.getTransferWorkList(treeNode, treeNode.getParentNode().getLogicalInfo().toString());

			/*
			 * send update adjacent request to its adjacent node,
			 * and leave request to its parent
			 */
			treeNode.getContent().setData(SPGeneralAction.loadData());
			
			if ((treeNode.getLogicalInfo().getNumber() % 2) == 0)
			{
				tbodyLeave = new SPLeaveBody(serverpeer.getPhysicalInfo(), treeNode.getLogicalInfo(),
						treeNode.getContent(), treeNode.getRightAdjacentNode(),
						true, transferWorkList,	treeNode.getParentNode().getLogicalInfo());
				
				if (treeNode.getRightAdjacentNode() != null)
				{
					tbody = new SPUpdateAdjacentLinkBody(serverpeer.getPhysicalInfo(),
							treeNode.getLogicalInfo(), adjacentInfo, false,
							treeNode.getRightAdjacentNode().getLogicalInfo());

					thead.setMsgType(MsgType.SP_UPDATE_ADJACENT_LINK.getValue());
					result = new Message(thead, tbody);
					serverpeer.sendMessage(treeNode.getRightAdjacentNode().getPhysicalInfo(), result);
				}
			}
			else
			{
				tbodyLeave = new SPLeaveBody(serverpeer.getPhysicalInfo(), treeNode.getLogicalInfo(),
						treeNode.getContent(), treeNode.getLeftAdjacentNode(),
						false, transferWorkList, treeNode.getParentNode().getLogicalInfo());
				
				if (treeNode.getLeftAdjacentNode() != null)
				{
					tbody = new SPUpdateAdjacentLinkBody(serverpeer.getPhysicalInfo(),
							treeNode.getLogicalInfo(), adjacentInfo, true,
							treeNode.getLeftAdjacentNode().getLogicalInfo());

					thead.setMsgType(MsgType.SP_UPDATE_ADJACENT_LINK.getValue());
					result = new Message(thead, tbody);
					serverpeer.sendMessage(treeNode.getLeftAdjacentNode().getPhysicalInfo(), result);
				}
			}

			thead.setMsgType(MsgType.SP_LEAVE.getValue());
			result = new Message(thead, tbodyLeave);
			serverpeer.sendMessage(treeNode.getParentNode().getPhysicalInfo(), result);

			/*
			 * notify its neighbor nodes
			 */ 
			RoutingTableInfo leftRoutingTable = treeNode.getLeftRoutingTable();
			RoutingItemInfo temptInfo;

			for (i = 0; i < leftRoutingTable.getTableSize(); i++)
			{
				temptInfo = leftRoutingTable.getRoutingTableNode(i);
				if (temptInfo != null)
				{
					tbody = new SPUpdateRoutingTableDirectlyBody(serverpeer.getPhysicalInfo(),
																treeNode.getLogicalInfo(),
																null, i, true, temptInfo.getLogicalInfo());

					thead.setMsgType(MsgType.SP_UPDATE_ROUTING_TABLE_DIRECTLY.getValue());
					result = new Message(thead, tbody);
					serverpeer.sendMessage(temptInfo.getPhysicalInfo(), result);
				}
			}

			RoutingTableInfo rightRoutingTable = treeNode.getRightRoutingTable();
			for (i = 0; i < rightRoutingTable.getTableSize(); i++)
			{
				temptInfo = rightRoutingTable.getRoutingTableNode(i);
				if (temptInfo != null)
				{
					tbody =	new SPUpdateRoutingTableDirectlyBody(serverpeer.getPhysicalInfo(),
																treeNode.getLogicalInfo(),
																null, i, false, temptInfo.getLogicalInfo());

					thead.setMsgType(MsgType.SP_UPDATE_ROUTING_TABLE_DIRECTLY.getValue());
					result = new Message(thead, tbody);
					serverpeer.sendMessage(temptInfo.getPhysicalInfo(), result);
				}
			}

			/*
			 * ready to replace
			 */ 
			tbody =	new SPLeaveFindReplacementNodeReplyBody(serverpeer.getPhysicalInfo(),
															body.getLogicalRequester());

			thead.setMsgType(MsgType.SP_LEAVE_FIND_REPLACEMENT_NODE_REPLY.getValue());
			result = new Message(thead, tbody);
			serverpeer.sendMessage(body.getPhysicalRequester(), result);

			//4. delete logical node
			SPGeneralAction.deleteTreeNode(serverpeer, treeNode);	
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	public boolean isConsumed(Message msg) throws EventHandleException 
	{
		if (msg.getHead().getMsgType() == MsgType.SP_LEAVE_FIND_REPLACEMENT_NODE.getValue())
			return true;
		return false;
	}

}