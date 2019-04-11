/*
 * @(#) SPInsertListener.java 1.0 2006-2-22
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.peer.event;

import edu.buaa.act.jcsindex.global.AbstractInstance;
import edu.buaa.act.jcsindex.global.peer.ServerPeer;
import edu.buaa.act.jcsindex.global.peer.info.BoundaryValue;
import edu.buaa.act.jcsindex.global.peer.info.IndexValue;
import edu.buaa.act.jcsindex.global.peer.info.RoutingItemInfo;
import edu.buaa.act.jcsindex.global.peer.info.TreeNode;
import edu.buaa.act.jcsindex.global.peer.management.EventHandleException;
import edu.buaa.act.jcsindex.global.protocol.Head;
import edu.buaa.act.jcsindex.global.protocol.Message;
import edu.buaa.act.jcsindex.global.protocol.MsgType;
import edu.buaa.act.jcsindex.global.protocol.body.Body;
import edu.buaa.act.jcsindex.global.protocol.body.SPInsertBody;
import edu.buaa.act.jcsindex.global.protocol.body.SPLBFindLightlyNodeBody;

import java.io.ObjectOutputStream;

/**
 * Implement a listener for processing SP_INSERT message.
 * 
 * @author Vu Quang Hieu
 * @author (Modified by) Xu Linhao
 * @version 1.0 2006-2-22
 */

public class SPInsertListener extends ActionAdapter
{

	public SPInsertListener(AbstractInstance instance)
	{
		super(instance);
	}

	public void actionPerformed(ObjectOutputStream oos, Message msg) throws EventHandleException
	{
		super.actionPerformed(oos, msg);
		
    	Message result = null;
    	Head head = new Head();
    	
		try
		{
			/* get the handler of the ServerPeer */
			ServerPeer serverpeer = (ServerPeer) instance.peer();
			
			/* get the message body */
			SPInsertBody body = (SPInsertBody) msg.getBody();
			
			TreeNode treeNode = serverpeer.getTreeNode(body.getLogicalDestination());
			if (treeNode == null)
			{
				System.out.println("SP_INSERT: Tree node is null, do not process the message");
				return;				
			}
			
			IndexValue insertedData = body.getData();
			
			BoundaryValue minValue = treeNode.getContent().getMinValue();
			BoundaryValue maxValue = treeNode.getContent().getMaxValue();

			if ((insertedData.compareTo(minValue) >= 0) && (insertedData.compareTo(maxValue) < 0))
			{
				treeNode.getContent().insertData(insertedData, 0);
				/*doing load balancing*/
				
				if ((treeNode.getContent().isOverloaded()) &&
					(!treeNode.isProcessLoadBalance()))
				{
					if ((treeNode.getLeftChild() == null) && (treeNode.getRightChild() == null))
					{
						if (treeNode.getLogicalInfo().getNumber() % 2 == 0)
							this.doRotate(serverpeer, treeNode, true);
						else
							this.doRotate(serverpeer, treeNode, false);
					}
					else
						SPGeneralAction.doLoadBalance(serverpeer, treeNode, true, true);
				}
				
			}
			else
			{
				body.setPhysicalSender(serverpeer.getPhysicalInfo());
				body.setLogicalSender(treeNode.getLogicalInfo());
				
				if (minValue.compareTo(insertedData) > 0)
				{
					int index = treeNode.getLeftRoutingTable().getTableSize() - 1;
					int found = -1;
					while ((index >= 0) && (found == -1))
					{
						RoutingItemInfo nodeInfo = treeNode.getLeftRoutingTable().getRoutingTableNode(index);
						if (nodeInfo != null)
						{
							if (nodeInfo.getMaxValue().compareTo(insertedData)>0)
							{
								found = index;
							}
						}
						index--;
					}

					if (found != -1)
					{
						RoutingItemInfo transferInfo = treeNode.getLeftRoutingTable().getRoutingTableNode(found);
						body.setLogicalDestination(transferInfo.getLogicalInfo());

						head.setMsgType(MsgType.SP_INSERT.getValue());
						result = new Message(head, body);
						serverpeer.sendMessage(transferInfo.getPhysicalInfo(), result);
					}
					else
					{
						if (treeNode.getLeftChild() != null)
						{
							body.setLogicalDestination(treeNode.getLeftChild().getLogicalInfo());

							head.setMsgType(MsgType.SP_INSERT.getValue());
							result = new Message(head, body);
							serverpeer.sendMessage(treeNode.getLeftChild().getPhysicalInfo(), result);
						}
						else
						{
							if (treeNode.getLeftAdjacentNode() != null)
							{
								body.setLogicalDestination(treeNode.getLeftAdjacentNode().getLogicalInfo());

								head.setMsgType(MsgType.SP_INSERT.getValue());
								result = new Message(head, body);
								serverpeer.sendMessage(treeNode.getLeftAdjacentNode().getPhysicalInfo(), result);
							}
							else 
							{
								treeNode.getContent().insertData(insertedData, 1);
								SPGeneralAction.updateRangeValues(serverpeer, treeNode);
							}
						}
					}
				}
				else
				{
					int index = treeNode.getRightRoutingTable().getTableSize() - 1;
					int found = -1;
					while ((index >= 0) && (found == -1))
					{
						RoutingItemInfo nodeInfo = (RoutingItemInfo)treeNode.getRightRoutingTable().getRoutingTableNode(index); 
						if (nodeInfo != null)
						{
							if (nodeInfo.getMinValue().compareTo(insertedData)<=0)
							{
								found = index;
							}
						}
						index--;
					}
					
					if (found != -1)
					{
						RoutingItemInfo transferInfo = treeNode.getRightRoutingTable().getRoutingTableNode(found);
						body.setLogicalDestination(transferInfo.getLogicalInfo());

						head.setMsgType(MsgType.SP_INSERT.getValue());
						result = new Message(head, body);
						serverpeer.sendMessage(transferInfo.getPhysicalInfo(), result);
					}
					else
					{
						if (treeNode.getRightChild() != null)
						{
							body.setLogicalDestination(treeNode.getRightChild().getLogicalInfo());

							head.setMsgType(MsgType.SP_INSERT.getValue());
							result = new Message(head, body);
							serverpeer.sendMessage(treeNode.getRightChild().getPhysicalInfo(), result);
						}
						else
						{
							if (treeNode.getRightAdjacentNode() != null)
							{
								body.setLogicalDestination(treeNode.getRightAdjacentNode().getLogicalInfo());

								head.setMsgType(MsgType.SP_INSERT.getValue());
								result = new Message(head, body);
								serverpeer.sendMessage(treeNode.getRightAdjacentNode().getPhysicalInfo(), result);
							}
							else
							{
								treeNode.getContent().insertData(insertedData, 2);
								SPGeneralAction.updateRangeValues(serverpeer, treeNode);
							}
						}
					}
				}
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new EventHandleException("Super peer inserts index failure", e);
		}
	}

	/**
	 * Find a lightly loaded node to do global load balancing
	 * 
	 * @param serverpeer the handler of <code>ServerPeer</code>
	 * @param treeNode information of the node in the tree structure
	 * @param direction direction of searching
	 */
	private void doRotate(ServerPeer serverpeer, TreeNode treeNode, boolean direction)
	{
		RoutingItemInfo tempInfo;
		Head head = new Head();
		Body body = null;

		try
		{
			if (!direction)
			{
				tempInfo = treeNode.getRightRoutingTable().getRoutingTableNode(0);
			}
			else
			{
				tempInfo = treeNode.getLeftRoutingTable().getRoutingTableNode(0);
			}
	
			if (tempInfo != null) 
			{
				body = new SPLBFindLightlyNodeBody(serverpeer.getPhysicalInfo(),
						treeNode.getLogicalInfo(), serverpeer.getPhysicalInfo(), 
						treeNode.getLogicalInfo(), treeNode.getContent().getOrder(), 
						1, direction, treeNode.getNumOfQuery(), tempInfo.getLogicalInfo());
	
				head.setMsgType(MsgType.SP_LB_FIND_LIGHTLY_NODE.getValue());
				Message message = new Message(head, body);
				serverpeer.sendMessage(tempInfo.getPhysicalInfo(), message);
			}
			else 
			{
				body = new SPLBFindLightlyNodeBody(serverpeer.getPhysicalInfo(), 
						treeNode.getLogicalInfo(), serverpeer.getPhysicalInfo(), 
						treeNode.getLogicalInfo(), treeNode.getContent().getOrder(), 
						0, direction, treeNode.getNumOfQuery(), treeNode.getParentNode().getLogicalInfo());
	
				head.setMsgType(MsgType.SP_LB_FIND_LIGHTLY_NODE.getValue());
				Message message = new Message(head, body);
				serverpeer.sendMessage(treeNode.getParentNode().getPhysicalInfo(), message);
			}
			treeNode.processLoadBalance(true);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	public boolean isConsumed(Message msg) throws EventHandleException 
	{
		if (msg.getHead().getMsgType() == MsgType.SP_INSERT.getValue()) {
			// TODO: JUST FOR TEST
			System.out.println("***SP_INSERT");
			return true;
		}
		return false;
	}

}