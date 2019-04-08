/*
 * @(#) SPLBFindLightlyNodeListener.java 1.0 2006-2-22
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
import edu.buaa.act.jcsindex.global.utils.PeerMath;

import java.io.ObjectOutputStream;
import java.util.Vector;

/**
 * Implement a listener for processing SP_LB_FIND_LIGHTLY_NODE message.
 * 
 * @author Vu Quang Hieu
 * @author (Modified by) Xu Linhao
 * @version 1.0 2006-2-22
 */

public class SPLBFindLightlyNodeListener extends ActionAdapter
{

	public SPLBFindLightlyNodeListener(AbstractInstance instance)
	{
		super(instance);
	}

	public void actionPerformed(ObjectOutputStream oos, Message msg) throws EventHandleException
	{
    	Message result = null;
    	Head thead = new Head();
		Body tbody = null;
    	
		try
		{
			/* get the handler of the ServerPeer */
			ServerPeer serverpeer = (ServerPeer) instance.peer();
			
			/* get the message body */
			SPLBFindLightlyNodeBody body = (SPLBFindLightlyNodeBody) msg.getBody();
			
			TreeNode treeNode = serverpeer.getTreeNode(body.getLogicalDestination());
			if (treeNode == null)
			{
				System.out.println("SP_LB_FIND_LIGHTLY_NODE: Tree node is null, do not process the message");
				return;				
			}
			
			RoutingItemInfo temptInfo;
			boolean direction = body.getDirection();

			if (!direction)
			{
				if (!PeerMath.checkNodesSameTree(body.getLogicalOverloaded().getLevel(),
						body.getLogicalOverloaded().getNumber(),
						treeNode.getLogicalInfo().getLevel(),
						treeNode.getLogicalInfo().getNumber()))
				{
					tbody = new SPLBNoRotationNodeBody(serverpeer.getPhysicalInfo(),
							treeNode.getLogicalInfo(), body.getLogicalOverloaded());

					thead.setMsgType(MsgType.SP_LB_NO_ROTATION_NODE.getValue());
					result = new Message(thead, tbody);
					serverpeer.sendMessage(body.getPhysicalOverloaded(), result);
				}
				else
				{
					if ((treeNode.getRightChild() != null) 
							&& (!treeNode.getRightChild().getLogicalInfo().equals(body.getLogicalSender()))) 
					{
						body.setPhysicalSender(serverpeer.getPhysicalInfo());
						body.setLogicalSender(treeNode.getLogicalInfo());
						body.setNextIndex(body.getNextIndex() + 1);
						body.setLogicalDestination(treeNode.getRightChild().getLogicalInfo());

						thead.setMsgType(MsgType.SP_LB_FIND_LIGHTLY_NODE.getValue());
						result = new Message(thead, body);
						serverpeer.sendMessage(treeNode.getRightChild().getPhysicalInfo(), result);
					}
					else
					{
						if (((treeNode.getContent().getOrder() * 2) <= body.getOrder()) 
							&& (treeNode.getLeftChild() == null) 
							&& (treeNode.getRightChild() == null))
						{
							this.doLeaveAndJoin(serverpeer, treeNode, body);
						}
						else
						{
							int nextIndex = body.getNextIndex();
							if (treeNode.getRightRoutingTable().getTableSize() > nextIndex) 
							{
								temptInfo = treeNode.getRightRoutingTable().getRoutingTableNode(nextIndex);
								if (temptInfo != null)
								{
									body.setPhysicalSender(serverpeer.getPhysicalInfo());
									body.setLogicalSender(treeNode.getLogicalInfo());
									body.setNextIndex(nextIndex + 1);
									body.setLogicalDestination(temptInfo.getLogicalInfo());

									thead.setMsgType(MsgType.SP_LB_FIND_LIGHTLY_NODE.getValue());
									result = new Message(thead, body);
									serverpeer.sendMessage(temptInfo.getPhysicalInfo(), result);
								}
								else 
								{
									if ((nextIndex - 1) < 0)
									{
										tbody = new SPLBNoRotationNodeBody(serverpeer.getPhysicalInfo(), 
													treeNode.getLogicalInfo(),
													body.getLogicalOverloaded());

										thead.setMsgType(MsgType.SP_LB_NO_ROTATION_NODE.getValue());
										result = new Message(thead, tbody);
										serverpeer.sendMessage(body.getPhysicalOverloaded(), result);
									}
									else
									{
										body.setPhysicalSender(serverpeer.getPhysicalInfo());
										body.setLogicalSender(treeNode.getLogicalInfo());
										body.setNextIndex(nextIndex - 1);
										body.setLogicalDestination(treeNode.getParentNode().getLogicalInfo());

										thead.setMsgType(MsgType.SP_LB_FIND_LIGHTLY_NODE.getValue());
										result = new Message(thead, body);
										serverpeer.sendMessage(treeNode.getParentNode().getPhysicalInfo(), result);
									}
								}
							}
							else 
							{
								tbody = new SPLBNoRotationNodeBody(serverpeer.getPhysicalInfo(), 
														treeNode.getLogicalInfo(),
														body.getLogicalOverloaded());

								thead.setMsgType(MsgType.SP_LB_NO_ROTATION_NODE.getValue());
								result = new Message(thead, tbody);
								serverpeer.sendMessage(body.getPhysicalOverloaded(), result);
							}
						}
					}
				}
			}
			else
			{
				if (!PeerMath.checkNodesSameTree(treeNode.getLogicalInfo().getLevel(),
												treeNode.getLogicalInfo().getNumber(),
												body.getLogicalOverloaded().getLevel(),
												body.getLogicalOverloaded().getNumber()))
				{
					tbody = new SPLBNoRotationNodeBody(serverpeer.getPhysicalInfo(), 
												treeNode.getLogicalInfo(),
												body.getLogicalOverloaded());

					thead.setMsgType(MsgType.SP_LB_NO_ROTATION_NODE.getValue());
					result = new Message(thead, tbody);
					serverpeer.sendMessage(body.getPhysicalOverloaded(), result);
				}
				else
				{
					if ((treeNode.getLeftChild() != null) 
							&& (!treeNode.getLeftChild().getLogicalInfo().equals(body.getLogicalSender()))) 
					{
						body.setPhysicalSender(serverpeer.getPhysicalInfo());
						body.setLogicalSender(treeNode.getLogicalInfo());
						body.setNextIndex(body.getNextIndex() + 1);
						body.setLogicalDestination(treeNode.getLeftChild().getLogicalInfo());

						thead.setMsgType(MsgType.SP_LB_FIND_LIGHTLY_NODE.getValue());
						result = new Message(thead, body);
						serverpeer.sendMessage(treeNode.getLeftChild().getPhysicalInfo(), result);
					}
					else
					{
						if (((treeNode.getContent().getOrder() * 2) <= body.getOrder()) 
								&& (treeNode.getLeftChild() == null) && (treeNode.getRightChild() == null))
						{
							this.doLeaveAndJoin(serverpeer, treeNode, body);
						}
						else 
						{
							int nextIndex = body.getNextIndex();
							if (treeNode.getLeftRoutingTable().getTableSize() > nextIndex) {
								temptInfo = treeNode.getLeftRoutingTable().getRoutingTableNode(nextIndex);
								if (temptInfo != null)
								{
									body.setPhysicalSender(serverpeer.getPhysicalInfo());
									body.setLogicalSender(treeNode.getLogicalInfo());
									body.setNextIndex(nextIndex + 1);
									body.setLogicalDestination(temptInfo.getLogicalInfo());

									thead.setMsgType(MsgType.SP_LB_FIND_LIGHTLY_NODE.getValue());
									result = new Message(thead, body);
									serverpeer.sendMessage(temptInfo.getPhysicalInfo(), result);
								}
								else 
								{
									if (nextIndex - 1 < 0)
									{
										tbody = new SPLBNoRotationNodeBody(serverpeer.getPhysicalInfo(), 
															treeNode.getLogicalInfo(),
															body.getLogicalOverloaded());

										thead.setMsgType(MsgType.SP_LB_NO_ROTATION_NODE.getValue());
										result = new Message(thead, tbody);
										serverpeer.sendMessage(body.getPhysicalOverloaded(), result);
									}
									else
									{
										body.setPhysicalSender(serverpeer.getPhysicalInfo());
										body.setLogicalSender(treeNode.getLogicalInfo());
										body.setNextIndex(nextIndex - 1);
										body.setLogicalDestination(treeNode.getParentNode().getLogicalInfo());

										thead.setMsgType(MsgType.SP_LB_FIND_LIGHTLY_NODE.getValue());
										result = new Message(thead, body);
										serverpeer.sendMessage(treeNode.getParentNode().getPhysicalInfo(), result);
									}
								}
							}
							else 
							{
								tbody = new SPLBNoRotationNodeBody(serverpeer.getPhysicalInfo(), 
															treeNode.getLogicalInfo(),
															body.getLogicalOverloaded());

								thead.setMsgType(MsgType.SP_LB_NO_ROTATION_NODE.getValue());
								result = new Message(thead, tbody);
								serverpeer.sendMessage(body.getPhysicalOverloaded(), result);
							}
						}
					}
				}
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new EventHandleException("Find an underload super peer failure", e);
		}
	}

	/**
	 * Forced-leave its current position and forced-join as a child
	 * of the overloaded node
	 * 
	 * @param serverpeer the handler of <code>ServerPeer</code>
	 * @param treeNode information of the node in the tree structure
	 * @param body SPLBFindLightlyNode protocol
	 */
	private void doLeaveAndJoin(ServerPeer serverpeer, TreeNode treeNode, SPLBFindLightlyNodeBody body)
	{
		Message message = null;
		Head thead = new Head();
		Body tbody = null;
		
		RoutingItemInfo temptInfo;
		
		try
		{
			AdjacentNodeInfo adjacentInfo = new AdjacentNodeInfo(treeNode.getParentNode().getPhysicalInfo(),
																 treeNode.getParentNode().getLogicalInfo());
	
			//get transfer worklist
			Vector<String> transferWorkList = SPGeneralAction.getTransferWorkList(treeNode, treeNode.getParentNode().getLogicalInfo().toString());
	
			//1. send update adjacent request to its adjacent node and leave request to its parent
			if ((treeNode.getLogicalInfo().getNumber() % 2) == 0)
			{
				tbody = new SPLeaveBody(serverpeer.getPhysicalInfo(), treeNode.getLogicalInfo(),
										treeNode.getContent(), treeNode.getRightAdjacentNode(),
										true, transferWorkList, treeNode.getParentNode().getLogicalInfo());
				
				if (treeNode.getRightAdjacentNode() != null)
				{
					tbody = new SPUpdateAdjacentLinkBody(serverpeer.getPhysicalInfo(),
										treeNode.getLogicalInfo(), adjacentInfo, false,
										treeNode.getRightAdjacentNode().getLogicalInfo());
	
					thead.setMsgType(MsgType.SP_UPDATE_ADJACENT_LINK.getValue());
					message = new Message(thead, tbody);
					serverpeer.sendMessage(treeNode.getRightAdjacentNode().getPhysicalInfo(), message);
				}
			}
			else
			{
				tbody = new SPLeaveBody(serverpeer.getPhysicalInfo(), treeNode.getLogicalInfo(),
										treeNode.getContent(), treeNode.getLeftAdjacentNode(),
										false, transferWorkList, treeNode.getParentNode().getLogicalInfo());
	
				if (treeNode.getLeftAdjacentNode()!=null)
				{
					tbody = new SPUpdateAdjacentLinkBody(serverpeer.getPhysicalInfo(),
										treeNode.getLogicalInfo(), adjacentInfo, true,
										treeNode.getLeftAdjacentNode().getLogicalInfo());
	
					thead.setMsgType(MsgType.SP_UPDATE_ADJACENT_LINK.getValue());
					message = new Message(thead, tbody);
					serverpeer.sendMessage(treeNode.getLeftAdjacentNode().getPhysicalInfo(), message);
				}
			}
	
			thead.setMsgType(MsgType.SP_LEAVE.getValue());
			message = new Message(thead, tbody);
			serverpeer.sendMessage(treeNode.getParentNode().getPhysicalInfo(), message);
	
			//2. notify its neighbor nodes
			RoutingTableInfo leftRoutingTable = treeNode.getLeftRoutingTable();
			for (int i = 0; i < leftRoutingTable.getTableSize(); i++)
			{
				temptInfo = leftRoutingTable.getRoutingTableNode(i);
				if (temptInfo != null)
				{
					tbody = new SPUpdateRoutingTableDirectlyBody(serverpeer.getPhysicalInfo(),
																treeNode.getLogicalInfo(),
																null, i, true, temptInfo.getLogicalInfo());
	
					thead.setMsgType(MsgType.SP_UPDATE_ROUTING_TABLE_DIRECTLY.getValue());
					message = new Message(thead, tbody);
					serverpeer.sendMessage(temptInfo.getPhysicalInfo(), message);
				}
			}
			
			RoutingTableInfo rightRoutingTable = treeNode.getRightRoutingTable();
			for (int i = 0; i < rightRoutingTable.getTableSize(); i++)
			{
				temptInfo = rightRoutingTable.getRoutingTableNode(i);
				if (temptInfo != null)
				{
					tbody = new SPUpdateRoutingTableDirectlyBody(serverpeer.getPhysicalInfo(),
																treeNode.getLogicalInfo(), null, i, false,
																temptInfo.getLogicalInfo());
	
					thead.setMsgType(MsgType.SP_UPDATE_ROUTING_TABLE_DIRECTLY.getValue());
					message = new Message(thead, tbody);
					serverpeer.sendMessage(temptInfo.getPhysicalInfo(), message);
				}
			}
	
			//3. send forced-join request to the overloaded node
			tbody = new SPJoinForcedBody(serverpeer.getPhysicalInfo(), !body.getDirection(),
										body.getLogicalOverloaded());
	
			thead.setMsgType(MsgType.SP_JOIN_FORCED.getValue());
			message = new Message(thead, tbody);
			serverpeer.sendMessage(body.getPhysicalOverloaded(), message);
	
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
		if (msg.getHead().getMsgType() == MsgType.SP_LB_FIND_LIGHTLY_NODE.getValue())
			return true;
		return false;
	}

}