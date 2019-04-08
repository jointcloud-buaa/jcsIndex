/*
 * @(#) SPJoinListener.java 1.0 2006-2-22
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
import edu.buaa.act.jcsindex.global.protocol.body.SPJoinBody;
import edu.buaa.act.jcsindex.global.protocol.body.SPJoinSplitDataBody;
import edu.buaa.act.jcsindex.global.utils.PeerMath;

import java.io.ObjectOutputStream;

/**
 * Implement a listener for processing SP_JOIN message.
 * 
 * @author Vu Quang Hieu
 * @author (Modified by) Xu Linhao
 * @version 1.0 2006-2-22
 */

public class SPJoinListener extends ActionAdapter
{

	public SPJoinListener(AbstractInstance instance)
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
			SPJoinBody body = (SPJoinBody) msg.getBody();


			TreeNode treeNode = serverpeer.getTreeNode(body.getLogicalDestination());
			if (treeNode == null)
			{
				System.out.println("SP_JOIN: Tree node is null, do not process the message");
				return;
			}


			int indexLeftRTMiss  = -1;
			int indexRightRTMiss = -1;
			
			RoutingTableInfo leftRT = treeNode.getLeftRoutingTable();
			for (int i = 0; i < leftRT.getTableSize(); i++) 
			{
				if (leftRT.getRoutingTableNode(i) == null)
					indexLeftRTMiss = i;
			}

			RoutingTableInfo rightRT = treeNode.getRightRoutingTable();
			for (int i = 0; i < rightRT.getTableSize(); i++) 
			{
				if (rightRT.getRoutingTableNode(i) == null)
					indexRightRTMiss = i;
			}


			if ((indexLeftRTMiss != -1) || (indexRightRTMiss != -1)) 
			{
				System.out.println("***Routing table isnt`n full, forward request to its parent");
				//routing table isn't full, forward request to its parent			
				body.setPhysicalSender(serverpeer.getPhysicalInfo());
				body.setLogicalSender(treeNode.getLogicalInfo());
				body.setLogicalDestination(treeNode.getParentNode().getLogicalInfo());

				thead.setMsgType(MsgType.SP_JOIN.getValue());
				result = new Message(thead, body);			
				serverpeer.sendMessage(treeNode.getParentNode().getPhysicalInfo(), result);
			}
			else
			{
				System.out.println("***Routing table is full, can insert new node or forward request to its adjacent nodes");
				//routing table is full, can insert new node or forward request to its adjacent nodes
				if ((treeNode.getLeftChild() == null) || (treeNode.getRightChild() == null)){
					System.out.println("***Have at least one child, insert new node as other child");
					//have at least one child, insert new node as other child
					ChildNodeInfo childNode = null;
					LogicalInfo logicalNewNode;
					int childLevel;
					int childNumber;
					int numOfExpectedRTReply;

					childLevel = treeNode.getLogicalInfo().getLevel() + 1;

					if (treeNode.getLeftChild() == null)
					{
						childNumber = treeNode.getLogicalInfo().getNumber() * 2 - 1;
						logicalNewNode = new LogicalInfo(childLevel, childNumber);

						childNode = new ChildNodeInfo(body.getNewNode(), logicalNewNode);
						treeNode.setLeftChild(childNode);					

						numOfExpectedRTReply = 
							PeerMath.getNumberOfExpectedRTReply(true,
									treeNode.getRightChild() != null,
									treeNode.getLeftRoutingTable(),	
									treeNode.getRightRoutingTable());					
					}
					else
					{
						childNumber = treeNode.getLogicalInfo().getNumber() * 2;
						logicalNewNode = new LogicalInfo(childLevel, childNumber);

						childNode = new ChildNodeInfo(body.getNewNode(), logicalNewNode);
						treeNode.setRightChild(childNode);

						numOfExpectedRTReply = 
							PeerMath.getNumberOfExpectedRTReply(false, 
									treeNode.getLeftChild() != null,
									treeNode.getLeftRoutingTable(),	
									treeNode.getRightRoutingTable());					
					}

					//split data
					ContentInfo content;
					if ((childNumber % 2) == 0)
					{
						content = SPGeneralAction.splitData(treeNode, true, serverpeer.getOrder());

						//update routing table for new node
						RoutingItemInfo newNodeInfo = 
							SPGeneralAction.doAccept(serverpeer, treeNode, 
									body.getNewNode(), new LogicalInfo(childLevel, childNumber),
									content, numOfExpectedRTReply, false, false);
						SPGeneralAction.updateRoutingTable(serverpeer, treeNode, newNodeInfo);
					}
					else
					{
						if (treeNode.getLeftAdjacentNode() == null)
						{
							content = SPGeneralAction.splitData(treeNode, false, serverpeer.getOrder());

							//update routing table for new node
							RoutingItemInfo newNodeInfo = 
								SPGeneralAction.doAccept(serverpeer, treeNode, 
										body.getNewNode(), new LogicalInfo(childLevel, childNumber),
										content, numOfExpectedRTReply, false, false);
							SPGeneralAction.updateRoutingTable(serverpeer, treeNode, newNodeInfo);
						}
						else
						{
							//the new node will share workload with the node's left adjacent
							//ProcessGeneral.updateRangeValues(peerNode, treeNode);
							RoutingItemInfo parentNodeInfo
								= new RoutingItemInfo(serverpeer.getPhysicalInfo(),
										treeNode.getLogicalInfo(),
										treeNode.getLeftChild(),
										treeNode.getRightChild(),
										treeNode.getContent().getMinValue(),
										treeNode.getContent().getMaxValue());
							
							tbody = new SPJoinSplitDataBody(serverpeer.getPhysicalInfo(),
									treeNode.getLogicalInfo(), body.getNewNode(), 
									logicalNewNode,	numOfExpectedRTReply, 
									treeNode.getLeftRoutingTable(),
									treeNode.getRightRoutingTable(), 
									parentNodeInfo, treeNode.getRightChild(),
									treeNode.getLeftAdjacentNode().getLogicalInfo());

							thead.setMsgType(MsgType.SP_JOIN_SPLIT_DATA.getValue());
							result = new Message(thead, tbody);
							serverpeer.sendMessage(treeNode.getLeftAdjacentNode().getPhysicalInfo(), result);
							treeNode.setLeftAdjacentNode(new AdjacentNodeInfo(body.getNewNode(), logicalNewNode));
						}
					}
				}
				else 
				{
					System.out.println("***Have full child, select an adjacent node to forward request");
					//have full children, select an adjacent node to forward request
					//or forward request to a node in its routing table

					RoutingItemInfo temptInfo = null;
					boolean isForwardedRequest = false;
					body.setPhysicalSender(serverpeer.getPhysicalInfo());
					body.setLogicalSender(treeNode.getLogicalInfo());

					int i = leftRT.getTableSize() - 1;
					while ((!isForwardedRequest) && (i >= 0)) 
					{
						if (leftRT.getRoutingTableNode(i) != null) 
						{
							temptInfo = leftRT.getRoutingTableNode(i);
							if ((temptInfo.getLeftChild() == null) || (temptInfo.getRightChild() == null)) 
							{
								isForwardedRequest = true;
								body.setLogicalDestination(temptInfo.getLogicalInfo());

								thead.setMsgType(MsgType.SP_JOIN.getValue());
								result = new Message(thead, body);
								serverpeer.sendMessage(temptInfo.getPhysicalInfo(), result);
							}
						}
						i--;
					}
					
					i = rightRT.getTableSize() - 1;
					while ((!isForwardedRequest) && (i >= 0)) 
					{
						if (rightRT.getRoutingTableNode(i) != null) 
						{
							temptInfo = rightRT.getRoutingTableNode(i);
							if ((temptInfo.getLeftChild() == null) || (temptInfo.getRightChild() == null)) 
							{
								isForwardedRequest = true;
								body.setLogicalDestination(temptInfo.getLogicalInfo());

								thead.setMsgType(MsgType.SP_JOIN.getValue());
								result = new Message(thead, body);
								serverpeer.sendMessage(temptInfo.getPhysicalInfo(), result);
							}
						}
						i--;
					}

					if (!isForwardedRequest) 
					{
						System.out.println("***Forward request to one of its adjacent nodes");
						//forward request to one of its adjacent nodes
						if (treeNode.getLeftAdjacentNode().getLogicalInfo().getLevel() <= treeNode.getRightAdjacentNode().getLogicalInfo().getLevel())
						{
							body.setLogicalDestination(treeNode.getLeftAdjacentNode().getLogicalInfo());

							thead.setMsgType(MsgType.SP_JOIN.getValue());
							result = new Message(thead, body);
							serverpeer.sendMessage(treeNode.getLeftAdjacentNode().getPhysicalInfo(), result);
						}
						else
						{
							body.setLogicalDestination(treeNode.getRightAdjacentNode().getLogicalInfo());

							thead.setMsgType(MsgType.SP_JOIN.getValue());
							result = new Message(thead, body);
							serverpeer.sendMessage(treeNode.getRightAdjacentNode().getPhysicalInfo(), result);
						}
					}
				}
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new EventHandleException("Super peer joins network failure", e);
		}
	}

	public boolean isConsumed(Message msg) throws EventHandleException 
	{
		if (msg.getHead().getMsgType() == MsgType.SP_JOIN.getValue()) {
			// TODO: JUST FOR TEST
			System.out.println("***SP_JOIN Consumed");
			return true;
		}
		return false;
	}

}