/*
 * @(#) SPNotifyFailureListener.java 1.0 2006-12-09
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
import edu.buaa.act.jcsindex.global.protocol.body.SPNotifyFailureBody;
import edu.buaa.act.jcsindex.global.utils.PeerMath;

import java.io.ObjectOutputStream;

/**
 * Implement a listener for processing SP_NOTIFY_FAILURE message.
 * 
 * @author Vu Quang Hieu
 * @version 1.0 2006-12-09
 */

public class SPNotifyFailureListener extends ActionAdapter
{

	public SPNotifyFailureListener(AbstractInstance instance)
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
			SPNotifyFailureBody body = (SPNotifyFailureBody) msg.getBody();
			
			/* get the correspondent tree node*/
			TreeNode treeNode = serverpeer.getTreeNode(body.getLogicalDestination());
			if (treeNode == null)
			{
				System.out.println("SP_NOTIFY_FAILURE: Tree node is null, do not process the message");
				return;				
			}
			
			if (isFailedNodeChild(treeNode.getLogicalInfo(), body.getFailedNode()))
			{
				//double check again, if it is failed, call SPGeneralAction.startRecoveryProcess
			}
			else
			{
				//find a link leading to the parent of the failed node
				if (treeNode.getLogicalInfo().getLevel() <= body.getFailedNode().getLevel())
				{
					if (treeNode.getParentNode() != null)
					{
						body.setPhysicalSender(serverpeer.getPhysicalInfo());
						body.setLogicalSender(treeNode.getLogicalInfo());
						body.setLogicalDestination(treeNode.getParentNode().getLogicalInfo());
						
						head.setMsgType(MsgType.SP_NOTIFY_FAILURE.getValue());
						result = new Message(head, body);
						serverpeer.sendMessage(treeNode.getParentNode().getPhysicalInfo(), result);
					}
				}
				else
				{
					if (PeerMath.compareNodePosition(treeNode.getLogicalInfo().getLevel(), 
							treeNode.getLogicalInfo().getNumber(),
							body.getFailedNode().getLevel(), 
							body.getFailedNode().getNumber()))
					{
						//this node is on the left of the failed node
						for (int i = treeNode.getRightRoutingTable().getTableSize() - 1; i >= 0; i--)
						{
							RoutingItemInfo nodeInfo = 
								treeNode.getRightRoutingTable().getRoutingTableNode(i);							
							if (nodeInfo != null)
							{
								if (isFailedNodeChild(nodeInfo.getLogicalInfo(), body.getFailedNode()))
								{
									body.setPhysicalSender(serverpeer.getPhysicalInfo());
									body.setLogicalSender(treeNode.getLogicalInfo());
									body.setLogicalDestination(nodeInfo.getLogicalInfo());
									
									head.setMsgType(MsgType.SP_NOTIFY_FAILURE.getValue());
									result = new Message(head, body);
									serverpeer.sendMessage(nodeInfo.getPhysicalInfo(), result);
									break;
								}
							}
						}
					}	
					else
					{
						//this node is on the right of the failed node
						for (int i = treeNode.getLeftRoutingTable().getTableSize() - 1; i >= 0; i--)
						{
							RoutingItemInfo nodeInfo =
								treeNode.getLeftRoutingTable().getRoutingTableNode(i);
							if (nodeInfo != null)
							{
								if (isFailedNodeChild(nodeInfo.getLogicalInfo(), body.getFailedNode()))
								{
									body.setPhysicalSender(serverpeer.getPhysicalInfo());
									body.setLogicalSender(treeNode.getLogicalInfo());
									body.setLogicalDestination(nodeInfo.getLogicalInfo());
									
									head.setMsgType(MsgType.SP_NOTIFY_FAILURE.getValue());
									result = new Message(head, body);
									serverpeer.sendMessage(nodeInfo.getPhysicalInfo(), result);
									break;
								}
							}
						}
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

	/**
	 * Return <code>true</code> if a child node is failed.
	 * 
	 * @param checkNode
	 * @param failedNode
	 * @return if a child node is failed, return <code>true</code>
	 */
	private boolean isFailedNodeChild(LogicalInfo checkNode, LogicalInfo failedNode)
	{
		if (checkNode.getLevel() == (failedNode.getLevel() - 1))
		{
			if (((checkNode.getNumber() * 2) == failedNode.getNumber()) 
				|| ((checkNode.getNumber() * 2 - 1) == failedNode.getNumber()))
			{
				return true;
			}
		}
		return false;
	}
	
	public boolean isConsumed(Message msg) throws EventHandleException 
	{
		if (msg.getHead().getMsgType() == MsgType.SP_NOTIFY_FAILURE.getValue())
			return true;
		return false;
	}

}
