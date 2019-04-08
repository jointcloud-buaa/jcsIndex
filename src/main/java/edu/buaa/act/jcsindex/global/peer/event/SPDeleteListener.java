/*
 * @(#) SPDeleteListener.java 1.0 2006-2-22
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
import edu.buaa.act.jcsindex.global.protocol.body.SPDeleteBody;

import java.io.ObjectOutputStream;

/**
 * Implement a listener for processing SP_DELETE message.
 * 
 * @author Vu Quang Hieu
 * @author (Modified by) Xu Linhao
 * @version 1.0 2006-2-22
 */

public class SPDeleteListener extends ActionAdapter
{

	public SPDeleteListener(AbstractInstance instance)
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
			SPDeleteBody body = (SPDeleteBody) msg.getBody();
						
			TreeNode treeNode = serverpeer.getTreeNode(body.getLogicalDestination());
			if (treeNode == null)
			{
				System.out.println("SP_DELETE: Tree node is null, do not process the message");
				return;				
			}
			
			/* data to be deleted */
			IndexValue data = body.getData();
			BoundaryValue minValue = treeNode.getContent().getMinValue();
			BoundaryValue maxValue = treeNode.getContent().getMaxValue();		

			if ((data.compareTo(minValue) >= 0) && (data.compareTo(maxValue) < 0))
			{
				treeNode.getContent().deleteData(data);
			}
			else
			{
				body.setPhysicalSender(serverpeer.getPhysicalInfo());
				body.setLogicalSender(treeNode.getLogicalInfo());

				/* 
				 * if minimal value is greater than the data to be deleted,
				 * then route the message to the node that hold the data
				 */
				if (minValue.compareTo(data) > 0)
				{
					int index = treeNode.getLeftRoutingTable().getTableSize() - 1;
					int found = -1;
					while ((index >= 0) && (found == -1))
					{
						if (treeNode.getLeftRoutingTable().getRoutingTableNode(index) != null)
						{
							RoutingItemInfo nodeInfo = treeNode.getLeftRoutingTable().getRoutingTableNode(index);
							if (nodeInfo.getMaxValue().compareTo(data) > 0)
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
						
						head.setMsgType(MsgType.SP_DELETE.getValue());
						result = new Message(head, body);
						serverpeer.sendMessage(transferInfo.getPhysicalInfo(), result);
					}
					else
					{
						if (treeNode.getLeftChild() != null)
						{
							body.setLogicalDestination(treeNode.getLeftChild().getLogicalInfo());
							
							head.setMsgType(MsgType.SP_DELETE.getValue());
							result = new Message(head, body);
							serverpeer.sendMessage(treeNode.getLeftChild().getPhysicalInfo(), result);
						}
						else
						{
							if (treeNode.getLeftAdjacentNode() != null)
							{
								body.setLogicalDestination(treeNode.getLeftAdjacentNode().getLogicalInfo());

								head.setMsgType(MsgType.SP_DELETE.getValue());
								result = new Message(head, body);
								serverpeer.sendMessage(treeNode.getLeftAdjacentNode().getPhysicalInfo(), result);
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
						if (treeNode.getRightRoutingTable().getRoutingTableNode(index) != null)
						{
							RoutingItemInfo nodeInfo = treeNode.getRightRoutingTable().getRoutingTableNode(index); 
							if (nodeInfo.getMinValue().compareTo(data) <= 0)
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

						head.setMsgType(MsgType.SP_DELETE.getValue());
						result = new Message(head, body);
						serverpeer.sendMessage(transferInfo.getPhysicalInfo(), result);
					}
					else
					{
						if (treeNode.getRightChild() != null)
						{
							body.setLogicalDestination(treeNode.getRightChild().getLogicalInfo());

							head.setMsgType(MsgType.SP_DELETE.getValue());
							result = new Message(head, body);
							serverpeer.sendMessage(treeNode.getRightChild().getPhysicalInfo(), result);
						}
						else
						{
							if (treeNode.getRightAdjacentNode() != null)
							{
								body.setLogicalDestination(treeNode.getRightAdjacentNode().getLogicalInfo());

								head.setMsgType(MsgType.SP_DELETE.getValue());
								result = new Message(head, body);
								serverpeer.sendMessage(treeNode.getRightAdjacentNode().getPhysicalInfo(), result);
							}
						}
					}
				}
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new EventHandleException("Super peer deletes index failure", e);
		}
	}

	public boolean isConsumed(Message msg) throws EventHandleException 
	{
		if (msg.getHead().getMsgType() == MsgType.SP_DELETE.getValue())
			return true;
		return false;
	}

}