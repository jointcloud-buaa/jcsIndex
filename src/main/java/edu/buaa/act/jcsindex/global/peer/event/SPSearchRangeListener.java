/*
 * @(#) SPSearchRangeListener.java 1.0 2006-2-22
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
import edu.buaa.act.jcsindex.global.protocol.body.SPSearchRangeBody;
import edu.buaa.act.jcsindex.global.protocol.body.SPSearchRangeResultBody;

import java.io.ObjectOutputStream;
import java.util.Vector;

/**
 * Implement a listener for processing SP_SEARCH_RANGE message.
 * 
 * @author Vu Quang Hieu
 * @author (Modified by) Xu Linhao
 * @version 1.0 2006-2-22
 */

public class SPSearchRangeListener extends ActionAdapter
{

	public SPSearchRangeListener(AbstractInstance instance)
	{
		super(instance);
	}

	public void actionPerformed(ObjectOutputStream oos, Message msg) throws EventHandleException
	{
		super.actionPerformed(oos, msg);
		
    	Message result = null;
    	Head thead = new Head();
		SPSearchRangeResultBody resultBody = null;
    	
		try
		{
			/* get the handler of the ServerPeer */
			ServerPeer serverpeer = (ServerPeer) instance.peer();
			
			/* get the message body */
			SPSearchRangeBody body = (SPSearchRangeBody) msg.getBody();
			
			TreeNode treeNode = serverpeer.getTreeNode(body.getLogicalDestination());
			if (treeNode == null)
			{
				System.out.println("SP_SEARCH_RANGE: Tree node is null, do not process the message");
				return;				
			}
			
			Vector<IndexValue> storedData = treeNode.getContent().getData();
			Vector<IndexValue> searchResult = new Vector<IndexValue>();
			
			IndexValue searchedMinValue = body.getMinValue();
			IndexValue searchedMaxValue = body.getMaxValue();
			
			boolean canLeft = body.getCanLeft();
			boolean canRight = body.getCanRight();

			BoundaryValue minValue = treeNode.getContent().getMinValue();
			BoundaryValue maxValue = treeNode.getContent().getMaxValue();

			//check inside it
			for (int i = 0; i <= storedData.size() - 1; i++) 
			{
				IndexValue temp = storedData.get(i);
				if ((searchedMinValue.compareTo(temp) <= 0) && (temp.compareTo(searchedMaxValue) <= 0))
				{
					searchResult.add(storedData.get(i));
				}
			}
			if (searchResult.size() != 0) 
			{
				resultBody = new SPSearchRangeResultBody(serverpeer.getPhysicalInfo(),
									treeNode.getLogicalInfo(), searchResult,
									body.getLogicalRequester());

				thead.setMsgType(MsgType.SP_SEARCH_RANGE_RESULT.getValue());
				result = new Message(thead, resultBody);
				serverpeer.sendMessage(body.getPhysicalRequester(), result);
			}

			//check if can forward request to the left hand side
			if ((minValue.compareTo(searchedMinValue) > 0) && (canLeft)) 
			{

				body.setPhysicalSender(serverpeer.getPhysicalInfo());
				body.setLogicalSender(treeNode.getLogicalInfo());

				if (searchResult.size() != 0) 
				{
					//forward request to left adjacent node
					if (treeNode.getLeftAdjacentNode() != null)
					{
						body.setCanRight(false);
						body.setLogicalDestination(treeNode.getLeftAdjacentNode().getLogicalInfo());

		  				thead.setMsgType(MsgType.SP_SEARCH_RANGE.getValue());
		  				result = new Message(thead, body);
						serverpeer.sendMessage(treeNode.getLeftAdjacentNode().getPhysicalInfo(), result);
					}
				}
				else 
				{
					//check routing table
					int index = treeNode.getLeftRoutingTable().getTableSize() - 1;
					int found = -1;
					while ((index >= 0) && (found == -1))
					{
						RoutingItemInfo nodeInfo = treeNode.getLeftRoutingTable().getRoutingTableNode(index);
						if (treeNode.getLeftRoutingTable().getRoutingTableNode(index) != null)
						{
							if (nodeInfo.getMaxValue().compareTo(searchedMinValue) > 0)
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

		  				thead.setMsgType(MsgType.SP_SEARCH_RANGE.getValue());
		  				result = new Message(thead, body);
						serverpeer.sendMessage(transferInfo.getPhysicalInfo(), result);
					}
					else
					{
						//forward request to left child
						if (treeNode.getLeftChild() != null)
						{
							body.setLogicalDestination(treeNode.getLeftChild().getLogicalInfo());

			  				thead.setMsgType(MsgType.SP_SEARCH_RANGE.getValue());
			  				result = new Message(thead, body);
							serverpeer.sendMessage(treeNode.getLeftChild().getPhysicalInfo(), result);
						}
						else
						{
							//forward request to left adjacent
							if (treeNode.getLeftAdjacentNode() != null)
							{
								body.setLogicalDestination(treeNode.getLeftAdjacentNode().getLogicalInfo());

				  				thead.setMsgType(MsgType.SP_SEARCH_RANGE.getValue());
				  				result = new Message(thead, body);
								serverpeer.sendMessage(treeNode.getLeftAdjacentNode().getPhysicalInfo(), result);
							}
						}
					}	
				}
			}

			if ((maxValue.compareTo(searchedMaxValue) <= 0) && (canRight)) 
			{

				body.setPhysicalSender(serverpeer.getPhysicalInfo());
				body.setLogicalSender(treeNode.getLogicalInfo());

				if (searchResult.size() != 0) 
				{
					//forward request to right adjacent node
					if (treeNode.getRightAdjacentNode() != null) 
					{
						body.setCanLeft(false);
						body.setLogicalDestination(treeNode.getRightAdjacentNode().getLogicalInfo());

		  				thead.setMsgType(MsgType.SP_SEARCH_RANGE.getValue());
		  				result = new Message(thead, body);
						serverpeer.sendMessage(treeNode.getRightAdjacentNode().getPhysicalInfo(), result);
					}
				}
				else 
				{
					//check routing table
					int index = treeNode.getRightRoutingTable().getTableSize() - 1;
					int found = -1;
					while ((index >= 0) && (found == -1))
					{
						if (treeNode.getRightRoutingTable().getRoutingTableNode(index) != null)
						{
							RoutingItemInfo nodeInfo = treeNode.getRightRoutingTable().getRoutingTableNode(index); 
							if (nodeInfo.getMinValue().compareTo(searchedMaxValue) <= 0)
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

		  				thead.setMsgType(MsgType.SP_SEARCH_RANGE.getValue());
		  				result = new Message(thead, body);
						serverpeer.sendMessage(transferInfo.getPhysicalInfo(), result);
					}
					else
					{
						if (treeNode.getRightChild() != null)
						{
							body.setLogicalDestination(treeNode.getRightChild().getLogicalInfo());

			  				thead.setMsgType(MsgType.SP_SEARCH_RANGE.getValue());
			  				result = new Message(thead, body);
							serverpeer.sendMessage(treeNode.getRightChild().getPhysicalInfo(), result);
						}
						else
						{
							if (treeNode.getRightAdjacentNode() != null)
							{
								body.setLogicalDestination(treeNode.getRightAdjacentNode().getLogicalInfo());

				  				thead.setMsgType(MsgType.SP_SEARCH_RANGE.getValue());
				  				result = new Message(thead, body);
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
			throw new EventHandleException("Super peer performs range search failure", e);
		}
	}

	public boolean isConsumed(Message msg) throws EventHandleException 
	{
		if (msg.getHead().getMsgType() == MsgType.SP_SEARCH_RANGE.getValue())
			return true;
		return false;
	}

}