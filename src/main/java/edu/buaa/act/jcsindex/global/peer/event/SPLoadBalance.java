/*
 * @(#) SPLoadBalance.java 1.0 2006-2-22
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.peer.event;

import edu.buaa.act.jcsindex.global.peer.ServerPeer;
import edu.buaa.act.jcsindex.global.peer.info.BoundaryValue;
import edu.buaa.act.jcsindex.global.peer.info.IndexValue;
import edu.buaa.act.jcsindex.global.peer.info.TreeNode;
import edu.buaa.act.jcsindex.global.peer.management.EventHandleException;
import edu.buaa.act.jcsindex.global.protocol.Head;
import edu.buaa.act.jcsindex.global.protocol.Message;
import edu.buaa.act.jcsindex.global.protocol.MsgType;
import edu.buaa.act.jcsindex.global.protocol.body.Body;
import edu.buaa.act.jcsindex.global.protocol.body.SPLBSplitDataBody;

import java.util.Vector;

/**
 * Used for doing local load balancing between adjacent nodes
 * 
 * @author Vu Quang Hieu
 * @author (Modified by) Xu Linhao
 * @version 1.0 2006-2-22
 */

public class SPLoadBalance
{
	
	// private members
	private ServerPeer serverpeer;
	private TreeNode treeNode;
	
	/**
	 * Constructor
	 * 
	 * @param serverpeer the handler of <code>ServerPeer</code>
	 * @param treeNode
	 */
	public SPLoadBalance(ServerPeer serverpeer, TreeNode treeNode)
	{
		this.serverpeer = serverpeer;
		this.treeNode  = treeNode;
	}
	
	/**
	 * Balance load between adjacent nodes
	 *
	 */
	public void balanceLoad() throws EventHandleException
	{
		BoundaryValue oldMinValue = treeNode.getContent().getMinValue();
		BoundaryValue oldMaxValue = treeNode.getContent().getMaxValue();
		
		int totalElement = treeNode.getContent().getData().size();
		
		int newNumOfLNElement = -1;
		int newNumOfRNElement = -1;
		int newNumOfElement = -1;
		
		int remain, i;
		
		try
		{
			if (treeNode.getLNOrder() > treeNode.getContent().getOrder())
			{
				treeNode.setLNElement(-1);
			}
			else if ((treeNode.getLNElement() * 2) > treeNode.getContent().getData().size())
			{
				treeNode.setLNElement(-1);
			}
	
			if (treeNode.getRNOrder() > treeNode.getContent().getOrder())
			{
				treeNode.setRNElement(-1);
			}
			else if ((treeNode.getRNElement() * 2) > treeNode.getContent().getData().size())
			{
				treeNode.setRNElement(-1);
			}
	
			if ((treeNode.getLNElement() == -1) && (treeNode.getRNElement() == -1))
			{
				treeNode.getContent().setOrder(treeNode.getContent().getOrder() * 2);
			}
			else 
			{
				if (treeNode.getLNElement() != -1) 
				{
					//can split to the right node
					totalElement += treeNode.getLNElement();
				}
				if (treeNode.getRNElement() != -1) 
				{
					//can split to the left node
					totalElement += treeNode.getRNElement();
				}
	
				if ((treeNode.getLNElement() != -1) && (treeNode.getRNElement() != -1)) 
				{
					newNumOfElement   = totalElement / 3;
					newNumOfLNElement = newNumOfElement;
					newNumOfRNElement = newNumOfElement;
					remain = totalElement % 3;
					if (remain == 1) 
					{
						newNumOfElement--;
						newNumOfLNElement++;
						newNumOfRNElement++;
					}
					else if (remain == 2) 
					{
						newNumOfLNElement++;
						newNumOfRNElement++;
					}
				}
				else 
				{
					if (treeNode.getLNElement() != -1) 
					{
						newNumOfElement   = totalElement / 2;
						newNumOfLNElement = totalElement - newNumOfElement;
					}
					if (treeNode.getRNElement() != -1) 
					{
						newNumOfElement   = totalElement / 2;
						newNumOfRNElement = totalElement - newNumOfElement;
					}
				}
	
				if ((newNumOfLNElement != -1) && (newNumOfLNElement > treeNode.getLNElement())) 
				{
					//split data to its left adjacent node
					int numOfData = newNumOfLNElement - treeNode.getLNElement();
					Vector<IndexValue> transferData = new Vector<IndexValue>();
					for (i = numOfData - 1; i >= 0; i--) 
					{
						transferData.add(treeNode.getContent().getData().get(i));
						treeNode.getContent().getData().remove(i);
					}
	
					BoundaryValue newNodeMinValue;
					IndexValue minValue = (IndexValue) treeNode.getContent().getData().get(0);
					if (minValue.getType() == IndexValue.NUMERIC_TYPE)
						newNodeMinValue = new BoundaryValue(oldMinValue.getStringValue(), Long.parseLong(minValue.getString()));
					else
						newNodeMinValue = new BoundaryValue(minValue.getString(), oldMinValue.getLongValue());
					
					treeNode.getContent().setMinValue(newNodeMinValue);
					
					Body body = new SPLBSplitDataBody(serverpeer.getPhysicalInfo(),
							treeNode.getLogicalInfo(), true, oldMinValue, 
							treeNode.getContent().getMinValue(), transferData, 
							treeNode.getLeftAdjacentNode().getLogicalInfo());
	
					Head head = new Head();
					head.setMsgType(MsgType.SP_LB_SPLIT_DATA.getValue());
					Message message = new Message(head, body);
					serverpeer.sendMessage(treeNode.getLeftAdjacentNode().getPhysicalInfo(), message);
				}
	
				if ((newNumOfRNElement != -1) && (newNumOfRNElement > treeNode.getRNElement())) 
				{
					//split data to its right adjacent node
					int numOfData = (newNumOfRNElement - treeNode.getRNElement());
					Vector<IndexValue> transferData = new Vector<IndexValue>();
					int dataSize = treeNode.getContent().getData().size();
					for (i = dataSize - 1; i > dataSize - 1 - numOfData; i--) 
					{
						transferData.add(0, treeNode.getContent().getData().get(i));
						treeNode.getContent().getData().remove(i);
					}
	
					BoundaryValue newNodeMaxValue;
					IndexValue maxValue = (IndexValue)transferData.get(0);
					if (maxValue.getType() == IndexValue.NUMERIC_TYPE)
						newNodeMaxValue = new BoundaryValue(oldMaxValue.getString(), Long.parseLong(maxValue.getString()));
					else
						newNodeMaxValue = new BoundaryValue(maxValue.getString(), oldMaxValue.getLongValue());
					
					treeNode.getContent().setMaxValue(newNodeMaxValue);
					
					Body body = new SPLBSplitDataBody(serverpeer.getPhysicalInfo(), 
							treeNode.getLogicalInfo(), false, treeNode.getContent().getMaxValue(), 
							oldMaxValue, transferData, treeNode.getRightAdjacentNode().getLogicalInfo());
	
					Head head = new Head();
					head.setMsgType(MsgType.SP_LB_SPLIT_DATA.getValue());
					Message message = new Message(head, body);
					serverpeer.sendMessage(treeNode.getRightAdjacentNode().getPhysicalInfo(), message);
				}
	
				if ((treeNode.getContent().getMinValue() != oldMinValue) 
					|| (treeNode.getContent().getMaxValue() != oldMaxValue)) 
				{
					SPGeneralAction.updateRangeValues(serverpeer, treeNode);
				}
			}
	
			treeNode.processLoadBalance(false);
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new EventHandleException(e);
		}
	}
}