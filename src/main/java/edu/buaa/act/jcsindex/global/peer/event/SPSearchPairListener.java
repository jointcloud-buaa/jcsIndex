/*
 * @(#) SPSearchPairListener.java 1.0 2006-3-24
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
import edu.buaa.act.jcsindex.global.protocol.body.SPSearchExactResultBody;
import edu.buaa.act.jcsindex.global.protocol.body.SPSearchPairBody;

import java.io.ObjectOutputStream;
import java.util.Vector;

/**
 * Implement a listener for processing SP_SEARCH_PAIR message.
 * 
 * @author Vu Quang Hieu
 * @version 1.0 2006-3-24
 */

public class SPSearchPairListener extends ActionAdapter
{

	public SPSearchPairListener(AbstractInstance instance)
	{
		super(instance);
	}

	public void actionPerformed(ObjectOutputStream oos, Message msg) throws EventHandleException
	{
		super.actionPerformed(oos, msg);
		
    	Message result = null;
    	Head thead = new Head();
		SPSearchExactResultBody resultBody = null;
    	
		try
		{
			/* get the handler of the ServerPeer */
			ServerPeer serverpeer = (ServerPeer) instance.peer();
			
			/* get the message body */
			SPSearchPairBody body = (SPSearchPairBody) msg.getBody();
			TreeNode treeNode = serverpeer.getTreeNode(body.getLogicalDestination());
			if (treeNode == null)
			{
				System.out.println("SP_SEARCH_PAIR: Tree node is null, do not process the message");
				return;				
			}
			
			Vector storedData = treeNode.getContent().getData();
			
			IndexPair searchData = body.getData();
			IndexValue searchedData = new IndexValue(IndexValue.STRING_TYPE, searchData.getKeyword());
			BoundaryValue minValue = treeNode.getContent().getMinValue();
			BoundaryValue maxValue = treeNode.getContent().getMaxValue();
			IndexValue returnedInfo = null;

	  	  	if ((searchedData.compareTo(minValue) >= 0) && (searchedData.compareTo(maxValue) < 0))
	  	  	{
	  	  		treeNode.incNumOfQuery(1);

	  	  		//data either be found or not be found
	  	  		boolean blnFound = false;
	  	  		int i = 0;
	  	  		while ((i < storedData.size()) && (!blnFound)) 
	  	  		{
	  	  			if (((IndexValue)storedData.get(i)).compareTo(searchedData)==0)
	  	  			{
	  	  				//TODO: add additional comparison with fieldID
	  	  				blnFound = true;
	  	  				returnedInfo = (IndexValue)storedData.get(i);
	  	  			}
	  	  			i++;
	  	  		}

	  	  		//TODO: return the result to the request node, need to be modified
	  	  		if (blnFound) 
	  	  		{
	  	  			resultBody = new SPSearchExactResultBody(serverpeer.getPhysicalInfo(),
	  	  							treeNode.getLogicalInfo(), true, returnedInfo,
	  	  							body.getLogicalRequester());
	  	  		}
	  	  		else 
	  	  		{
	  	  			resultBody =	new SPSearchExactResultBody(serverpeer.getPhysicalInfo(),
	  	  							treeNode.getLogicalInfo(), false, returnedInfo,
	  	  							body.getLogicalRequester());
	  	  		}

	  	  		thead.setMsgType(MsgType.SP_SEARCH_EXACT_RESULT.getValue());
	  	  		result = new Message(thead, resultBody);
	  	  		serverpeer.sendMessage(body.getPhysicalRequester(), result);
	    	}
	  	  	else
	  	  	{
	  	  		body.setPhysicalSender(serverpeer.getPhysicalInfo());
	  	  		body.setLogicalSender(treeNode.getLogicalInfo());

	  	  		if (minValue.compareTo(searchedData) > 0)
	  	  		{
	  	  			int index = treeNode.getLeftRoutingTable().getTableSize() - 1;
	  	  			int found = -1;
	  	  			while ((index >= 0) && (found == -1))
	  	  			{
	  	  				if (treeNode.getLeftRoutingTable().getRoutingTableNode(index) != null)
	  	  				{
	  	  					RoutingItemInfo nodeInfo = treeNode.getLeftRoutingTable().getRoutingTableNode(index);
	  	  					if (nodeInfo.getMaxValue().compareTo(searchedData) > 0)
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

	  	  				thead.setMsgType(MsgType.SP_SEARCH_PAIR.getValue());
	  	  				result = new Message(thead, body);
	  	  				serverpeer.sendMessage(transferInfo.getPhysicalInfo(), result);
	  	  			}
	  	  			else
	  	  			{
	  	  				if (treeNode.getLeftChild() != null)
	  	  				{
	  	  					body.setLogicalDestination(treeNode.getLeftChild().getLogicalInfo());

	  	  					thead.setMsgType(MsgType.SP_SEARCH_PAIR.getValue());
	  	  					result = new Message(thead, body);
	  	  					serverpeer.sendMessage(treeNode.getLeftChild().getPhysicalInfo(), result);
	  	  				}
	  	  				else
	  	  				{
	  	  					if (treeNode.getLeftAdjacentNode() != null)
	  	  					{
	  	  						body.setLogicalDestination(treeNode.getLeftAdjacentNode().getLogicalInfo());

	  	  						thead.setMsgType(MsgType.SP_SEARCH_PAIR.getValue());
	  	  						result = new Message(thead, body);
	  	  						serverpeer.sendMessage(treeNode.getLeftAdjacentNode().getPhysicalInfo(), result);
	  	  					}
	  	  					else 
	  	  					{
	  	  						resultBody = new SPSearchExactResultBody(serverpeer.getPhysicalInfo(),
	  	  										treeNode.getLogicalInfo(), false, null,
	  	  										body.getLogicalRequester());

	  	  						thead.setMsgType(MsgType.SP_SEARCH_EXACT_RESULT.getValue());
	  	  						result = new Message(thead, resultBody);
	  	  						serverpeer.sendMessage(body.getPhysicalRequester(), result);
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
	  	  					if (nodeInfo.getMinValue().compareTo(searchedData) <= 0)
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

		  				thead.setMsgType(MsgType.SP_SEARCH_PAIR.getValue());
		  				result = new Message(thead, body);
	  	  				serverpeer.sendMessage(transferInfo.getPhysicalInfo(), result);
	  	  			}
	  	  			else
	  	  			{
	  	  				if (treeNode.getRightChild() != null)
	  	  				{
	  	  					body.setLogicalDestination(treeNode.getRightChild().getLogicalInfo());

	  	  					thead.setMsgType(MsgType.SP_SEARCH_PAIR.getValue());
	  	  					result = new Message(thead, body);
	  	  					serverpeer.sendMessage(treeNode.getRightChild().getPhysicalInfo(), result);
	  	  				}
	  	  				else
	  	  				{
	  	  					if (treeNode.getRightAdjacentNode() != null)
	  	  					{
	  	  						body.setLogicalDestination(treeNode.getRightAdjacentNode().getLogicalInfo());

	  	  						thead.setMsgType(MsgType.SP_SEARCH_PAIR.getValue());
	  	  						result = new Message(thead, body);
	  	  						serverpeer.sendMessage(treeNode.getRightAdjacentNode().getPhysicalInfo(), result);
	  	  					}
	  	  					else
	  	  					{
	  	  						resultBody = new SPSearchExactResultBody(serverpeer.getPhysicalInfo(),
	  	  											treeNode.getLogicalInfo(), false, null,
	  	  											body.getLogicalRequester());

	  	  						thead.setMsgType(MsgType.SP_SEARCH_EXACT_RESULT.getValue());
	  	  	  					result = new Message(thead, resultBody);
	  	  						serverpeer.sendMessage(body.getPhysicalRequester(), result);
	  	  					}
	  	  				}
	  	  			}
	  	  		}
	  	  	}
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new EventHandleException("SP_SEARCH_PAIR operation failure", e);
		}
	}

	public boolean isConsumed(Message msg) throws EventHandleException 
	{
		if (msg.getHead().getMsgType() == MsgType.SP_SEARCH_PAIR.getValue())
			return true;
		return false;
	}

}