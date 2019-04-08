/*
 * @(#) SPLeaveListener.java 1.0 2006-2-22
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.peer.event;

import edu.buaa.act.jcsindex.global.AbstractInstance;
import edu.buaa.act.jcsindex.global.peer.ServerPeer;
import edu.buaa.act.jcsindex.global.peer.info.AdjacentNodeInfo;
import edu.buaa.act.jcsindex.global.peer.info.BoundaryValue;
import edu.buaa.act.jcsindex.global.peer.info.IndexValue;
import edu.buaa.act.jcsindex.global.peer.info.TreeNode;
import edu.buaa.act.jcsindex.global.peer.management.EventHandleException;
import edu.buaa.act.jcsindex.global.protocol.Message;
import edu.buaa.act.jcsindex.global.protocol.MsgType;
import edu.buaa.act.jcsindex.global.protocol.body.SPLeaveBody;
import edu.buaa.act.jcsindex.global.utils.PeerMath;

import java.io.ObjectOutputStream;
import java.util.Vector;

/**
 * Implement a listener for processing SP_LEAVE message.
 * 
 * @author Vu Quang Hieu
 * @author (Modified by) Xu Linhao
 * @version 1.0 2006-2-22
 */

public class SPLeaveListener extends ActionAdapter
{

	public SPLeaveListener(AbstractInstance instance)
	{
		super(instance);
	}
 
	public void actionPerformed(ObjectOutputStream oos, Message msg) throws EventHandleException
	{
		super.actionPerformed(oos, msg);
		
		try
		{
			/* get the handler of the ServerPeer */
			ServerPeer serverpeer = (ServerPeer) instance.peer();
			
			/* get the message body */
			SPLeaveBody body = (SPLeaveBody) msg.getBody();
			
			TreeNode treeNode = serverpeer.getTreeNode(body.getLogicalDestination());
			if (treeNode == null)
			{
				System.out.println("SP_LEAVE: Tree node is null, do not process the message");
				return;				
			}
			
			AdjacentNodeInfo adjacentInfo = body.getAdjacentInfo();
			Vector<IndexValue> childData  = body.getContent().getData();
			boolean direction = body.getDirection();
			Vector transferWorkList = body.getWorkList();

	    	/* get data, update min, max values and child, adjacent links */
			/*
	    	for (int i = 0; i < childData.size(); i++)
	    		treeNode.getContent().insertData((IndexValue)childData.get(i), 0);
			 */
			SPGeneralAction.saveData(childData);
			
	    	for (int i = 0; i < transferWorkList.size(); i++)
	    		treeNode.putWork((String)transferWorkList.get(i));

	    	/* update range of values */
	    	if (treeNode.getContent().getMaxValue().compareTo(body.getContent().getMinValue())==0)
	    	{
	    		treeNode.getContent().setMaxValue(body.getContent().getMaxValue());
	    	}
	    	else if (treeNode.getContent().getMinValue().compareTo(body.getContent().getMaxValue())==0)
	    	{
	    		treeNode.getContent().setMinValue(body.getContent().getMinValue());
	    	}
	    	else
	    	{
	    		if (body.getContent().getMinValue().compareTo(treeNode.getContent().getMinValue()) < 0)
	    		{
	    			treeNode.getContent().setMinValue(body.getContent().getMinValue());
	    		}
	    		if (body.getContent().getMaxValue().compareTo(treeNode.getContent().getMaxValue()) > 0)
	    		{
	    			treeNode.getContent().setMaxValue(body.getContent().getMaxValue());
	    		}
	    	}

	    	/* update adjacent and child */
	    	if (!direction)
	    	{
	    		treeNode.setLeftAdjacentNode(adjacentInfo);
	    		treeNode.setLeftChild(null);
	    		if (treeNode.getLogicalInfo().getNumber() == 1)	    			
	    			treeNode.getContent().setMinValue(new BoundaryValue(IndexValue.MIN_KEY.getString(), Long.MIN_VALUE));
	    	}
	    	else
	    	{
	    		treeNode.setRightAdjacentNode(adjacentInfo);
	    		treeNode.setRightChild(null);
	    		if (treeNode.getLogicalInfo().getNumber() == PeerMath.pow(2, treeNode.getLogicalInfo().getLevel()))
	    			treeNode.getContent().setMaxValue(new BoundaryValue(IndexValue.MAX_KEY.getString(), Long.MAX_VALUE));
	    	}

	    	/* notify its neighbor nodes */
	    	SPGeneralAction.updateRangeValues(serverpeer, treeNode);
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new EventHandleException("Super peer leaves network failure", e);
		}
	}

	public boolean isConsumed(Message msg) throws EventHandleException 
	{
		if (msg.getHead().getMsgType() == MsgType.SP_LEAVE.getValue())
			return true;
		return false;
	}

}