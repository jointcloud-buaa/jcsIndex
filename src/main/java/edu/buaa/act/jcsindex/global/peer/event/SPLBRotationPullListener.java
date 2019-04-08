/*
 * @(#) SPLBRotationPullListener.java 1.0 2006-10-10
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.peer.event;

import edu.buaa.act.jcsindex.global.AbstractInstance;
import edu.buaa.act.jcsindex.global.peer.ServerPeer;
import edu.buaa.act.jcsindex.global.peer.info.PhysicalInfo;
import edu.buaa.act.jcsindex.global.peer.info.TreeNode;
import edu.buaa.act.jcsindex.global.peer.management.EventHandleException;
import edu.buaa.act.jcsindex.global.protocol.Message;
import edu.buaa.act.jcsindex.global.protocol.MsgType;
import edu.buaa.act.jcsindex.global.protocol.body.SPLBRotationPullBody;

import java.io.ObjectOutputStream;

/**
 * Implement a listener for processing SP_LB_ROTATION_PULL message.
 * 
 * @author Vu Quang Hieu
 * @version 1.0 2006-10-10
 */

public class SPLBRotationPullListener extends ActionAdapter
{

	public SPLBRotationPullListener(AbstractInstance instance)
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
			SPLBRotationPullBody body = (SPLBRotationPullBody) msg.getBody();
			
			TreeNode treeNode = body.getTreeNode();
		    PhysicalInfo physicalSender = body.getPhysicalSender();
		    boolean direction = body.getDirection();

		    treeNode.setStatus(TreeNode.ACTIVE);
		    treeNode.addCoOwnerList(physicalSender);

		    //clean the previous slave if having
		    SPGeneralAction.deleteTreeNode(serverpeer, treeNode);

		    //add new node
		    serverpeer.addListItem(treeNode);
		    boolean result =
		        SPGeneralAction.transferFakeNode(serverpeer, treeNode, 
		        		direction, false, body.getPhysicalSender());

		    if (result)
		    {
		    	ActivateStablePosition activateStablePosition =
		    		new ActivateStablePosition(serverpeer, treeNode, ServerPeer.TIME_TO_STABLE_POSITION);
		    }
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new EventHandleException("SP_LB_ROTATION_PULL operation failure", e);
		}
	}

	public boolean isConsumed(Message msg) throws EventHandleException 
	{
		if (msg.getHead().getMsgType() == MsgType.SP_LB_ROTATION_PULL.getValue())
			return true;
		return false;
	}

}