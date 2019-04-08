/*
 * @(#) SPLeaveUrgentListener.java 1.0 2006-3-3
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
import edu.buaa.act.jcsindex.global.protocol.body.SPLeaveUrgentBody;

import java.io.ObjectOutputStream;

/**
 * Implement a listener for processing SP_LEAVE_URGENT message.
 * 
 * @author Vu Quang Hieu
 * @author (Modified by) Xu Linhao
 * @version 1.0 2006-2-22
 */

public class SPLeaveUrgentListener extends ActionAdapter
{

	public SPLeaveUrgentListener(AbstractInstance instance)
	{
		super(instance);
	}

	public void actionPerformed(ObjectOutputStream oos, Message msg) throws EventHandleException
	{
		try
		{
			/* get the handler of the ServerPeer */
			ServerPeer serverpeer = (ServerPeer) instance.peer();
			PhysicalInfo physicalInfo = serverpeer.getPhysicalInfo();
			
			/* get the message body */
			SPLeaveUrgentBody body = (SPLeaveUrgentBody) msg.getBody();
			
			TreeNode treeNode = body.getTreeNode();
			serverpeer.addListItem(treeNode);
			
			if (SPGeneralAction.checkRotationPull(treeNode))
    		{
    			SPGeneralAction.doLeave(serverpeer, physicalInfo, treeNode);
    		}
    		else{
    			SPGeneralAction.doFindReplacement(serverpeer, physicalInfo, treeNode);
    		}
			
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new EventHandleException("SP_LEAVE_URGENT operation failure", e);
		}
	}	
	
	public boolean isConsumed(Message msg) throws EventHandleException 
	{
		if (msg.getHead().getMsgType() == MsgType.SP_LEAVE_URGENT.getValue())
			return true;
		return false;
	}

}