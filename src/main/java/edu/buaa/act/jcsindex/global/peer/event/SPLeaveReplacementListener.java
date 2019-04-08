/*
 * @(#) SPLeaveFindReplaceListener.java 1.0 2006-2-22
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.peer.event;

import edu.buaa.act.jcsindex.global.AbstractInstance;
import edu.buaa.act.jcsindex.global.peer.ServerPeer;
import edu.buaa.act.jcsindex.global.peer.info.TreeNode;
import edu.buaa.act.jcsindex.global.peer.management.EventHandleException;
import edu.buaa.act.jcsindex.global.protocol.Message;
import edu.buaa.act.jcsindex.global.protocol.MsgType;
import edu.buaa.act.jcsindex.global.protocol.body.SPLeaveReplacementBody;

import java.io.ObjectOutputStream;

/**
 * Implement a listener for processing SP_LEAVE_FIND_REPLACEMENT_NODE message.
 * 
 * @author Vu Quang Hieu
 * @author (Modified by) Xu Linhao
 * @version 1.0 2006-2-22
 */
 
public class SPLeaveReplacementListener extends ActionAdapter
{

	public SPLeaveReplacementListener(AbstractInstance instance)
	{
		super(instance);
	}

	public void actionPerformed(ObjectOutputStream oos, Message msg) throws EventHandleException
	{
		try
		{
			/* get the handler of the ServerPeer */
			ServerPeer serverpeer = (ServerPeer) instance.peer();
			
			/* get the message body */
			SPLeaveReplacementBody body = (SPLeaveReplacementBody) msg.getBody();
		
			TreeNode treeNode = body.getTreeNode();
			treeNode.setContent(body.getContent());
			SPGeneralAction.saveData(body.getContent().getData());
			treeNode.setStatus(TreeNode.ACTIVE);
			treeNode.setRole(TreeNode.MASTER);
			treeNode.addCoOwnerList(body.getPhysicalSender());
			serverpeer.addListItem(treeNode);			
			SPGeneralAction.updateRotateRoutingTable(serverpeer, treeNode);

			serverpeer.setActivateStablePosition(new ActivateStablePosition(serverpeer, treeNode, ServerPeer.TIME_TO_STABLE_POSITION));
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new EventHandleException("Replace a super peer's position failure when it leaves network", e);
		}	
	}

	public boolean isConsumed(Message msg) throws EventHandleException 
	{
		if (msg.getHead().getMsgType() == MsgType.SP_LEAVE_REPLACEMENT.getValue())
			return true;
		return false;
	}

}