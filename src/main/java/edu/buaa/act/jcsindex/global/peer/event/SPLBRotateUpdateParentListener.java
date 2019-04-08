/*
 * @(#) SPLBRotateUpdateParentListener.java 1.0 2006-2-22
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.peer.event;

import edu.buaa.act.jcsindex.global.AbstractInstance;
import edu.buaa.act.jcsindex.global.peer.ServerPeer;
import edu.buaa.act.jcsindex.global.peer.info.ParentNodeInfo;
import edu.buaa.act.jcsindex.global.peer.info.TreeNode;
import edu.buaa.act.jcsindex.global.peer.management.EventHandleException;
import edu.buaa.act.jcsindex.global.protocol.Head;
import edu.buaa.act.jcsindex.global.protocol.Message;
import edu.buaa.act.jcsindex.global.protocol.MsgType;
import edu.buaa.act.jcsindex.global.protocol.body.Body;
import edu.buaa.act.jcsindex.global.protocol.body.SPLBRotateUpdateParentBody;
import edu.buaa.act.jcsindex.global.protocol.body.SPLBRotateUpdateParentReplyBody;

import java.io.ObjectOutputStream;

/**
 * Implement a listener for processing SP_LB_ROTATE_UPDATE_PARENT message.
 * 
 * @author Vu Quang Hieu
 * @author (Modified by) Xu Linhao
 * @version 1.0 2006-2-22
 */

public class SPLBRotateUpdateParentListener extends ActionAdapter
{

	public SPLBRotateUpdateParentListener(AbstractInstance instance)
	{
		super(instance);
	}

	public void actionPerformed(ObjectOutputStream oos, Message msg) throws EventHandleException
	{
		super.actionPerformed(oos, msg);
		
    	Head thead = new Head();
    	Body tbody = null;
    	
		try
		{
			/* get the handler of the ServerPeer */
			ServerPeer serverpeer = (ServerPeer) instance.peer();
			
			/* get the message body */
			SPLBRotateUpdateParentBody body = (SPLBRotateUpdateParentBody) msg.getBody();
			
			TreeNode treeNode = serverpeer.getTreeNode(body.getLogicalDestination());
			if (treeNode == null)
			{
				System.out.println("SP_LB_ROTATE_UPDATE_PARENT: Tree node is null, do not process the message");
				return;				
			}
			
			ParentNodeInfo newParent = new ParentNodeInfo(body.getPhysicalSender(), body.getLogicalSender());

			treeNode.setParentNode(newParent);
			if ((treeNode.getLogicalInfo().getNumber() % 2) == 0)
			{	
				tbody = new SPLBRotateUpdateParentReplyBody(serverpeer.getPhysicalInfo(),
															treeNode.getLogicalInfo(),
															true, body.getLogicalSender());
			}
			else
			{
				tbody = new SPLBRotateUpdateParentReplyBody(serverpeer.getPhysicalInfo(), 
															treeNode.getLogicalInfo(),
															false, body.getLogicalSender());
			}

			thead.setMsgType(MsgType.SP_LB_ROTATE_UPDATE_PARENT_REPLY.getValue());
			Message message = new Message(thead, tbody);
			serverpeer.sendMessage(body.getPhysicalSender(), message);

		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new EventHandleException("SP_LB_ROTATE_UPDATE_PARENT operation failure", e);
		}
	}

	public boolean isConsumed(Message msg) throws EventHandleException 
	{
		if (msg.getHead().getMsgType() == MsgType.SP_LB_ROTATE_UPDATE_PARENT.getValue())
			return true;
		return false;
	}

}