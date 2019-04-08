/*
 * @(#) SPLIChildReplyListener.java 1.0 2006-12-10
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
import edu.buaa.act.jcsindex.global.protocol.body.SPLIChildReplyBody;

import java.io.ObjectOutputStream;

/**
 * Implement a listener for processing SP_LI_CHILD_REPLY message.
 * 
 * @author Vu Quang Hieu
 * @version 1.0 2006-12-10
 */

public class SPLIChildReplyListener extends ActionAdapter
{

	public SPLIChildReplyListener(AbstractInstance instance)
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
			SPLIChildReplyBody body = (SPLIChildReplyBody) msg.getBody();
			
			/* get the correspondent tree node*/
			TreeNode treeNode = serverpeer.getTreeNode(body.getLogicalDestination());
			if (treeNode == null)
			{
				System.out.println("SP_LI_CHILD_REPLY: Tree node is null, do not process the message");
				return;				
			}
			
			treeNode.setLeftChild(body.getLeftChild());
			treeNode.setRightChild(body.getRightChild());
			
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new EventHandleException("Message processing fails", e);
		}
	}

	public boolean isConsumed(Message msg) throws EventHandleException 
	{
		if (msg.getHead().getMsgType() == MsgType.SP_LI_CHILD_REPLY.getValue())
			return true;
		return false;
	}

}