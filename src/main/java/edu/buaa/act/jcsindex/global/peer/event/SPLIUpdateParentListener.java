/*
 * @(#) SPLIUpdateParentListener.java 1.0 2006-12-10
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
import edu.buaa.act.jcsindex.global.protocol.body.SPLIUpdateParentBody;

import java.io.ObjectOutputStream;

/**
 * Implement a listener for processing SP_LI_UPDATE_PARENT message.
 * 
 * @author Vu Quang Hieu
 * @version 1.0 2006-12-10
 */

public class SPLIUpdateParentListener extends ActionAdapter
{

	public SPLIUpdateParentListener(AbstractInstance instance)
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
			SPLIUpdateParentBody body = (SPLIUpdateParentBody) msg.getBody();
			
			/* get the correspondent tree node*/
			TreeNode treeNode = serverpeer.getTreeNode(body.getLogicalDestination());
			if (treeNode == null)
			{
				System.out.println("SP_LI_UPDATE_PARENT: Tree node is null, do not process the message");
				return;				
			}
			
			treeNode.getParentNode().setPhysicalInfo(body.getPhysicalParent());
			
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new EventHandleException("Message processing fails", e);
		}
	}

	public boolean isConsumed(Message msg) throws EventHandleException 
	{
		if (msg.getHead().getMsgType() == MsgType.SP_LI_UPDATE_PARENT.getValue())
			return true;
		return false;
	}

}