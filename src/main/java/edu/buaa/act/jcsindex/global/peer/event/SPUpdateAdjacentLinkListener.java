/*
 * @(#) SPUpdateAdjacentLinkListener.java 1.0 2006-2-22
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
import edu.buaa.act.jcsindex.global.protocol.body.SPUpdateAdjacentLinkBody;

import java.io.ObjectOutputStream;

/**
 * Implement a listener for processing SP_UPDATE_ADJACENT_LINK message.
 * 
 * @author Vu Quang Hieu
 * @author (Modified by) Xu Linhao
 * @version 1.0 2006-2-22
 */

public class SPUpdateAdjacentLinkListener extends ActionAdapter
{

	public SPUpdateAdjacentLinkListener(AbstractInstance instance)
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
			SPUpdateAdjacentLinkBody body = (SPUpdateAdjacentLinkBody) msg.getBody();
			
			TreeNode treeNode = serverpeer.getTreeNode(body.getLogicalDestination());
			if (treeNode == null)
			{
				System.out.println("SP_UPDATE_ADJACENT_LINK: Tree node is null, do not process the message");
				return;				
			}
			
			if (!body.getDirection())
			{
				treeNode.setLeftAdjacentNode(body.getNewAdjacent());
			}
			else
			{
				treeNode.setRightAdjacentNode(body.getNewAdjacent());
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new EventHandleException("Super peer updates adjacent link failure", e);
		}
	}

	public boolean isConsumed(Message msg) throws EventHandleException 
	{
		if (msg.getHead().getMsgType() == MsgType.SP_UPDATE_ADJACENT_LINK.getValue())
			return true;
		return false;
	}

}