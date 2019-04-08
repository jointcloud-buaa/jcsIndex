/*
 * @(#) SPLBGetLoadInfoResendListener.java 1.0 2006-2-22
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.peer.event;

import edu.buaa.act.jcsindex.global.AbstractInstance;
import edu.buaa.act.jcsindex.global.peer.ServerPeer;
import edu.buaa.act.jcsindex.global.peer.info.TreeNode;
import edu.buaa.act.jcsindex.global.peer.management.EventHandleException;
import edu.buaa.act.jcsindex.global.protocol.Head;
import edu.buaa.act.jcsindex.global.protocol.Message;
import edu.buaa.act.jcsindex.global.protocol.MsgType;
import edu.buaa.act.jcsindex.global.protocol.body.Body;
import edu.buaa.act.jcsindex.global.protocol.body.SPLBGetLoadInfoBody;
import edu.buaa.act.jcsindex.global.protocol.body.SPLBGetLoadInfoResendBody;

import java.io.ObjectOutputStream;

/**
 * Implement a listener for processing SP_LB_GET_LOAD_INFO_RESEND message.
 * 
 * @author Vu Quang Hieu
 * @author (Modified by) Xu Linhao
 * @version 1.0 2006-2-22
 */

public class SPLBGetLoadInfoResendListener extends ActionAdapter
{

	public SPLBGetLoadInfoResendListener(AbstractInstance instance)
	{
		super(instance);
	}

	public void actionPerformed(ObjectOutputStream oos, Message msg) throws EventHandleException
	{
    	Message result = null;
    	Head thead = new Head();
    	Body tbody = null;
    	
		try
		{
			/* get the handler of the ServerPeer */
			ServerPeer serverpeer = (ServerPeer) instance.peer();
			
			/* get the message body */
			SPLBGetLoadInfoResendBody body = (SPLBGetLoadInfoResendBody) msg.getBody();
			
			TreeNode treeNode = serverpeer.getTreeNode(body.getLogicalDestination());
			if (treeNode == null)
			{
				System.out.println("SP_LB_GET_LOAD_INFO_RESEND: Tree node is null, do not process the message");
				return;				
			}
			
			boolean direction = body.getDirection();
			if (!direction)
			{
				tbody = new SPLBGetLoadInfoBody(serverpeer.getPhysicalInfo(), treeNode.getLogicalInfo(),
												false, treeNode.getRightAdjacentNode().getLogicalInfo());

				thead.setMsgType(MsgType.SP_LB_GET_LOAD_INFO.getValue());
				result = new Message(thead, tbody);
				serverpeer.sendMessage(treeNode.getRightAdjacentNode().getPhysicalInfo(), result);
			}
			else
			{
				tbody = new SPLBGetLoadInfoBody(serverpeer.getPhysicalInfo(), treeNode.getLogicalInfo(), 
												true, treeNode.getLeftAdjacentNode().getLogicalInfo());

				thead.setMsgType(MsgType.SP_LB_GET_LOAD_INFO.getValue());
				result = new Message(thead, tbody);
				serverpeer.sendMessage(treeNode.getLeftAdjacentNode().getPhysicalInfo(), result);
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new EventHandleException("SP_LB_GET_LOAD_INFO_RESEND operation failure", e);
		}
	}

	public boolean isConsumed(Message msg) throws EventHandleException 
	{
		if (msg.getHead().getMsgType() == MsgType.SP_LB_GET_LOAD_INFO_RESEND.getValue())
			return true;
		return false;
	}

}