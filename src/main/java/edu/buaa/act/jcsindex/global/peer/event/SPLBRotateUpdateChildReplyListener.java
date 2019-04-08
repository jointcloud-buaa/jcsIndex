/*
 * @(#) SPLBRotateUpdateChildReplyListener.java 1.0 2006-2-22
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
import edu.buaa.act.jcsindex.global.protocol.body.SPLBRotateUpdateChildReplyBody;
import edu.buaa.act.jcsindex.global.protocol.body.SPLBStablePositionBody;

import java.io.ObjectOutputStream;

/**
 * Implement a listener for processing SP_LB_ROTATE_UPDATE_CHILD_REPLY message.
 * 
 * @author Vu Quang Hieu
 * @author (Modified by) Xu Linhao
 * @version 1.0 2006-2-22
 */

public class SPLBRotateUpdateChildReplyListener extends ActionAdapter
{

	public SPLBRotateUpdateChildReplyListener(AbstractInstance instance)
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
			SPLBRotateUpdateChildReplyBody body = (SPLBRotateUpdateChildReplyBody) msg.getBody();
			
			TreeNode treeNode = serverpeer.getTreeNode(body.getLogicalDestination());
			if (treeNode == null)
			{
				System.out.println("SP_LB_ROTATE_UPDATE_CHILD_REPLY: Tree node is null, do not process the message");
				return;				
			}
			
			ParentNodeInfo newParent = new ParentNodeInfo(body.getPhysicalSender(), body.getLogicalSender());
			treeNode.setParentNode(newParent);

			synchronized (treeNode)
			{
				int numOfExpectedRTReply;
				numOfExpectedRTReply = treeNode.getNumOfExpectedRTReply();
				if (numOfExpectedRTReply > 0)
				{
					numOfExpectedRTReply --;
					treeNode.setNumOfExpectedRTReply(numOfExpectedRTReply);
					if (numOfExpectedRTReply == 0)
					{
						serverpeer.stopActivateStablePosition();
						tbody = new SPLBStablePositionBody(serverpeer.getPhysicalInfo(),
								treeNode.getLogicalInfo(), treeNode.getLogicalInfo());
		
						thead.setMsgType(MsgType.SP_LB_STABLE_POSITION.getValue());
						Message message = new Message(thead, tbody);
						for (int i = 0; i < treeNode.getCoOwnerSize(); i++)
						{
							serverpeer.sendMessage(treeNode.getCoOwnerList(i), message);
						}
						treeNode.clearCoOwnerList();				
					}
				}
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new EventHandleException("SP_LB_ROTATE_UPDATE_CHILD_REPLY operation failure", e);
		}
	}

	public boolean isConsumed(Message msg) throws EventHandleException 
	{
		if (msg.getHead().getMsgType() == MsgType.SP_LB_ROTATE_UPDATE_CHILD_REPLY.getValue())
			return true;
		return false;
	}

}