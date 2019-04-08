/*
 * @(#) SPLIAdjacentReplyListener.java 1.0 2006-12-04
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
import edu.buaa.act.jcsindex.global.protocol.body.SPLIAdjacentReplyBody;

import java.io.ObjectOutputStream;

/**
 * Implement a listener for processing SP_LI_ADJACENT_REPLY message.
 * 
 * @author Vu Quang Hieu
 * @version 1.0 2006-12-04
 */

public class SPLIAdjacentReplyListener extends ActionAdapter
{

	public SPLIAdjacentReplyListener(AbstractInstance instance)
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
			SPLIAdjacentReplyBody body = (SPLIAdjacentReplyBody) msg.getBody();
			
			/* get the correspondent tree node*/
			TreeNode treeNode = serverpeer.getTreeNode(body.getLogicalDestination());
			if (treeNode == null)
			{
				System.out.println("SP_LI_ADJACENT_REPLY: Tree node is null, do not process the message");
				return;				
			}
			
			if (body.getDirection() == SPLIAdjacentReplyBody.FROM_LEFT_TO_RIGHT)
			{
				treeNode.setLeftAdjacentNode(body.getNewAdjacent());
			}
			else
			{
				treeNode.setRightAdjacentNode(body.getNewAdjacent());
			}
			
			int numOfExpectedRTReply = treeNode.getNumOfExpectedRTReply();
			if (numOfExpectedRTReply > 0)
			{
				numOfExpectedRTReply --;
				treeNode.setNumOfExpectedRTReply(numOfExpectedRTReply);
				if (numOfExpectedRTReply == 0)
				{
					treeNode.setStatus(TreeNode.ACTIVE);
					if (SPGeneralAction.checkRotationPull(treeNode))
		    		{
		    			SPGeneralAction.doLeave(serverpeer, serverpeer.getPhysicalInfo(), treeNode);
		    		}
		    		else{
		    			SPGeneralAction.doFindReplacement(serverpeer, serverpeer.getPhysicalInfo(), treeNode);
		    		}
				}
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new EventHandleException("Message processing fails", e);
		}
	}

	public boolean isConsumed(Message msg) throws EventHandleException 
	{
		if (msg.getHead().getMsgType() == MsgType.SP_LI_ADJACENT_REPLY.getValue())
			return true;
		return false;
	}

}