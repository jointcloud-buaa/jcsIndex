/*
 * @(#) ActivateStablePosition.java 1.0 2006-2-24
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.peer.event;

import edu.buaa.act.jcsindex.global.peer.ServerPeer;
import edu.buaa.act.jcsindex.global.peer.info.TreeNode;
import edu.buaa.act.jcsindex.global.protocol.Head;
import edu.buaa.act.jcsindex.global.protocol.Message;
import edu.buaa.act.jcsindex.global.protocol.MsgType;
import edu.buaa.act.jcsindex.global.protocol.body.Body;
import edu.buaa.act.jcsindex.global.protocol.body.SPLBStablePositionBody;

import java.util.Timer;
import java.util.TimerTask;

/**
 * Used for activating stable position of a super peer.
 * Usually during rotation process, a super peer is activated
 * after receiving enough feedbacks from its parent, children 
 * and neighbor nodes. However, in some cases, it may not received
 * enough replies. As a result, it cannot be activated.
 * 
 * @author Vu Quang Hieu
 * @version 1.0 2006-2-24
 */

public class ActivateStablePosition 
{
	
	// protected members
	protected ServerPeer serverpeer;
	protected TreeNode treeNode;
	protected Timer timer;

	public ActivateStablePosition(ServerPeer serverpeer, TreeNode treeNode, int seconds) 
	{		
		this.serverpeer = serverpeer;
		this.treeNode = treeNode;
		this.timer = new Timer();
		this.timer.schedule(new ReminderStablePosition(), seconds * 1000);
	}

	public TreeNode getTreeNode()
	{
		return this.treeNode;
	}

	public void stop()
	{
		this.timer.cancel();
	}

	class ReminderStablePosition extends TimerTask
	{
		public void run()
		{
			try
			{
				if (treeNode.getRole() == 1)
				{
					//System.out.println("Activate stable position of node " + treeNode.getLogicalInfo().toString());
					
					Body body = new SPLBStablePositionBody(serverpeer.getPhysicalInfo(),
										treeNode.getLogicalInfo(), treeNode.getLogicalInfo());
					for (int i = 0; i < treeNode.getCoOwnerSize(); i++) 
					{	
						Head head = new Head();
						head.setMsgType(MsgType.SP_LB_STABLE_POSITION.getValue());
						Message result = new Message(head, body);
						serverpeer.sendMessage(treeNode.getCoOwnerList(i), result);					
					}
					treeNode.setNumOfExpectedRTReply(0);
					treeNode.clearCoOwnerList();
				}
				timer.cancel();
				serverpeer.stopActivateStablePosition();
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
		}
	}
	
}
