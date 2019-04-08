/*
 * @(#) SPLeaveNotifyListener.java 1.0 2006-3-4
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.peer.event;

import edu.buaa.act.jcsindex.global.AbstractInstance;
import edu.buaa.act.jcsindex.global.peer.ServerPeer;
import edu.buaa.act.jcsindex.global.peer.info.PhysicalInfo;
import edu.buaa.act.jcsindex.global.peer.info.RoutingItemInfo;
import edu.buaa.act.jcsindex.global.peer.info.TreeNode;
import edu.buaa.act.jcsindex.global.peer.management.EventHandleException;
import edu.buaa.act.jcsindex.global.protocol.Message;
import edu.buaa.act.jcsindex.global.protocol.MsgType;
import edu.buaa.act.jcsindex.global.protocol.body.SPLeaveNotifyBody;

import java.io.ObjectOutputStream;

/**
 * Implement a listener for processing SP_LEAVE_NOTIFY message.
 * 
 * @author Vu Quang Hieu 
 * @version 1.0 2006-3-4
 */

public class SPLeaveNotifyListener extends ActionAdapter
{

	public SPLeaveNotifyListener(AbstractInstance instance)
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
			SPLeaveNotifyBody body = (SPLeaveNotifyBody) msg.getBody();
			TreeNode treeNode = serverpeer.getTreeNode(body.getLogicalDestination());
			if (treeNode == null)
			{
				System.out.println("SP_LEAVE_NOTIFY: Tree node is null, do not process the message");
				return;				
			}
			
			PhysicalInfo physicalReplacer = body.getPhysicalReplacer();
			RoutingItemInfo nodeInfo;
			int index = body.getIndex();
			
			switch (body.getPosition())
			{
				case 0: //departure is the parent node
					treeNode.getParentNode().setPhysicalInfo(physicalReplacer);
					break;
				case 1: //departure is the left adjacent node
					if (treeNode.getLeftAdjacentNode() != null)
						treeNode.getLeftAdjacentNode().setPhysicalInfo(physicalReplacer);
					break;
				case 2: //departure is the right adjacent node
					if (treeNode.getRightAdjacentNode() != null)
						treeNode.getRightAdjacentNode().setPhysicalInfo(physicalReplacer);
					break;
				case 3:	//departure is the left neighbor node			
					nodeInfo = treeNode.getLeftRoutingTable().getRoutingTableNode(index);
				    if (nodeInfo != null)
				    	nodeInfo.setPhysicalInfo(physicalReplacer);
					break;
				case 4: //departure is the right neighbor node
					nodeInfo = treeNode.getRightRoutingTable().getRoutingTableNode(index);
					if (nodeInfo != null)
						nodeInfo.setPhysicalInfo(physicalReplacer);
					break;
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new EventHandleException("Super peer notifies its neighbors failure when it leaves network", e);
		}
	}	
	
	public boolean isConsumed(Message msg) throws EventHandleException 
	{
		if (msg.getHead().getMsgType() == MsgType.SP_LEAVE_NOTIFY.getValue())
			return true;
		return false;
	}

}