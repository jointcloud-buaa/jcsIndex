/*
 * @(#) CheckImbalance.java 1.0 2006-2-22
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.peer.request;

import edu.buaa.act.jcsindex.global.ServerInstance;
import edu.buaa.act.jcsindex.global.logging.LogManager;
import edu.buaa.act.jcsindex.global.peer.ServerPeer;
import edu.buaa.act.jcsindex.global.peer.info.LogicalInfo;
import edu.buaa.act.jcsindex.global.peer.info.RoutingTableInfo;
import edu.buaa.act.jcsindex.global.peer.info.TreeNode;
import edu.buaa.act.jcsindex.global.protocol.Head;
import edu.buaa.act.jcsindex.global.protocol.Message;
import edu.buaa.act.jcsindex.global.protocol.MsgType;
import edu.buaa.act.jcsindex.global.protocol.body.Body;
import edu.buaa.act.jcsindex.global.protocol.body.SPNotifyImbalanceBody;
import edu.buaa.act.jcsindex.global.utils.PeerMath;

import java.util.Timer;
import java.util.TimerTask;

/**
 * Used for checking the status of the workload at peers.
 * 
 * @author Vu Quang Hieu
 * @version 1.0 2006-2-22
 */

public class CheckImbalance 
{

	// private members
	private ServerInstance serverinstance;
	private ServerPeer serverpeer;
	private Timer timer;
		
	public CheckImbalance(ServerPeer serverpeer, int seconds) 
	{
		this.serverpeer = serverpeer;
		this.serverinstance  = (ServerInstance) serverpeer.getInstance();
		this.timer = new Timer();
		this.timer.schedule(new ReminderCheckImbalance(), 0, seconds * 1000);
	}

	class ReminderCheckImbalance extends TimerTask
	{
		public void run()
		{
			Head head = new Head();
			Body body = null;
			try
			{
				//System.out.println("Checking imbalance ... ");
				if (serverpeer.getListSize() == 1)
				{
					TreeNode treeNode = serverpeer.getListItem(0);
					if (((treeNode.getLeftChild() != null) || (treeNode.getRightChild() != null))
						&& (!treeNode.isNotifyImbalance()))
					{
						LogicalInfo missingNode;
						int missNodeLevel = treeNode.getLogicalInfo().getLevel();
						int missNodeNumber;
						RoutingTableInfo leftRoutingTable = treeNode.getLeftRoutingTable();
						for (int i = 0; i < leftRoutingTable.getTableSize(); i++)
						{
							if (leftRoutingTable.getRoutingTableNode(i) == null)
							{
								missNodeNumber = treeNode.getLogicalInfo().getNumber() - PeerMath.pow(2, i);
								missingNode = new LogicalInfo(missNodeLevel, missNodeNumber);
								treeNode.notifyImbalance(true);
								treeNode.setMissingNode(missingNode);
								body = new SPNotifyImbalanceBody(serverpeer.getPhysicalInfo(),
										treeNode.getLogicalInfo(), missingNode, true,
										treeNode.getParentNode().getLogicalInfo());
	
								head.setMsgType(MsgType.SP_NOTIFY_IMBALANCE.getValue());
								Message message = new Message(head, body);
								serverpeer.sendMessage(treeNode.getParentNode().getPhysicalInfo(), message);

								// add log event
//								serverinstance.log(LogEventType.INFORMATION.getValue(), MsgType.description(head.getMsgType()) + " request is sent out...",	Inet.getInetAddress2().getHostName(), Inet.getInetAddress(), System.getProperty("user.name"));
								LogManager.getLogger(serverpeer).info(MsgType.description(head.getMsgType()) + " request is sent out...");
								return;
							}
						}
						
						RoutingTableInfo rightRoutingTable = treeNode.getRightRoutingTable();
						for (int i = 0; i < rightRoutingTable.getTableSize(); i++)
						{
							if (rightRoutingTable.getRoutingTableNode(i) == null)
							{
								missNodeNumber = treeNode.getLogicalInfo().getNumber() + PeerMath.pow(2, i);
								missingNode = new LogicalInfo(missNodeLevel, missNodeNumber);
								treeNode.notifyImbalance(true);
								treeNode.setMissingNode(missingNode);
								
								body = new SPNotifyImbalanceBody(serverpeer.getPhysicalInfo(), 
										treeNode.getLogicalInfo(), missingNode, false,
										treeNode.getParentNode().getLogicalInfo());
	
								head.setMsgType(MsgType.SP_NOTIFY_IMBALANCE.getValue());
								Message message = new Message(head, body);
								serverpeer.sendMessage(treeNode.getParentNode().getPhysicalInfo(), message);

								// add log event
//								serverinstance.log(LogEventType.INFORMATION.getValue(), MsgType.description(head.getMsgType()) + " request is sent out...",	Inet.getInetAddress2().getHostName(), Inet.getInetAddress(), System.getProperty("user.name"));
								LogManager.getLogger(serverpeer).info(MsgType.description(head.getMsgType()) + " request is sent out...");
								return;
							}
						}	
					}
				}
			}
			catch (Exception e)
			{
//				serverinstance.log(LogEventType.ERROR.getValue(), "Exception happens because:\r\n" + Tools.getException(e),	Inet.getInetAddress2().getHostName(), Inet.getInetAddress(), System.getProperty("user.name"));
//				LogManager.getLogger(serverpeer).error("Exception happens in ReminderCheckImbalance", e);
			}
		}
	}
}
