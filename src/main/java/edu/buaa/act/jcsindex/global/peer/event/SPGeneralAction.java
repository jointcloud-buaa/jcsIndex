package edu.buaa.act.jcsindex.global.peer.event;

import edu.buaa.act.jcsindex.global.peer.ServerPeer;
import edu.buaa.act.jcsindex.global.peer.info.*;
import edu.buaa.act.jcsindex.global.protocol.Head;
import edu.buaa.act.jcsindex.global.protocol.Message;
import edu.buaa.act.jcsindex.global.protocol.MsgType;
import edu.buaa.act.jcsindex.global.protocol.body.*;
import edu.buaa.act.jcsindex.global.utils.PeerMath;

import java.util.Vector;

/**
 * Implement general methods, which may be used by a variety of listeners
 * 
 * @author Vu Quang Hieu 
 * @version 1.0 2006-2-22
 */

public class SPGeneralAction 
{

	/**
	 * Do local load balancing with adjacent nodes
	 * 
	 * @param serverpeer the handler of <code>ServerPeer</code>
	 * @param treeNode information of the current node in the tree structure
	 * @param canLeft <code>true</code> can do load balancing with 
	 * the left adjacent node, <code>false</code> cannot do 
	 * @param canRight <code>true</code> can do load balancing with
	 * the right adjacent node, <code>false</code> cannot do
	 */
	public static void doLoadBalance(ServerPeer serverpeer, TreeNode treeNode, boolean canLeft, boolean canRight)
	{
		Message result = null;
		Head head = new Head();
		Body body = null;

		try
		{
			//check if can split data to a left node
			if ((canLeft) && (treeNode.getLeftAdjacentNode() != null))
			{
				treeNode.setLNElement(-2);
				
				head.setMsgType(MsgType.SP_LB_GET_LOAD_INFO.getValue());
				body = new SPLBGetLoadInfoBody(serverpeer.getPhysicalInfo(), treeNode.getLogicalInfo(),
											   true, treeNode.getLeftAdjacentNode().getLogicalInfo());
				result = new Message(head, body);
				serverpeer.sendMessage(treeNode.getLeftAdjacentNode().getPhysicalInfo(), result);
			}
			else
			{
				treeNode.setLNElement(-1);
			}

			//check if can split data to a right node
			if ((canRight) && (treeNode.getRightAdjacentNode() != null))
			{
				treeNode.setRNElement(-2);

				head.setMsgType(MsgType.SP_LB_GET_LOAD_INFO.getValue());
				body = new SPLBGetLoadInfoBody(serverpeer.getPhysicalInfo(), treeNode.getLogicalInfo(),
											   false, treeNode.getRightAdjacentNode().getLogicalInfo());
				result = new Message(head, body);
				serverpeer.sendMessage(treeNode.getRightAdjacentNode().getPhysicalInfo(), result);
			}
			else
			{
				treeNode.setRNElement(-1);
			}

			if ((treeNode.getLNElement() == -1) && (treeNode.getRNElement() == -1)) 
			{
				//can't split, should increase order
				treeNode.getContent().setOrder(treeNode.getContent().getOrder() * 2);
			}
			else
			{
				treeNode.processLoadBalance(true);
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	/**
	 * Send messages to update range of values to neighbor nodes
	 * 
	 * @param serverpeer the handler of <code>ServerPeer</code>
	 * @param treeNode information of the node in the tree structure
	 */
	public static void updateRangeValues(ServerPeer serverpeer, TreeNode treeNode) 
	{
		Message result = null;
		Head head = new Head();
		Body body = null;	

		RoutingItemInfo transferInfo;
		RoutingItemInfo nodeInfo;

		BoundaryValue minValue = treeNode.getContent().getMinValue();
		BoundaryValue maxValue = treeNode.getContent().getMaxValue();

		try
		{
			nodeInfo = new RoutingItemInfo(serverpeer.getPhysicalInfo(), 
					treeNode.getLogicalInfo(), treeNode.getLeftChild(), 
					treeNode.getRightChild(), minValue, maxValue);

			RoutingTableInfo leftRoutingTable = treeNode.getLeftRoutingTable();
			for (int j = 0; j < leftRoutingTable.getTableSize(); j++) 
			{
				if (leftRoutingTable.getRoutingTableNode(j) != null) 
				{
					transferInfo = leftRoutingTable.getRoutingTableNode(j);

					head.setMsgType(MsgType.SP_UPDATE_MAX_MIN_VALUE.getValue());
					body = new SPUpdateMaxMinValueBody(serverpeer.getPhysicalInfo(),
							treeNode.getLogicalInfo(), nodeInfo, j, true, 
							transferInfo.getLogicalInfo());
					result = new Message(head, body);
					serverpeer.sendMessage(transferInfo.getPhysicalInfo(), result);
				}
			}

			RoutingTableInfo rightRoutingTable = treeNode.getRightRoutingTable();
			for (int j = 0; j < rightRoutingTable.getTableSize(); j++) 
			{
				if (treeNode.getRightRoutingTable().getRoutingTableNode(j) != null) 
				{
					transferInfo = rightRoutingTable.getRoutingTableNode(j);

					head.setMsgType(MsgType.SP_UPDATE_MAX_MIN_VALUE.getValue());
					body = new SPUpdateMaxMinValueBody(serverpeer.getPhysicalInfo(), 
							treeNode.getLogicalInfo(), nodeInfo, j, false, 
							transferInfo.getLogicalInfo());
					result = new Message(head, body);
					serverpeer.sendMessage(transferInfo.getPhysicalInfo(), result);
				}
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	/**
	 * Update routing table of a node whose position is changed 
	 * due to network restructuring
	 * 
	 * @param serverpeer the handler of <code>ServerPeer</code>
	 * @param treeNode information of the node in the tree structure
	 */
	public static void updateRotateRoutingTable(ServerPeer serverpeer, TreeNode treeNode)
	{
		Message result = null;
		Head head = new Head();
		Body body = null;	

		RoutingItemInfo transferInfo;
		RoutingItemInfo nodeInfo;

		BoundaryValue minValue = treeNode.getContent().getMinValue();
		BoundaryValue maxValue = treeNode.getContent().getMaxValue();
	
		synchronized (treeNode)
		{
			try
			{
				nodeInfo = new RoutingItemInfo(serverpeer.getPhysicalInfo(), 
						treeNode.getLogicalInfo(), treeNode.getLeftChild(), 
						treeNode.getRightChild(), minValue, maxValue);
	
				int numOfExpectedRTReply = 0;
				for (int j = 0; j < treeNode.getLeftRoutingTable().getTableSize(); j++) 
				{
					if (treeNode.getLeftRoutingTable().getRoutingTableNode(j) != null) 
					{
						numOfExpectedRTReply++;				
						transferInfo = treeNode.getLeftRoutingTable().getRoutingTableNode(j);
						body = new SPLBRotateUpdateRoutingTableBody(serverpeer.getPhysicalInfo(),
								treeNode.getLogicalInfo(), nodeInfo, j, true, transferInfo.getLogicalInfo());
						
						head.setMsgType(MsgType.SP_LB_ROTATE_UPDATE_ROUTING_TABLE.getValue());
						result = new Message(head, body);
						serverpeer.sendMessage(transferInfo.getPhysicalInfo(), result);
					}
				}
				for (int j = 0; j < treeNode.getRightRoutingTable().getTableSize(); j++) 
				{
					if (treeNode.getRightRoutingTable().getRoutingTableNode(j) != null) 
					{
						numOfExpectedRTReply++;
						transferInfo = treeNode.getRightRoutingTable().getRoutingTableNode(j);
						body = new SPLBRotateUpdateRoutingTableBody(serverpeer.getPhysicalInfo(), 
								treeNode.getLogicalInfo(), nodeInfo, j, false, transferInfo.getLogicalInfo());
						
						head.setMsgType(MsgType.SP_LB_ROTATE_UPDATE_ROUTING_TABLE.getValue());
						result = new Message(head, body);
						serverpeer.sendMessage(transferInfo.getPhysicalInfo(), result);						
					}
				}
				
				//Notify children, parent, adjacents if necessary
				if (treeNode.getParentNode() != null)
				{
					if ((treeNode.getLogicalInfo().getNumber() % 2) == 0)
					{	
						body = new SPLBRotateUpdateChildBody(serverpeer.getPhysicalInfo(),
								treeNode.getLogicalInfo(), true, treeNode.getParentNode().getLogicalInfo());
					}
					else
					{
						body = new SPLBRotateUpdateChildBody(serverpeer.getPhysicalInfo(),
								treeNode.getLogicalInfo(), false, treeNode.getParentNode().getLogicalInfo());
					}
				
				
					head.setMsgType(MsgType.SP_LB_ROTATE_UPDATE_CHILD.getValue());
					result = new Message(head, body);
					serverpeer.sendMessage(treeNode.getParentNode().getPhysicalInfo(), result);
					numOfExpectedRTReply++;
				}
				
				if (treeNode.getLeftChild() != null)
				{
					body = new SPLBRotateUpdateParentBody(serverpeer.getPhysicalInfo(), 
							treeNode.getLogicalInfo(), treeNode.getLeftChild().getLogicalInfo());
					
					head.setMsgType(MsgType.SP_LB_ROTATE_UPDATE_PARENT.getValue());
					result = new Message(head, body);
					serverpeer.sendMessage(treeNode.getLeftChild().getPhysicalInfo(), result);
					numOfExpectedRTReply++;
				}
				
				if (treeNode.getRightChild() != null)
				{
					body = new SPLBRotateUpdateParentBody(serverpeer.getPhysicalInfo(), 
							treeNode.getLogicalInfo(), treeNode.getRightChild().getLogicalInfo());
					
					head.setMsgType(MsgType.SP_LB_ROTATE_UPDATE_PARENT.getValue());
					result = new Message(head, body);
					serverpeer.sendMessage(treeNode.getRightChild().getPhysicalInfo(), result);
					numOfExpectedRTReply++;
				}
				
				if (treeNode.getLeftAdjacentNode() != null)
				{
					body = new SPLBRotateUpdateAdjacentBody(serverpeer.getPhysicalInfo(), 
							treeNode.getLogicalInfo(), true, treeNode.getLeftAdjacentNode().getLogicalInfo());
					
					head.setMsgType(MsgType.SP_LB_ROTATE_UPDATE_ADJACENT.getValue());
					result = new Message(head, body);
					serverpeer.sendMessage(treeNode.getLeftAdjacentNode().getPhysicalInfo(), result);
					numOfExpectedRTReply++;
				}
				
				if (treeNode.getRightAdjacentNode() != null)
				{
					body = new SPLBRotateUpdateAdjacentBody(serverpeer.getPhysicalInfo(), 
							treeNode.getLogicalInfo(), false, treeNode.getRightAdjacentNode().getLogicalInfo());
					
					head.setMsgType(MsgType.SP_LB_ROTATE_UPDATE_ADJACENT.getValue());
					result = new Message(head, body);
					serverpeer.sendMessage(treeNode.getRightAdjacentNode().getPhysicalInfo(), result);
					numOfExpectedRTReply++;
				}
				treeNode.setNumOfExpectedRTReply(numOfExpectedRTReply);
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
		}
	}

	/**
	 * Check if pulling or deleting of a node affects balance of the 
	 * tree structure
	 * 
	 * @param treeNode information of the node in the tree structure
	 * @return <code>true</code> absence of the node doesn't affect
	 * the balance of the tree, <code>false</code> absence of the node
	 * may cause imbalance of the tree
	 */
	public static boolean checkRotationPull(TreeNode treeNode)
	{
		RoutingItemInfo temptInfo;
		if ((treeNode.getLeftChild() != null) || 
			(treeNode.getRightChild() != null))
		{
			return false;
		}

		RoutingTableInfo leftRoutingTable = treeNode.getLeftRoutingTable();
		for (int i = 0; i < leftRoutingTable.getTableSize(); i++)
		{
			temptInfo = leftRoutingTable.getRoutingTableNode(i);
			if (temptInfo != null)
			{
				if ((temptInfo.getLeftChild() != null) || 
					(temptInfo.getRightChild() != null))
				{
					return false;
				}
			}
		}

		RoutingTableInfo rightRoutingTable = treeNode.getRightRoutingTable();
		for (int i = 0; i < rightRoutingTable.getTableSize(); i++)
		{
			temptInfo = rightRoutingTable.getRoutingTableNode(i);
			if (temptInfo != null)
			{
				if ((temptInfo.getLeftChild() != null) || 
					(temptInfo.getRightChild() != null))
				{
					return false;
				}
			}
		}
		return true;
	}

	/**
	 * Check if pushing or adding a node as a child affects balance of
	 * the tree structure 
	 * 
	 * @param treeNode information of the node in the tree structure
	 * @return <code>true</code> adding of a node as a child doesn't
	 * affect the balance of the tree, <code>false</code> adding of 
	 * a node as a child affects the balance of the tree
	 */
	public static boolean checkRotationPush(TreeNode treeNode)
	{
		RoutingTableInfo leftRoutingTable = treeNode.getLeftRoutingTable();
		for (int i = 0; i < leftRoutingTable.getTableSize(); i++)
		{
			if (leftRoutingTable.getRoutingTableNode(i) == null)
				return false;
		}

		RoutingTableInfo rightRoutingTable = treeNode.getRightRoutingTable();
		for (int i = 0; i < rightRoutingTable.getTableSize(); i++)
		{
			if (rightRoutingTable.getRoutingTableNode(i) == null)
				return false;
		}
		return true;
	}

	/**
	 * Accept the new node as a child
	 * 
	 * @param treeNode information of the node in the tree structure
	 * @param newNodePhysicalInfo physical address of the new node
	 * @param newNodeLogicalInfo logical address of the new node
	 * @param content range of values, data in charge by the new node
	 * @param numOfExpectedRTReply number of routing table replies expected to receive
	 * @param isFake <code>true</code>: the new node is a fake node,
	 * which is created for network restructuring purpose and will be 
	 * transfered to another node, <code>false</code>: the new node 
	 * is a real new joining node
	 * @param direction direction of doing rotation, only has meaning
	 * if isFake is <code>true</code>
	 * @return information of the new node
	 */
	public static RoutingItemInfo doAccept(ServerPeer serverpeer,
										   TreeNode treeNode, PhysicalInfo newNodePhysicalInfo,
										   LogicalInfo newNodeLogicalInfo, ContentInfo content,
										   int numOfExpectedRTReply, boolean isFake, boolean direction)
	{
		Message result = null;
		Head head = new Head();
		Body body = null;	

		AdjacentNodeInfo newNodeLeftAdjacent = null;
		AdjacentNodeInfo newNodeRightAdjacent = null;
		
		try
		{
			if ((newNodeLogicalInfo.getNumber() % 2) == 0) 
			{
				//new node join on the right of this node
				newNodeLeftAdjacent = 
					new AdjacentNodeInfo(serverpeer.getPhysicalInfo(), treeNode.getLogicalInfo());
				if (treeNode.getRightAdjacentNode() != null)
				{
					newNodeRightAdjacent = treeNode.getRightAdjacentNode();
					body = new SPUpdateAdjacentLinkBody(
							serverpeer.getPhysicalInfo(), treeNode.getLogicalInfo(), 
							new AdjacentNodeInfo(newNodePhysicalInfo, newNodeLogicalInfo),
							false, treeNode.getRightAdjacentNode().getLogicalInfo());
	
					head.setMsgType(MsgType.SP_UPDATE_ADJACENT_LINK.getValue());
					result = new Message(head, body);
					serverpeer.sendMessage(treeNode.getRightAdjacentNode().getPhysicalInfo(), result);
				}
				treeNode.setRightAdjacentNode(new AdjacentNodeInfo(newNodePhysicalInfo, newNodeLogicalInfo));
			}
			else
			{
				newNodeRightAdjacent = new AdjacentNodeInfo(serverpeer.getPhysicalInfo(), treeNode.getLogicalInfo());
				if (treeNode.getLeftAdjacentNode() != null)
				{
					newNodeLeftAdjacent = treeNode.getLeftAdjacentNode();
					body = new SPUpdateAdjacentLinkBody(
							serverpeer.getPhysicalInfo(), treeNode.getLogicalInfo(), 
							new AdjacentNodeInfo(newNodePhysicalInfo, newNodeLogicalInfo),
							true, treeNode.getLeftAdjacentNode().getLogicalInfo());
	
					head.setMsgType(MsgType.SP_UPDATE_ADJACENT_LINK.getValue());
					result = new Message(head, body);
					serverpeer.sendMessage(treeNode.getLeftAdjacentNode().getPhysicalInfo(), result);
				}
				treeNode.setLeftAdjacentNode(new AdjacentNodeInfo(newNodePhysicalInfo, newNodeLogicalInfo));
			}
	
			body = new SPJoinAcceptBody(serverpeer.getPhysicalInfo(), 
					treeNode.getLogicalInfo(), newNodeLogicalInfo, 
					newNodeLeftAdjacent, newNodeRightAdjacent, 
					content, numOfExpectedRTReply, isFake, direction);
	
			head.setMsgType(MsgType.SP_JOIN_ACCEPT.getValue());
			result = new Message(head, body);
			serverpeer.sendMessage(newNodePhysicalInfo, result);
	
			RoutingItemInfo newNodeInfo = 
				new RoutingItemInfo(newNodePhysicalInfo, newNodeLogicalInfo,
						null, null, content.getMinValue(), content.getMaxValue());

			return newNodeInfo;
		}
		catch (Exception e)
		{
			e.printStackTrace();
			return null;
		}
	}
	
	/**
	 * Split data to the new node from the current node
	 * 
	 * @param treeNode information of the node in the tree structure
	 * @param direction splitting direction
	 * @param initialOrder predefined order of the system
	 * @return range of values and data in charged by the new node
	 */
	public static ContentInfo splitData(TreeNode treeNode, boolean direction, int initialOrder)
	{

		ContentInfo newNodeContentInfo = null;		

		Vector<IndexValue> transferData = new Vector<IndexValue>();
		// TODO: WARNING
		// storeData从磁盘中获取，存在疑义。
		// 另外就是loadData实际上什么也没做，暂且留着
		Vector<IndexValue> storedData = loadData();
		
		BoundaryValue minValue = treeNode.getContent().getMinValue();
		BoundaryValue maxValue = treeNode.getContent().getMaxValue();
		BoundaryValue midValue = BoundaryValue.average(minValue, maxValue);
		// TODO: 获取节点的范围
		BoundaryValue srl = treeNode.getContent().getSubtreeRangeL();
		BoundaryValue srr = treeNode.getContent().getSubtreeRangeR();
		
		if (!direction)
		{
			//split data to the left hand side			
			for (int i = storedData.size() - 1; i >= 0; i--)
			{
				IndexValue checkValue = (IndexValue)storedData.get(i);
				if (checkValue.compareTo(midValue) < 0)
				{
					storedData.remove(i);
					transferData.add(checkValue);
				}
			}
			
			//update data
			treeNode.getContent().setMinValue(midValue);
			if (treeNode.getContent().getOrder() >= (initialOrder * 2))
			{
				treeNode.getContent().setOrder(treeNode.getContent().getOrder() / 2);
			}
			
			newNodeContentInfo = new ContentInfo(minValue, midValue, treeNode.getContent().getOrder(), transferData);
			// TODO：添加的一段代码用于生成subtree range
			treeNode.setContent(new ContentInfo(midValue, maxValue, treeNode.getContent().getOrder(), storedData, srl, srr));
		}
		else
		{
			//split data to the right hand side
			for (int i = storedData.size() - 1; i >= 0; i--)
			{
				IndexValue checkValue = (IndexValue)storedData.get(i);
				if (checkValue.compareTo(midValue) >= 0)
				{
					storedData.remove(i);
					transferData.add(checkValue);
				}
			}
			
			//update data
			treeNode.getContent().setMaxValue(midValue);
			if (treeNode.getContent().getOrder() >= (initialOrder * 2))
			{
				treeNode.getContent().setOrder(treeNode.getContent().getOrder() / 2);
			}
			
			newNodeContentInfo = new ContentInfo(midValue, maxValue, treeNode.getContent().getOrder(), transferData);
			// TODO：添加的一段代码用于生成subtree range
			treeNode.setContent(new ContentInfo(minValue, midValue, treeNode.getContent().getOrder(), storedData, srl, srr));
		}
		// TODO: WARNING
		// 以下两段代码实际上没有任何用处，暂且留着
		deleteAllData();
		saveData(storedData);
		return newNodeContentInfo;
	}

	/**
	 * Send update routing table to neighbor nodes
	 * 
	 * @param treeNode information of the node in the tree structure
	 * @param nodeInfo information of the updated/changed node
	 */
	public static void updateRoutingTable(ServerPeer serverpeer, TreeNode treeNode, RoutingItemInfo nodeInfo)
	{
		Message result = null;
		Head head = new Head();
		Body body = null;

		RoutingItemInfo senderInfo;
		PhysicalInfo destinationPhysicalInfo;
		LogicalInfo destinationLogicalInfo;
		
		try
		{
			senderInfo = new RoutingItemInfo(serverpeer.getPhysicalInfo(), treeNode.getLogicalInfo(), treeNode.getLeftChild(), 
											treeNode.getRightChild(), treeNode.getContent().getMinValue(), 
											treeNode.getContent().getMaxValue());
	
			for (int i = 0; i < treeNode.getLeftRoutingTable().getTableSize(); i++) 
			{
				if (treeNode.getLeftRoutingTable().getRoutingTableNode(i) != null)
				{
					destinationPhysicalInfo = treeNode.getLeftRoutingTable().getRoutingTableNode(i).getPhysicalInfo();
					destinationLogicalInfo  = treeNode.getLeftRoutingTable().getRoutingTableNode(i).getLogicalInfo();
					
					body = new SPUpdateRoutingTableIndirectlyBody(serverpeer.getPhysicalInfo(),
											treeNode.getLogicalInfo(), senderInfo, nodeInfo, 
											i, true, false, destinationLogicalInfo);
	
					head.setMsgType(MsgType.SP_UPDATE_ROUTING_TABLE_INDIRECTLY.getValue());
					result = new Message(head, body);
					serverpeer.sendMessage(destinationPhysicalInfo, result);
				}
			}
			for (int i = 0; i < treeNode.getRightRoutingTable().getTableSize(); i++) 
			{
				if (treeNode.getRightRoutingTable().getRoutingTableNode(i) != null) 
				{
					destinationPhysicalInfo = treeNode.getRightRoutingTable().getRoutingTableNode(i).getPhysicalInfo();;
					destinationLogicalInfo  = treeNode.getRightRoutingTable().getRoutingTableNode(i).getLogicalInfo();;
					body = new SPUpdateRoutingTableIndirectlyBody(serverpeer.getPhysicalInfo(),
											treeNode.getLogicalInfo(), senderInfo, nodeInfo, 
											i, false, false, destinationLogicalInfo);
	
					head.setMsgType(MsgType.SP_UPDATE_ROUTING_TABLE_INDIRECTLY.getValue());
					result = new Message(head, body);
					serverpeer.sendMessage(destinationPhysicalInfo, result);
				}
			}
	
			//need also to forward to its left child
			if (((nodeInfo.getLogicalInfo().getNumber() % 2) == 0) && (treeNode.getLeftChild() != null)) 
			{
				body = new SPUpdateRoutingTableDirectlyBody(serverpeer.getPhysicalInfo(),
											treeNode.getLogicalInfo(), nodeInfo, 0, true,
											treeNode.getLeftChild().getLogicalInfo());
	
				head.setMsgType(MsgType.SP_UPDATE_ROUTING_TABLE_DIRECTLY.getValue());
				result = new Message(head, body);
				serverpeer.sendMessage(treeNode.getLeftChild().getPhysicalInfo(), result);
			}
	
			//need also to forward to its right child
			if (((nodeInfo.getLogicalInfo().getNumber() % 2) == 1) && (treeNode.getRightChild() != null)) 
			{
				body = new SPUpdateRoutingTableDirectlyBody(serverpeer.getPhysicalInfo(),
											treeNode.getLogicalInfo(), nodeInfo, 0, false,
											treeNode.getRightChild().getLogicalInfo());
	
				head.setMsgType(MsgType.SP_UPDATE_ROUTING_TABLE_DIRECTLY.getValue());
				result = new Message(head, body);
				serverpeer.sendMessage(treeNode.getRightChild().getPhysicalInfo(), result);
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	/**
	 * Delete a node from the system/tree structure
	 * 
	 * @param treeNode information of the node in the tree structure
	 */
	public static void deleteTreeNode(ServerPeer serverpeer, TreeNode treeNode)
	{
		try
		{
			for (int i = 0; i < serverpeer.getListSize(); i++)
			{
				TreeNode temptNode = serverpeer.getListItem(i);
				if (temptNode.getLogicalInfo().equals(treeNode.getLogicalInfo()))
				{
					//stop activate stable position process, should be only one master node
					if (temptNode.getCoOwnerSize() > 0)
					{
						if (temptNode.getRole() == 1)
						{
							serverpeer.stopActivateStablePosition();
							Body body = new SPLBStablePositionBody(serverpeer.getPhysicalInfo(),
										temptNode.getLogicalInfo(),	temptNode.getLogicalInfo());
	
							Head head = new Head();
							head.setMsgType(MsgType.SP_LB_STABLE_POSITION.getValue());
							Message result = new Message(head, body);
							for (int j = 0; j < temptNode.getCoOwnerSize(); j++)
							{
								serverpeer.sendMessage(temptNode.getCoOwnerList(j), result);						
							}
						}
						else
						{
							//temptNode.role == 0
							for (int j = treeNode.getCoOwnerSize() - 1; j >= 0; j--)
							{
								if (treeNode.getCoOwnerList(j).equals(serverpeer.getPhysicalInfo()))
								{
									treeNode.removeCoOwnerList(j);
								}
							}
						}
					}
	
					serverpeer.removeListItem(i);
				}
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	/**
	 * Transfer remaining work of a leaving node to its parent,
	 * may not be necessary later if we remove worklist
	 * 
	 * @param treeNode information of the node in the tree structure
	 * @param replaceString identifier of the node taking charge of the
	 * remaining work (parent of the leaving node)
	 * @return list of remaining work
	 */
	public static Vector<String> getTransferWorkList(TreeNode treeNode, String replaceString)
	{
		Vector<String> result = new Vector<String>();

		for (int i = 0; i < treeNode.getWorkSize(); i++)
		{
			String temp = treeNode.getWork(i);
			if ((temp.indexOf("INSERT") != -1)
				|| (temp.indexOf("DELETE") != -1)
				|| (temp.indexOf("LBGETLOADINFO") != -1)
				|| (temp.indexOf("LBSPLITDATA") != -1)
				|| (temp.indexOf("JOIN") != -1)
				|| (temp.indexOf("LEAVEFINDREPLACEMENTNODE") != -1))
			{
				int lastColon = temp.lastIndexOf(":");
				if (temp.substring(lastColon + 1).equals(treeNode.getLogicalInfo().toString()))
				{
					temp = temp.substring(0, lastColon + 1) + replaceString;
				}
				result.add(temp);
			}
		}

		return result;
	}
	
	/**
	 * transfer fake node to other physical peer node
	 * @param treeNode TreeNode
	 * @param direction boolean
	 */
	public static boolean transferFakeNode(ServerPeer serverpeer, TreeNode treeNode, 
			boolean direction, boolean isAccept, PhysicalInfo physicalSender){
		  
		TreeNode temptNode;
	    int i;

	    try{
	    
	    	if (!direction)
	    	{
	    		for (i = 0; i < serverpeer.getListSize(); i++)
	    		{
	    			temptNode = serverpeer.getListItem(i);

	    			if ((treeNode.getRightAdjacentNode().getLogicalInfo().equals(temptNode.getLogicalInfo())) &&
	    				(!temptNode.getLeftAdjacentNode().getLogicalInfo().equals(treeNode.getLogicalInfo())))
	    			{
	    				//inconsist data
	    				if (isAccept)
	    				{
	    					AdjacentNodeInfo newAdjacent =
	    						new AdjacentNodeInfo(serverpeer.getPhysicalInfo(), treeNode.getLogicalInfo());
	    					temptNode.setLeftAdjacentNode(newAdjacent);
	    				}
	    				else
	    				{
	    					ActivateStablePosition activateStablePositionNode = serverpeer.getActivateStablePosition();
	    					if (activateStablePositionNode != null)
	    					{
	    						if (treeNode.getLogicalInfo().equals(activateStablePositionNode.getTreeNode().getLogicalInfo()))
	    						{
	    							serverpeer.stopActivateStablePosition();
	    						}
	    					}	
	    					treeNode.setRole(0);
	    				
	    					Body tbody = new SPLBRotationPullBody(serverpeer.getPhysicalInfo(), 
	    							treeNode.getLogicalInfo(), treeNode, false);	    				
	    			    	Head thead = new Head();
	    			    	thead.setMsgType(MsgType.SP_LB_ROTATION_PULL.getValue());
		    		    	Message result = new Message(thead, tbody);
		    		    	serverpeer.sendMessage(temptNode.getLeftAdjacentNode().getPhysicalInfo(), result);
	    		    	
		    		    	treeNode.clearCoOwnerList();
		    		    	treeNode.addCoOwnerList(temptNode.getLeftAdjacentNode().getPhysicalInfo());
		    		    	treeNode.deleteAllWork();
		    		    	return false;
		    			}
		    		}
	
		    		if (temptNode.getLeftAdjacentNode().getLogicalInfo().equals(treeNode.getLogicalInfo()))
		    		{
		    			if (temptNode.getRole() == 0)
		    			{
		    				PhysicalInfo coOwner = (PhysicalInfo)temptNode.getCoOwnerList(0);
		    				treeNode.setRole(0);
		    				
		    				Body tbody = new SPLBRotationPullBody(serverpeer.getPhysicalInfo(), 
		    						treeNode.getLogicalInfo(), treeNode, false);	    				
		    		    	Head thead = new Head();
		    		    	thead.setMsgType(MsgType.SP_LB_ROTATION_PULL.getValue());
		    		    	Message result = new Message(thead, tbody);
		    		    	serverpeer.sendMessage(coOwner, result);
		    				
		    		    	treeNode.clearCoOwnerList();
		    		    	treeNode.addCoOwnerList(coOwner);
		    		    	treeNode.deleteAllWork();
		    		    	return false;
		    			}	    			
		    			
		    			else
		    			{
		    				treeNode.setContent(temptNode.getContent());
		    				if (SPGeneralAction.checkRotationPull(temptNode))
		    				{
		    					//1. must update adjacent link
		    					treeNode.setRightAdjacentNode(temptNode.getRightAdjacentNode());
	
		    					//2. delete node
		    					deleteNode(serverpeer, treeNode, temptNode, i, false);
		    				}
		    				else
		    				{
		    					// need to update adjacent links
		    					AdjacentNodeInfo newRightAdjacent =
		    						new AdjacentNodeInfo(temptNode.getRightAdjacentNode().getPhysicalInfo(),
		    								temptNode.getLogicalInfo());
		    					treeNode.setRightAdjacentNode(newRightAdjacent);
		    					if (isAccept)
		    					{	
		    						RoutingItemInfo nodeInfo 
		    							= new RoutingItemInfo(serverpeer.getPhysicalInfo(),
		    									treeNode.getLogicalInfo(), null, null, 
		    									treeNode.getContent().getMinValue(),
		    									treeNode.getContent().getMaxValue());
		    					
		    						Body tbody = new SPUpdateRoutingTableBody(serverpeer.getPhysicalInfo(), 
		    								treeNode.getLogicalInfo(), nodeInfo, treeNode.getParentNode().getLogicalInfo());	    				
		    	    		  		Head thead = new Head();
		    	    		   		thead.setMsgType(MsgType.SP_UPDATE_ROUTING_TABLE.getValue());
		    	    		   		Message result = new Message(thead, tbody);
		    	    		   		serverpeer.sendMessage(treeNode.getParentNode().getPhysicalInfo(), result);
		    					}
		    					
		    					temptNode.setRole(0);
		    					AdjacentNodeInfo newLeftAdjacent =
		    						new AdjacentNodeInfo(serverpeer.getPhysicalInfo(), treeNode.getLogicalInfo());
		    					temptNode.setLeftAdjacentNode(newLeftAdjacent);
		    				
		    					Body tbody = new SPLBRotationPullBody(serverpeer.getPhysicalInfo(), 
	    							temptNode.getLogicalInfo(), temptNode, false);	    				
		    					Head thead = new Head();
		    					thead.setMsgType(MsgType.SP_LB_ROTATION_PULL.getValue());
		    					Message result = new Message(thead, tbody);
		    					serverpeer.sendMessage(temptNode.getRightAdjacentNode().getPhysicalInfo(), result);
			    		   	
			    		   		treeNode.clearCoOwnerList();
			    		   		treeNode.addCoOwnerList(temptNode.getRightAdjacentNode().getPhysicalInfo());
			    		   		treeNode.deleteAllWork();
		    				}
		    				
		    				if (!isAccept)
		    				{
		    					SPGeneralAction.updateRotateRoutingTable(serverpeer, treeNode);
		    					return true;
		    				}
		    				else
		    				{
		    					return false;
		    				}
		    			}	    		
	    			}
		   		}
		    	
		   		//can't find correspondance adjacent node
		   		ActivateStablePosition activateStablePositionNode = serverpeer.getActivateStablePosition();
		   		if (activateStablePositionNode != null)
		    	{
		    		if (treeNode.getLogicalInfo().equals(activateStablePositionNode.getTreeNode().getLogicalInfo()))
		    			serverpeer.stopActivateStablePosition();
		    	}
	
		    	treeNode.setRole(0);
		    		
		    	Body tbody = new SPLBRotationPullBody(serverpeer.getPhysicalInfo(), 
						treeNode.getLogicalInfo(), treeNode, false);	    				
			    Head thead = new Head();
			    thead.setMsgType(MsgType.SP_LB_ROTATION_PULL.getValue());
			    Message result = new Message(thead, tbody);
			    serverpeer.sendMessage(treeNode.getRightAdjacentNode().getPhysicalInfo(), result);
			    	
			    treeNode.clearCoOwnerList();
			    treeNode.addCoOwnerList(treeNode.getRightAdjacentNode().getPhysicalInfo());
			    treeNode.deleteAllWork();
	
			    return false;
		    }
		    	
		    else
		    {
		    	for (i = 0; i < serverpeer.getListSize(); i++)
		    	{
		    		temptNode = serverpeer.getListItem(i);
	
		    		if ((treeNode.getLeftAdjacentNode().getLogicalInfo().equals(temptNode.getLogicalInfo())) &&
		    			(!temptNode.getRightAdjacentNode().getLogicalInfo().equals(treeNode.getLogicalInfo()))) 
		    		{
		    			if (isAccept) 
		    			{
		    				AdjacentNodeInfo newAdjacent =
		    					new AdjacentNodeInfo(serverpeer.getPhysicalInfo(), treeNode.getLogicalInfo());
		    				temptNode.setRightAdjacentNode(newAdjacent);
		    			}
		    			else 
		    			{
		    				ActivateStablePosition activateStablePositionNode = serverpeer.getActivateStablePosition();
		    				if (activateStablePositionNode != null)
		    				{
		    					if (treeNode.getLogicalInfo().equals(activateStablePositionNode.getTreeNode().getLogicalInfo()))
		    					{
		    						serverpeer.stopActivateStablePosition();
		    					}
		    				}
		    				treeNode.setRole(0);
		    				
		    				Body tbody = new SPLBRotationPullBody(serverpeer.getPhysicalInfo(), 
		    						treeNode.getLogicalInfo(), treeNode, true);	    				
		    		    	Head thead = new Head();
		    		    	thead.setMsgType(MsgType.SP_LB_ROTATION_PULL.getValue());
		    		    	Message result = new Message(thead, tbody);
		    		    	serverpeer.sendMessage(temptNode.getRightAdjacentNode().getPhysicalInfo(), result);
		    		    	
		    		    	treeNode.clearCoOwnerList();
		    		    	treeNode.addCoOwnerList(temptNode.getRightAdjacentNode().getPhysicalInfo());
		    		    	treeNode.deleteAllWork();
		    		    	return false;
		    			}
		    		}
	
		    		if (temptNode.getRightAdjacentNode().getLogicalInfo().equals(treeNode.getLogicalInfo())) 
		    		{
		    			if (temptNode.getRole() == 0) 
		    			{
		    				//forward pull request to master node
		    				PhysicalInfo coOwner = (PhysicalInfo)temptNode.getCoOwnerList(0);
		    				treeNode.setRole(0);
		    				
		    				Body tbody = new SPLBRotationPullBody(serverpeer.getPhysicalInfo(), 
		    						treeNode.getLogicalInfo(), treeNode, false);	    				
		    		    	Head thead = new Head();
		    		    	thead.setMsgType(MsgType.SP_LB_ROTATION_PULL.getValue());
		    		    	Message result = new Message(thead, tbody);
		    		    	serverpeer.sendMessage(coOwner, result);
		    				
		    		    	treeNode.clearCoOwnerList();
		    		    	treeNode.addCoOwnerList(coOwner);
		    		    	treeNode.deleteAllWork();
		    		    	return false;
		    			}
		    			else 
		    			{
		    				treeNode.setContent(temptNode.getContent());
		    				if (SPGeneralAction.checkRotationPull(temptNode)) 
		    				{
		    					//can delete tempt node without affecting the tree balance
		    					//1. must update adjacent link
		    					treeNode.setLeftAdjacentNode(temptNode.getLeftAdjacentNode());
	
		    					//2. delete node
		    					deleteNode(serverpeer, treeNode, temptNode, i, true);
		    				}
		    				else 
		    				{
		    					// need to update adjacent links
		    					AdjacentNodeInfo newLeftAdjacent =
		    						new AdjacentNodeInfo(temptNode.getLeftAdjacentNode().getPhysicalInfo(), 
		    								temptNode.getLogicalInfo());
		    					treeNode.setLeftAdjacentNode(newLeftAdjacent);
		    					if (isAccept) 
		    					{	                
		    						RoutingItemInfo nodeInfo
		    							= new RoutingItemInfo(serverpeer.getPhysicalInfo(),
		                                             treeNode.getLogicalInfo(), null, null,
		                                             treeNode.getContent().getMinValue(),
		                                             treeNode.getContent().getMaxValue());
		    						
		    						Body tbody = new SPUpdateRoutingTableBody(serverpeer.getPhysicalInfo(), 
		    								treeNode.getLogicalInfo(), nodeInfo, treeNode.getParentNode().getLogicalInfo());	    				
		    	    		  		Head thead = new Head();
		    	    		   		thead.setMsgType(MsgType.SP_UPDATE_ROUTING_TABLE.getValue());
		    	    		   		Message result = new Message(thead, tbody);
		    	    		   		serverpeer.sendMessage(treeNode.getParentNode().getPhysicalInfo(), result);
		    					}
		    					temptNode.setRole(0);
		              
		    					AdjacentNodeInfo newRightAdjacent =
		    						new AdjacentNodeInfo(serverpeer.getPhysicalInfo(),
		    								treeNode.getLogicalInfo());
		    					temptNode.setRightAdjacentNode(newRightAdjacent);
		    					
		    					Body tbody = new SPLBRotationPullBody(serverpeer.getPhysicalInfo(), 
		    							temptNode.getLogicalInfo(), temptNode, true);	    				
		    					Head thead = new Head();
		    					thead.setMsgType(MsgType.SP_LB_ROTATION_PULL.getValue());
		    					Message result = new Message(thead, tbody);
		    					serverpeer.sendMessage(temptNode.getLeftAdjacentNode().getPhysicalInfo(), result);
			    		   	
			    		   		treeNode.clearCoOwnerList();
			    		   		treeNode.addCoOwnerList(temptNode.getLeftAdjacentNode().getPhysicalInfo());
			    		   		treeNode.deleteAllWork();
			   	            }
		    				
		    				if (!isAccept)
		    				{
		    					SPGeneralAction.updateRotateRoutingTable(serverpeer, treeNode);
		    					return true;
		    				}
		    				else{
		    					return false;
		    				}
		    			}
		    		}
		    	}
		    	
		    	//can't find correspondance adjacent node
		    	ActivateStablePosition activateStablePositionNode = serverpeer.getActivateStablePosition();
		   		if (activateStablePositionNode != null)
		    	{
		    		if (treeNode.getLogicalInfo().equals(activateStablePositionNode.getTreeNode().getLogicalInfo()))
		    			serverpeer.stopActivateStablePosition();
		    	}
		   		
		   		treeNode.setRole(0);
		   		Body tbody = new SPLBRotationPullBody(serverpeer.getPhysicalInfo(), 
						treeNode.getLogicalInfo(), treeNode, true);	    				
			    Head thead = new Head();
			    thead.setMsgType(MsgType.SP_LB_ROTATION_PULL.getValue());
			    Message result = new Message(thead, tbody);
			    serverpeer.sendMessage(treeNode.getLeftAdjacentNode().getPhysicalInfo(), result);
			    	
			    treeNode.clearCoOwnerList();
			    treeNode.addCoOwnerList(treeNode.getLeftAdjacentNode().getPhysicalInfo());
			    treeNode.deleteAllWork();
	
			    return false;	   		
		    }
	    }
	    catch (Exception e)
		{
			e.printStackTrace();
			return false;
		}
	}
	
	/**
	 * delete redundancy node after transfering content during rotation pull process
	 */
	private static void deleteNode(ServerPeer serverpeer, TreeNode pullNode, 
			TreeNode deleteNode, int index, boolean direction){

		//UpdateAdjacentLink updateAdjacentLinkProtocol;
	    AdjacentNodeInfo updateAdjacentInfoToParent, updateAdjacentInfoToAdjacent;
	    //Leave leaveProtocol;
	    ContentInfo content;

	    //UpdateRoutingTableDirectly updateRoutingTableDirectlyProtocol;
	    RoutingTableInfo leftRT = deleteNode.getLeftRoutingTable();
	    RoutingTableInfo rightRT = deleteNode.getRightRoutingTable();
	    RoutingItemInfo temptInfo;
	    int i;

	    //get transfer worklist
	    Vector transferWorkList =
	        SPGeneralAction.getTransferWorkList(deleteNode,
	        		deleteNode.getParentNode().getLogicalInfo().toString());

	    try{
		    if (!direction)
		    {
		    	if (deleteNode.getLogicalInfo().getNumber() % 2 == 0)
		    	{
		    		//pullNode is deleteNode's parent, transfer worklist to pullNode
		    		for (i = 0; i<transferWorkList.size(); i++)
		    			pullNode.putWork((String)transferWorkList.get(i));
	
		    		//2. update adjacent links
		    		pullNode.setRightAdjacentNode(deleteNode.getRightAdjacentNode());
		    		if (deleteNode.getRightAdjacentNode() != null)
		    		{
		    			updateAdjacentInfoToAdjacent =
		    				new AdjacentNodeInfo(serverpeer.getPhysicalInfo(), 
		    						pullNode.getLogicalInfo());
		          
		    			Body tbody = new SPUpdateAdjacentLinkBody(serverpeer.getPhysicalInfo(), 
								pullNode.getLogicalInfo(), updateAdjacentInfoToAdjacent, false,
								deleteNode.getRightAdjacentNode().getLogicalInfo());	    			
					    Head thead = new Head();
					    thead.setMsgType(MsgType.SP_UPDATE_ADJACENT_LINK.getValue());
					    Message result = new Message(thead, tbody);
					    serverpeer.sendMessage(deleteNode.getRightAdjacentNode().getPhysicalInfo(), result);				    
		    		}
	
		    		pullNode.setRightChild(null);
		    		
		    		//can be eliminated as there are lbrotateupdateroutingtable msgs
		    		SPGeneralAction.updateRangeValues(serverpeer, pullNode);
		    	}
		    	else
		    	{
		    		//pullNode is deleteNode's left adjacent, delete node must have a right
		    		//adjacent node which is its parent
	
		    		//1. update adjacent links
		    		pullNode.setRightAdjacentNode(deleteNode.getRightAdjacentNode());
	
		    		//2. send leave request to parent's node
		    		updateAdjacentInfoToParent =
		    			new AdjacentNodeInfo(serverpeer.getPhysicalInfo(), 
		    					pullNode.getLogicalInfo());
		    		content = new ContentInfo(deleteNode.getContent().getMaxValue(),
		    				deleteNode.getContent().getMaxValue(),
		    				deleteNode.getContent().getOrder(),	new Vector());
		        
		    		Body tbody1 = new SPLeaveBody(serverpeer.getPhysicalInfo(), 
		    				deleteNode.getLogicalInfo(), content, 
		    				updateAdjacentInfoToParent, false, transferWorkList, 
		    				deleteNode.getParentNode().getLogicalInfo());	    			
				    Head thead1 = new Head();
				    thead1.setMsgType(MsgType.SP_LEAVE.getValue());
				    Message result1 = new Message(thead1, tbody1);
				    serverpeer.sendMessage(deleteNode.getParentNode().getPhysicalInfo(), result1);
		    		
		    		//3. update adjacent link again
				    updateAdjacentInfoToAdjacent = deleteNode.getRightAdjacentNode();
				    
		    		Body tbody2 = new SPUpdateAdjacentLinkBody(serverpeer.getPhysicalInfo(), 
		    				deleteNode.getLogicalInfo(), updateAdjacentInfoToAdjacent, 
		    				true, pullNode.getLogicalInfo());	    			
				    Head thead2 = new Head();
				    thead1.setMsgType(MsgType.SP_UPDATE_ADJACENT_LINK.getValue());
				    Message result2 = new Message(thead2, tbody2);
				    serverpeer.sendMessage(serverpeer.getPhysicalInfo(), result2);	    		
		    	}
		    }
		    else
		    {
		    	pullNode.setLeftAdjacentNode(deleteNode.getLeftAdjacentNode());
		    	if (deleteNode.getLogicalInfo().getNumber() % 2 == 1)
		    	{
		    		//pullNode is deleteNode's parent, transfer worklist to pullNode
		    		for (i = 0; i<transferWorkList.size(); i++)
		    			pullNode.putWork((String)transferWorkList.get(i));
	
		    		//1. update adjacent links
		    		pullNode.setLeftAdjacentNode(deleteNode.getLeftAdjacentNode());
		    		if (deleteNode.getLeftAdjacentNode() != null)
		    		{
		    			updateAdjacentInfoToAdjacent =
		    				new AdjacentNodeInfo(serverpeer.getPhysicalInfo(), pullNode.getLogicalInfo());
		    			
		    			Body tbody = new SPUpdateAdjacentLinkBody(serverpeer.getPhysicalInfo(), 
		    					pullNode.getLogicalInfo(), updateAdjacentInfoToAdjacent, true,
	                            deleteNode.getLeftAdjacentNode().getLogicalInfo());	    			
					    Head thead = new Head();
					    thead.setMsgType(MsgType.SP_UPDATE_ADJACENT_LINK.getValue());
					    Message result = new Message(thead, tbody);
					    serverpeer.sendMessage(deleteNode.getLeftAdjacentNode().getPhysicalInfo(), result);
		    		}
	
		    		//2. no need to send leave request
		    		pullNode.setLeftChild(null);
		    		//can be eliminated as there are lbrotateupdateroutingtable msgs
		    		SPGeneralAction.updateRangeValues(serverpeer, pullNode);
		    	}
		    	else
		    	{
		    		//pullNode is deleteNode's right adjacent, delete node must have a left
		    		//adjacent node which is its parent
	
		    		//1. update adjacent links
		    		pullNode.setLeftAdjacentNode(deleteNode.getLeftAdjacentNode());
	
		    		//2. send leave request to parent's node
		    		updateAdjacentInfoToParent =
		    			new AdjacentNodeInfo(serverpeer.getPhysicalInfo(), 
		    					pullNode.getLogicalInfo());
		    		content = new ContentInfo(deleteNode.getContent().getMinValue(),
		    				deleteNode.getContent().getMinValue(),
		    				deleteNode.getContent().getOrder(),	new Vector());
		    		
		    		Body tbody1 = new SPLeaveBody(serverpeer.getPhysicalInfo(), 
		    				deleteNode.getLogicalInfo(), content, 
		    				updateAdjacentInfoToParent, true, transferWorkList,
	                        deleteNode.getParentNode().getLogicalInfo());	    			
				    Head thead1 = new Head();
				    thead1.setMsgType(MsgType.SP_LEAVE.getValue());
				    Message result1 = new Message(thead1, tbody1);			    
				    serverpeer.sendMessage(deleteNode.getParentNode().getPhysicalInfo(), result1);
				    
				    //3. update adjacent link again
				    updateAdjacentInfoToAdjacent = deleteNode.getLeftAdjacentNode();
				    
				    Body tbody2 = new SPUpdateAdjacentLinkBody(serverpeer.getPhysicalInfo(), 
		    				deleteNode.getLogicalInfo(), updateAdjacentInfoToAdjacent, 
		    				false, pullNode.getLogicalInfo());	    			
				    Head thead2 = new Head();
				    thead1.setMsgType(MsgType.SP_UPDATE_ADJACENT_LINK.getValue());
				    Message result2 = new Message(thead2, tbody2);
				    serverpeer.sendMessage(serverpeer.getPhysicalInfo(), result2);			    
		    	}
		    }
	
		    //4. notify its neighbor nodes about its leaving
		    for (i = 0; i < leftRT.getTableSize(); i++) {
		    	temptInfo = (RoutingItemInfo) leftRT.getRoutingTableNode(i);
		    	if (temptInfo != null) 
		    	{
		    		Body tbody = new SPUpdateRoutingTableDirectlyBody(serverpeer.getPhysicalInfo(), 
		    				deleteNode.getLogicalInfo(), null, i, true, temptInfo.getLogicalInfo());	    			
				    Head thead = new Head();
				    thead.setMsgType(MsgType.SP_UPDATE_ROUTING_TABLE_DIRECTLY.getValue());
				    Message result = new Message(thead, tbody);
				    serverpeer.sendMessage(temptInfo.getPhysicalInfo(), result);			    
		      	}
		    }
		    for (i = 0; i < rightRT.getTableSize(); i++) 
		    {
		    	temptInfo = (RoutingItemInfo) rightRT.getRoutingTableNode(i);
		    	if (temptInfo != null) 
		    	{
		    		Body tbody = new SPUpdateRoutingTableDirectlyBody(serverpeer.getPhysicalInfo(), 
		    				deleteNode.getLogicalInfo(), null, i, false, temptInfo.getLogicalInfo());	    			
				    Head thead = new Head();
				    thead.setMsgType(MsgType.SP_UPDATE_ROUTING_TABLE_DIRECTLY.getValue());
				    Message result = new Message(thead, tbody);
				    serverpeer.sendMessage(temptInfo.getPhysicalInfo(), result);
		    	}
		    }
	
		    //5. stop activate stable position process, should be only one master node
		    //and delete node
		    if (deleteNode.getCoOwnerSize() > 0)
		    {
		    	serverpeer.stopActivateStablePosition();
		    	Body tbody = new SPLBStablePositionBody(serverpeer.getPhysicalInfo(), 
		    			deleteNode.getLogicalInfo(), deleteNode.getLogicalInfo());	    			
			    Head thead = new Head();
			    thead.setMsgType(MsgType.SP_LB_STABLE_POSITION.getValue());
			    Message result = new Message(thead, tbody);
			    for (int j = 0; j < deleteNode.getCoOwnerSize(); j++) 
			    {
			    	serverpeer.sendMessage((PhysicalInfo)deleteNode.getCoOwnerList(j), result);
			    }
		    }
		    
		    serverpeer.removeListItem(index);
	    }
	    catch (Exception e)
		{
			e.printStackTrace();			
		}
	}
	
	/**
	 * Send requests to lookup information of the failed node to other nodes
	 * 
	 * Algorithm: if it is a parent node, starts recovery process
	 * Otherwise, notify the parent of the failed node
	 * Messages: SPLINeighbor, SPLIChild, SPLIAdjacent
	 * SPLINeighborReply, SPLIChildReply, SPLIAdjacentReply...
	 * First step: only a parent node pings it children
	 * Failed node has no child. Do nothing, load balancing 
	 * can be triggered automatically later
	 * If the failed node has a child, it should have a sibling
	 * The sibling node takes responsible for report child info	 
	 */
	public static void startRecoveryProcess(ServerPeer serverpeer, TreeNode treeNode, LogicalInfo failedNodeInfo)			
	{
		Message result = null;
		Head head = new Head();
		Body body = null;

		PhysicalInfo destinationPhysicalInfo;
		LogicalInfo destinationLogicalInfo;	
		int failedNodeNumber = failedNodeInfo.getNumber();
		int failedNodeLevel = failedNodeInfo.getLevel();
		int leftRTSize = PeerMath.getRoutingTableSize(failedNodeNumber, failedNodeLevel, false);
	    int rightRTSize = PeerMath.getRoutingTableSize(failedNodeNumber, failedNodeLevel, true);
	    RoutingTableInfo leftRoutingTable = new RoutingTableInfo(leftRTSize);
	    RoutingTableInfo rightRoutingTable = new RoutingTableInfo(rightRTSize);
	    AdjacentNodeInfo leftAdjacentNode = null, rightAdjacentNode = null;
		ContentInfo content =
			new ContentInfo(treeNode.getContent().getMinValue(),
					treeNode.getContent().getMaxValue(),
					treeNode.getContent().getOrder(),
					new Vector<IndexValue>());	
		int numOfExpectedRTReply = 0;
		
		try
		{			
			if (failedNodeLevel == 0)
			{
				//special case, the root is failed
				treeNode.getParentNode().setPhysicalInfo(serverpeer.getPhysicalInfo());
				
				if (treeNode.getLogicalInfo().getNumber() % 2 == 0)
				{	
					treeNode.getLeftAdjacentNode().setPhysicalInfo(serverpeer.getPhysicalInfo());
					
					ChildNodeInfo rightChild = 
						new ChildNodeInfo(serverpeer.getPhysicalInfo(),
								treeNode.getLogicalInfo());
					rightAdjacentNode =
						new AdjacentNodeInfo(serverpeer.getPhysicalInfo(),
								treeNode.getLogicalInfo());
					content.setMinValue(new BoundaryValue(IndexValue.MIN_KEY.getString(), Long.MIN_VALUE));
					TreeNode newTreeNode = new TreeNode(failedNodeInfo, null,
							null, rightChild, null, rightAdjacentNode, 
							new RoutingTableInfo(0), new RoutingTableInfo(0), 
							content, 0, TreeNode.ACTIVE);
					serverpeer.addListItem(newTreeNode);
					SPGeneralAction.doLeave(serverpeer, serverpeer.getPhysicalInfo(), treeNode);
					
					//update instance here
				}
				else
				{
					ChildNodeInfo leftChild =
						new ChildNodeInfo(serverpeer.getPhysicalInfo(),
								treeNode.getLogicalInfo());
					ChildNodeInfo rightChild = null;
					
					RoutingItemInfo tempt = treeNode.getRightRoutingTable().getRoutingTableNode(0);
					if (tempt == null)
					{
						treeNode.getRightAdjacentNode().setPhysicalInfo(serverpeer.getPhysicalInfo());
						
						content.setMaxValue(new BoundaryValue(IndexValue.MAX_KEY.getString(), Long.MAX_VALUE));
						TreeNode newTreeNode = new TreeNode(failedNodeInfo, null,
								leftChild, null, null, null, 
								new RoutingTableInfo(0), new RoutingTableInfo(0), 
								content, 0, TreeNode.ACTIVE);
						serverpeer.addListItem(newTreeNode);
						SPGeneralAction.doLeave(serverpeer, serverpeer.getPhysicalInfo(), treeNode);
					}
					else
					{
						rightChild = new ChildNodeInfo(tempt.getPhysicalInfo(), tempt.getLogicalInfo());
				
						if (treeNode.getRightChild() == null)
						{
							treeNode.getRightAdjacentNode().setPhysicalInfo(serverpeer.getPhysicalInfo());
							
							leftAdjacentNode = 
								new AdjacentNodeInfo(serverpeer.getPhysicalInfo(),
								treeNode.getLogicalInfo());
							content.setMinValue(treeNode.getContent().getMaxValue());
						}
						else
						{
							destinationPhysicalInfo = treeNode.getRightChild().getPhysicalInfo();;
							destinationLogicalInfo  = treeNode.getRightChild().getLogicalInfo();;
							
							body = new SPLIAdjacentRootBody(serverpeer.getPhysicalInfo(),
									treeNode.getLogicalInfo(), serverpeer.getPhysicalInfo(),
									failedNodeInfo, SPLIAdjacentRootBody.FROM_RIGHT_TO_LEFT, 
									failedNodeInfo, destinationLogicalInfo);
			
							head.setMsgType(MsgType.SP_LI_ADJACENT_ROOT.getValue());
							result = new Message(head, body);
							serverpeer.sendMessage(destinationPhysicalInfo, result);
						}
						
						if (tempt.getLeftChild() == null)
						{
							rightAdjacentNode = 
								new AdjacentNodeInfo(tempt.getPhysicalInfo(),
								tempt.getLogicalInfo());
							content.setMinValue(tempt.getMaxValue());
						}
						else
						{
							Body tbody1 = new SPLIUpdateParentBody(serverpeer.getPhysicalInfo(),
									treeNode.getLogicalInfo(), serverpeer.getPhysicalInfo(),
									tempt.getLogicalInfo());
							Head thead1 = new Head();
							thead1.setMsgType(MsgType.SP_LI_UPDATE_PARENT.getValue());
							Message result1 = new Message(thead1, tbody1);
							serverpeer.sendMessage(tempt.getPhysicalInfo(), result1);
							
							destinationPhysicalInfo = tempt.getLeftChild().getPhysicalInfo();;
							destinationLogicalInfo  = tempt.getLeftChild().getLogicalInfo();;
							
							body = new SPLIAdjacentRootBody(serverpeer.getPhysicalInfo(),
									treeNode.getLogicalInfo(), serverpeer.getPhysicalInfo(),
									failedNodeInfo, SPLIAdjacentRootBody.FROM_LEFT_TO_RIGHT, 
									failedNodeInfo, destinationLogicalInfo);
			
							head.setMsgType(MsgType.SP_LI_ADJACENT_ROOT.getValue());
							result = new Message(head, body);
							serverpeer.sendMessage(destinationPhysicalInfo, result);
						}
						
						TreeNode newTreeNode = new TreeNode(failedNodeInfo, null,
								leftChild, rightChild, leftAdjacentNode, rightAdjacentNode, 
								new RoutingTableInfo(0), new RoutingTableInfo(0), 
								content, 0, TreeNode.ACTIVE);
						serverpeer.addListItem(newTreeNode);
						if ((leftAdjacentNode != null) && (rightAdjacentNode != null))
						{
							//this node must hold both the root and its left child
							if (SPGeneralAction.checkRotationPull(treeNode))
							{
								SPGeneralAction.doLeave(serverpeer, serverpeer.getPhysicalInfo(), treeNode);
						    }
							else{
								SPGeneralAction.doFindReplacement(serverpeer, serverpeer.getPhysicalInfo(), treeNode);
						    }
							//update instance
						}
					}
				}
			}
			else
			{
				//the failed node is not the root
				//lookup routing table information
				for (int i = 0; i < treeNode.getLeftRoutingTable().getTableSize(); i++) 
				{				
					if (treeNode.getLeftRoutingTable().getRoutingTableNode(i) != null)
					{	
						numOfExpectedRTReply++;
						destinationPhysicalInfo = treeNode.getLeftRoutingTable().getRoutingTableNode(i).getPhysicalInfo();
						destinationLogicalInfo = treeNode.getLeftRoutingTable().getRoutingTableNode(i).getLogicalInfo();
						
						body = new SPLIRoutingTableBody(serverpeer.getPhysicalInfo(),
								treeNode.getLogicalInfo(), serverpeer.getPhysicalInfo(), 
								failedNodeInfo, failedNodeInfo, 
								i, SPLIRoutingTableBody.FROM_RIGHT_TO_LEFT, 
								destinationLogicalInfo);							
		
						head.setMsgType(MsgType.SP_LI_ROUTING_TABLE.getValue());
						result = new Message(head, body);
						serverpeer.sendMessage(destinationPhysicalInfo, result);
					}
				}
				
				for (int i = 0; i < treeNode.getRightRoutingTable().getTableSize(); i++) 
				{
					if (treeNode.getRightRoutingTable().getRoutingTableNode(i) != null) 
					{
						numOfExpectedRTReply++;
						destinationPhysicalInfo = treeNode.getRightRoutingTable().getRoutingTableNode(i).getPhysicalInfo();;
						destinationLogicalInfo  = treeNode.getRightRoutingTable().getRoutingTableNode(i).getLogicalInfo();;
						
						body = new SPLIRoutingTableBody(serverpeer.getPhysicalInfo(),
								treeNode.getLogicalInfo(), serverpeer.getPhysicalInfo(),
								failedNodeInfo, failedNodeInfo, 
								i, SPLIRoutingTableBody.FROM_LEFT_TO_RIGHT, 
								destinationLogicalInfo);
		
						head.setMsgType(MsgType.SP_LI_ROUTING_TABLE.getValue());
						result = new Message(head, body);
						serverpeer.sendMessage(destinationPhysicalInfo, result);
					}
				}
		
				if (((failedNodeInfo.getNumber() % 2) == 0)) 
				{
					//failed node is the right child
					
					//update itself				
					treeNode.getRightChild().setPhysicalInfo(serverpeer.getPhysicalInfo());
					
					if (treeNode.getLeftChild() != null)
					{
						numOfExpectedRTReply++;
						destinationPhysicalInfo = treeNode.getLeftChild().getPhysicalInfo();;
						destinationLogicalInfo  = treeNode.getLeftChild().getLogicalInfo();;
						
						body = new SPLIRoutingTableBody(serverpeer.getPhysicalInfo(),
								treeNode.getLogicalInfo(), serverpeer.getPhysicalInfo(),
								failedNodeInfo, failedNodeInfo, 
								0, SPLIRoutingTableBody.FROM_RIGHT_TO_LEFT, 
								destinationLogicalInfo);
		
						head.setMsgType(MsgType.SP_LI_ROUTING_TABLE.getValue());
						result = new Message(head, body);
						serverpeer.sendMessage(destinationPhysicalInfo, result);
						
						if (treeNode.getRightAdjacentNode().getLogicalInfo().equals(failedNodeInfo))
						{
							//left adjacent node of the failed node is its parent
							leftAdjacentNode =
								new AdjacentNodeInfo(serverpeer.getPhysicalInfo(),
										treeNode.getLogicalInfo());	
						}
						else
						{
							//send a request to find a left adjacent node
							if (treeNode.getRightAdjacentNode() != null)
							{
								numOfExpectedRTReply++;
								destinationPhysicalInfo = treeNode.getRightAdjacentNode().getPhysicalInfo();
								destinationLogicalInfo = treeNode.getRightAdjacentNode().getLogicalInfo();
								
								body = new SPLIAdjacentBody(serverpeer.getPhysicalInfo(),
										treeNode.getLogicalInfo(), serverpeer.getPhysicalInfo(),
										failedNodeInfo, SPLIAdjacentBody.FROM_RIGHT_TO_LEFT,
										failedNodeInfo, destinationLogicalInfo);
								
								head.setMsgType(MsgType.SP_LI_ADJACENT.getValue());
								result = new Message(head, body);
								serverpeer.sendMessage(destinationPhysicalInfo, result);
							}
						}
					}
					else
					{
						//without full children, left adjacent node of the
						//failed node has to be its parent
						leftAdjacentNode =
							new AdjacentNodeInfo(serverpeer.getPhysicalInfo(),
									treeNode.getLogicalInfo());					
					}
					
					if (treeNode.getParentNode() != null)
					{
						numOfExpectedRTReply++;
						destinationPhysicalInfo = treeNode.getParentNode().getPhysicalInfo();
						destinationLogicalInfo = treeNode.getParentNode().getLogicalInfo();
						
						body = new SPLIAdjacentBody(serverpeer.getPhysicalInfo(),
								treeNode.getLogicalInfo(), serverpeer.getPhysicalInfo(),
								failedNodeInfo, SPLIAdjacentBody.FROM_LEFT_TO_RIGHT,
								failedNodeInfo, destinationLogicalInfo);
						
						head.setMsgType(MsgType.SP_LI_ADJACENT.getValue());
						result = new Message(head, body);
						serverpeer.sendMessage(destinationPhysicalInfo, result);
					}				 
				}
				else
				{
					//failed node is the left child
					
					//update itself				
					treeNode.getLeftChild().setPhysicalInfo(serverpeer.getPhysicalInfo());
					
					if (treeNode.getRightChild() != null){
						numOfExpectedRTReply++;
						destinationPhysicalInfo = treeNode.getRightChild().getPhysicalInfo();;
						destinationLogicalInfo  = treeNode.getRightChild().getLogicalInfo();;
							
						body = new SPLIRoutingTableBody(serverpeer.getPhysicalInfo(),
								treeNode.getLogicalInfo(), serverpeer.getPhysicalInfo(),
								failedNodeInfo, failedNodeInfo, 
								0, SPLIRoutingTableBody.FROM_LEFT_TO_RIGHT, 
								destinationLogicalInfo);
			
						head.setMsgType(MsgType.SP_LI_ROUTING_TABLE.getValue());
						result = new Message(head, body);
						serverpeer.sendMessage(destinationPhysicalInfo, result);	
						
						if (treeNode.getLeftAdjacentNode().getLogicalInfo().equals(failedNodeInfo))
						{
							//right adjacent node of the failed node is its parent
							rightAdjacentNode =
									new AdjacentNodeInfo(serverpeer.getPhysicalInfo(),
										treeNode.getLogicalInfo());	
						}
						else
						{
							//send a request to find a right adjacent node
							if (treeNode.getLeftAdjacentNode() != null)
							{
								numOfExpectedRTReply++;
								destinationPhysicalInfo = treeNode.getLeftAdjacentNode().getPhysicalInfo();
								destinationLogicalInfo = treeNode.getLeftAdjacentNode().getLogicalInfo();
								
								body = new SPLIAdjacentBody(serverpeer.getPhysicalInfo(),
										treeNode.getLogicalInfo(), serverpeer.getPhysicalInfo(),
										failedNodeInfo, SPLIAdjacentBody.FROM_LEFT_TO_RIGHT,
										failedNodeInfo, destinationLogicalInfo);
								
								head.setMsgType(MsgType.SP_LI_ADJACENT.getValue());
								result = new Message(head, body);
								serverpeer.sendMessage(destinationPhysicalInfo, result);
							}
						}
					}	
					else
					{
						//without full children, right adjacent node of the
						//failed node has to be its parent
						rightAdjacentNode =
							new AdjacentNodeInfo(serverpeer.getPhysicalInfo(),
									treeNode.getLogicalInfo());
					}
					
					if (treeNode.getParentNode() != null)
					{
						numOfExpectedRTReply++;
						destinationPhysicalInfo = treeNode.getParentNode().getPhysicalInfo();
						destinationLogicalInfo = treeNode.getParentNode().getLogicalInfo();
						
						body = new SPLIAdjacentBody(serverpeer.getPhysicalInfo(),
								treeNode.getLogicalInfo(), serverpeer.getPhysicalInfo(),
								failedNodeInfo, SPLIAdjacentBody.FROM_RIGHT_TO_LEFT,
								failedNodeInfo, destinationLogicalInfo);
						
						head.setMsgType(MsgType.SP_LI_ADJACENT.getValue());
						result = new Message(head, body);
						serverpeer.sendMessage(destinationPhysicalInfo, result);
					}	
				}
				
				ParentNodeInfo parentNode = 
					new ParentNodeInfo(serverpeer.getPhysicalInfo(), treeNode.getLogicalInfo());
				TreeNode newTreeNode = new TreeNode(failedNodeInfo, parentNode,
						null, null, leftAdjacentNode, rightAdjacentNode, leftRoutingTable, 
						rightRoutingTable, content, numOfExpectedRTReply, TreeNode.FAILED);
				serverpeer.addListItem(newTreeNode);
				
				if (numOfExpectedRTReply == 0)
				{
					newTreeNode.setStatus(TreeNode.ACTIVE);
					if (SPGeneralAction.checkRotationPull(newTreeNode))
		    		{
		    			SPGeneralAction.doLeave(serverpeer, serverpeer.getPhysicalInfo(), newTreeNode);
		    		}
		    		else{
		    			SPGeneralAction.doFindReplacement(serverpeer, serverpeer.getPhysicalInfo(), newTreeNode);
		    		}
				}
				else{
					ActivateFailedPosition activateFailedPosition =
						new ActivateFailedPosition(newTreeNode, ServerPeer.TIME_TO_RECOVER_FAILED_NODE);
				}	
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	
	/**
	 * Perform a LEAVE without FINDING a replacement node
	 * 
	 * @param treeNode 
	 * @throws Exception
	 */
	public static void doLeave(ServerPeer serverpeer, PhysicalInfo physicalInfo, TreeNode treeNode) throws Exception
	{
	    SPLeaveBody leaveProtocol;
	    SPUpdateRoutingTableDirectlyBody updateRoutingTableDirectlyProtocol;
	    RoutingTableInfo leftRoutingTable = treeNode.getLeftRoutingTable();
	    RoutingTableInfo rightRoutingTable = treeNode.getRightRoutingTable();
	    RoutingItemInfo temptInfo;
	    SPUpdateAdjacentLinkBody updateAdjacentLinkProtocol;
	    AdjacentNodeInfo adjacentInfo;	    
	    Vector<String> transferWorkList;	
	    int i;

	    if (treeNode.getParentNode()==null)
	    {
	    	//it is the root, do nothing
	    	SPGeneralAction.deleteTreeNode(serverpeer, treeNode);
	    	return;
	    }
	    
	    adjacentInfo = new AdjacentNodeInfo(treeNode.getParentNode().getPhysicalInfo(),
	        								treeNode.getParentNode().getLogicalInfo());

	    transferWorkList = SPGeneralAction.getTransferWorkList(treeNode,
	    		treeNode.getParentNode().getLogicalInfo().toString());

	    //1. send update adjacent request to its adjacent node and leave request to its parent
	    if (treeNode.getLogicalInfo().getNumber() % 2 == 0)
	    {
	    	leaveProtocol = new SPLeaveBody(physicalInfo, 
	    			treeNode.getLogicalInfo(), treeNode.getContent(), 
	    			treeNode.getRightAdjacentNode(), true, transferWorkList,
	    			treeNode.getParentNode().getLogicalInfo());
	    	if (treeNode.getRightAdjacentNode()!=null)
	    	{
	    		updateAdjacentLinkProtocol =
	    			new SPUpdateAdjacentLinkBody(physicalInfo,
	    					treeNode.getLogicalInfo(), adjacentInfo, false,
	    					treeNode.getRightAdjacentNode().getLogicalInfo());
	    		Head head = new Head();
				head.setMsgType(MsgType.SP_UPDATE_ADJACENT_LINK.getValue());
				Message sendMessage = new Message(head, updateAdjacentLinkProtocol);
				serverpeer.sendMessage(treeNode.getRightAdjacentNode().getPhysicalInfo(), sendMessage);
	    	}
	    }
	    else
	    {
	    	leaveProtocol = new SPLeaveBody(physicalInfo, 
	    			treeNode.getLogicalInfo(), treeNode.getContent(), 
	    			treeNode.getLeftAdjacentNode(),	false, transferWorkList,
	    			treeNode.getParentNode().getLogicalInfo());
	    	if (treeNode.getLeftAdjacentNode()!=null)
	    	{
	    		updateAdjacentLinkProtocol =
	    			new SPUpdateAdjacentLinkBody(physicalInfo,
	    					treeNode.getLogicalInfo(), adjacentInfo, true,
	    					treeNode.getLeftAdjacentNode().getLogicalInfo());
	    		Head head = new Head();
				head.setMsgType(MsgType.SP_UPDATE_ADJACENT_LINK.getValue());
				Message sendMessage = new Message(head, updateAdjacentLinkProtocol);
				serverpeer.sendMessage(treeNode.getLeftAdjacentNode().getPhysicalInfo(), sendMessage);
	    	}
	    }
	    
	    Head head = new Head();
		head.setMsgType(MsgType.SP_LEAVE.getValue());
		Message sendMessage = new Message(head, leaveProtocol);
		serverpeer.sendMessage(treeNode.getParentNode().getPhysicalInfo(), sendMessage);

	    //2. notify its neighbor nodes
	    for (i=0; i<leftRoutingTable.getTableSize(); i++)
	    {
	    	temptInfo = (RoutingItemInfo)leftRoutingTable.getRoutingTableNode(i);
	    	if (temptInfo!=null)
	    	{
	    		updateRoutingTableDirectlyProtocol =
	    			new SPUpdateRoutingTableDirectlyBody(physicalInfo,
	    					treeNode.getLogicalInfo(), null, i, true,
	    					temptInfo.getLogicalInfo());
	    		Head head1 = new Head();
				head1.setMsgType(MsgType.SP_UPDATE_ROUTING_TABLE_DIRECTLY.getValue());
				Message sendMessage1 = new Message(head1, updateRoutingTableDirectlyProtocol);
				serverpeer.sendMessage(temptInfo.getPhysicalInfo(), sendMessage1);
	    	}
	    }
	    for (i=0; i<rightRoutingTable.getTableSize(); i++)
	    {
	    	temptInfo = (RoutingItemInfo)rightRoutingTable.getRoutingTableNode(i);
	    	if (temptInfo!=null)
	    	{
	    		updateRoutingTableDirectlyProtocol =
	    			new SPUpdateRoutingTableDirectlyBody(physicalInfo,
	    					treeNode.getLogicalInfo(), null, i, false,
	    					temptInfo.getLogicalInfo());
	    		Head head1 = new Head();
				head1.setMsgType(MsgType.SP_UPDATE_ROUTING_TABLE_DIRECTLY.getValue());
				Message sendMessage1 = new Message(head1, updateRoutingTableDirectlyProtocol);
				serverpeer.sendMessage(temptInfo.getPhysicalInfo(), sendMessage1);
	    	}
	    }

	    //3. send stabilization request to slaves if it is master node and there is any slave
	    if (treeNode.getCoOwnerSize() > 0)
	    {
	    	serverpeer.stopActivateStablePosition();
	    	SPLBStablePositionBody lbStablePositionProtocol
	    		= new SPLBStablePositionBody(physicalInfo, 
	    				treeNode.getLogicalInfo(), treeNode.getLogicalInfo());
	    	Head head1 = new Head();
			head1.setMsgType(MsgType.SP_LB_STABLE_POSITION.getValue());
			Message sendMessage1 = new Message(head1, lbStablePositionProtocol);
	    	for (i = 0; i<treeNode.getCoOwnerSize(); i++)
	    	{
	    		serverpeer.sendMessage((PhysicalInfo) treeNode.getCoOwnerList(i), sendMessage1);
	    	}
	    }

	    //4. delete logical node
	    SPGeneralAction.deleteTreeNode(serverpeer, treeNode);
	}
	
	/**
	 * FIND A replacement node for the LEAVING node
	 * 
	 * @param treeNode
	 * @throws Exception
	 */
	public static void doFindReplacement(ServerPeer serverpeer, PhysicalInfo physicalInfo, TreeNode treeNode) throws Exception
	{
		Head head = new Head();
		head.setMsgType(MsgType.SP_LEAVE_FIND_REPLACEMENT_NODE.getValue());
		Body body;		
		
		if ((treeNode.getLeftChild()!=null) || (treeNode.getRightChild()!=null))
		{
			//if the node has at least one child
			if ((treeNode.getLeftAdjacentNode()!=null) && (treeNode.getRightAdjacentNode()!=null))
			{	
				if (treeNode.getLeftAdjacentNode().getLogicalInfo().getLevel() >= treeNode.getRightAdjacentNode().getLogicalInfo().getLevel())
				{
					body = new SPLeaveFindReplacementNodeBody(physicalInfo,
							treeNode.getLogicalInfo(), physicalInfo,
							treeNode.getLogicalInfo(), 
							treeNode.getLeftAdjacentNode().getLogicalInfo());					
					Message message = new Message(head, body);				
					serverpeer.sendMessage(treeNode.getLeftAdjacentNode().getPhysicalInfo(), message);
				}
				else
				{
					body =	new SPLeaveFindReplacementNodeBody(physicalInfo,
							treeNode.getLogicalInfo(), physicalInfo,
							treeNode.getLogicalInfo(), treeNode.getRightAdjacentNode().getLogicalInfo());					
					Message message = new Message(head, body);
					serverpeer.sendMessage(treeNode.getRightAdjacentNode().getPhysicalInfo(), message);
				}
			}
			else if (treeNode.getLeftAdjacentNode()!=null)
			{
				body =	new SPLeaveFindReplacementNodeBody(physicalInfo, 
						treeNode.getLogicalInfo(), physicalInfo, 
						treeNode.getLogicalInfo(), treeNode.getLeftAdjacentNode().getLogicalInfo());				
				Message message = new Message(head, body);
				serverpeer.sendMessage(treeNode.getLeftAdjacentNode().getPhysicalInfo(), message);
			}
			else
			{
				body =	new SPLeaveFindReplacementNodeBody(physicalInfo,
						treeNode.getLogicalInfo(), physicalInfo,
						treeNode.getLogicalInfo(), treeNode.getRightAdjacentNode().getLogicalInfo());
				Message message = new Message(head, body);
				serverpeer.sendMessage(treeNode.getRightAdjacentNode().getPhysicalInfo(), message);
			}
		}
		else{
			//if the node has no child, check routing table
			RoutingItemInfo temptInfo, transferInfo = null;
			int i = 0;
			while ((i < treeNode.getLeftRoutingTable().getTableSize()) && 
					(transferInfo == null))
			{
				temptInfo = treeNode.getLeftRoutingTable().getRoutingTableNode(i);
				if (temptInfo != null)				
					if ((temptInfo.getLeftChild() != null) || 
					    (temptInfo.getRightChild() != null))					
						transferInfo = temptInfo;					
				i++;
			}

			i = 0;
			while ((i < treeNode.getRightRoutingTable().getTableSize()) && 
					(transferInfo == null))
			{
				temptInfo = treeNode.getRightRoutingTable().getRoutingTableNode(i);
				if (temptInfo != null)				
					if ((temptInfo.getLeftChild() != null) || 
						(temptInfo.getRightChild() != null))					
						transferInfo = temptInfo;					
				i++;
			}
			
			if (transferInfo != null)
			{	
				if (transferInfo.getLeftChild() != null)
				{					
					body =	new SPLeaveFindReplacementNodeBody(physicalInfo,
							treeNode.getLogicalInfo(), physicalInfo,
							treeNode.getLogicalInfo(), transferInfo.getLeftChild().getLogicalInfo());					
					Message message = new Message(head, body);
					serverpeer.sendMessage(transferInfo.getLeftChild().getPhysicalInfo(), message);
				}
				else
				{
					body =	new SPLeaveFindReplacementNodeBody(physicalInfo,
							treeNode.getLogicalInfo(), physicalInfo,
							treeNode.getLogicalInfo(), transferInfo.getRightChild().getLogicalInfo());					
					Message message = new Message(head, body);
					serverpeer.sendMessage(transferInfo.getRightChild().getPhysicalInfo(), message);
				}
			}			
		}
	}
	
	/**
	 * implemented by Quang Hieu
	 * get the data index size
	 * @return
	 */
	public static int getDataIndexSize()
	{
		int result = 0;
		
		try
		{
		}
		catch(Exception e)
		{
			System.out.println("Error while getting data index size");
			e.printStackTrace();
		}
		
		return result;
	}
	
	/**
	 * implemented by Quang Hieu
	 * load data from database to content
	 */
	public static Vector<IndexValue> loadData()
	{
		Vector<IndexValue> data = new Vector<IndexValue>();
		
		try
		{
		}
		catch(Exception e)
		{
			System.out.println("Error while loading data from bestpeerindexdb");
			e.printStackTrace();
		}		
		
		return data;
	}
	
	public static void deleteAllData()
	{
		try
		{
		}
		catch(Exception e)
		{
			System.out.println("Error while deleting all data from bestpeerindexdb");
			e.printStackTrace();
		}
	}
	
	/**
	 * implemented by Quang Hieu
	 * save data to database from content
	 */
	public static void saveData(Vector<IndexValue> data)
	{
	}
}
