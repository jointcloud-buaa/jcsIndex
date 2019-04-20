/*
 * @(#) TreeNode.java 1.0 2006-2-7
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.peer.info;

import java.io.Serializable;
import java.util.Vector;

/**
 * Implement the data structure of each BATON node.
 * 
 * @author Vu Quang Hieu
 * @author (Modified by) Xu Linhao
 * @version 1.0 2006-2-7
 */

public class TreeNode implements Serializable 
{

	// public members	
	// constant definition for role
	public final static int SLAVE  = 0;
	public final static int MASTER = 1;
	
	// constant definition for node status
	public final static int INACTIVE = 0;
	public final static int ACTIVE = 1;
	public final static int FAILED = 2;
	
	// private members
	private static final long serialVersionUID = -9049811313225106124L;

	// logical position
	private LogicalInfo logicalInfo;

	// link to parent node
	private ParentNodeInfo parentNode;
	
	// links to child nodes
	private ChildNodeInfo  leftChild;
	private ChildNodeInfo  rightChild;
	
	// links to adjacent nodes
	private AdjacentNodeInfo leftAdjacent;
	private AdjacentNodeInfo rightAdjacent;
	
	// routing table
	private RoutingTableInfo leftRoutingTable;
	private RoutingTableInfo rightRoutingTable;

	// index key set
	private ContentInfo content;

	// remaining work, may be eliminated later
	private Vector<String> workList;

	// status
	private int numOfExpectedRTReply;
	private int status;

	/*
	 * true denoting that the node has already notified its parent
	 * about the imbalance status; otherwise, does not notify its
	 * parent about the imbalance status.
	 */
	private boolean isImbalanceNotification;
	private LogicalInfo missingNode;

	// for doing load balance
	private boolean isInLoadBalanceProcess;
	private int numOfLNElement;
	private int numOfRNElement;
	private int orderOfLN;
	private int orderOfRN;

	// for sharing logical node
	private int role;
	private Vector<PhysicalInfo> coOwnerList;

	// for query statistic
	private int numOfQuery;

	/**
	 * Contruct a tree node.
	 * 
	 * @param logicalInfo the logical information of the node
	 * @param parentNode the parent node
	 * @param leftChild the left child node
	 * @param rightChild the right child node
	 * @param leftAdjacent the left adjacent node
	 * @param rightAdjacent the right adjacent node
	 * @param leftRoutingTable the left routing table
	 * @param rightRoutingTable the right routing table
	 * @param content the index key set
	 * @param numOfExpectedRTReply the number of expected replies for routing table
	 * @param status how many replies are conducted
	 */
	public TreeNode(LogicalInfo logicalInfo, ParentNodeInfo parentNode,
					ChildNodeInfo leftChild, ChildNodeInfo rightChild,
					AdjacentNodeInfo leftAdjacent, AdjacentNodeInfo rightAdjacent,
					RoutingTableInfo leftRoutingTable, RoutingTableInfo rightRoutingTable,
					ContentInfo content, int numOfExpectedRTReply, int status) 
	{
	    this.logicalInfo 		= logicalInfo;
	    this.parentNode 		= parentNode;
	    this.leftChild 			= leftChild;
	    this.rightChild 		= rightChild;
	    this.leftAdjacent 		= leftAdjacent;
	    this.rightAdjacent 		= rightAdjacent;
	    this.leftRoutingTable 	= leftRoutingTable;
	    this.rightRoutingTable 	= rightRoutingTable;
	    this.content 			= content;
	    this.numOfExpectedRTReply = numOfExpectedRTReply;
	    this.status 			= status;
	    this.workList 			= new Vector<String>();
	    this.coOwnerList 		= new Vector<PhysicalInfo>();
	    
	    this.isImbalanceNotification = false;
	    this.isInLoadBalanceProcess  = false;
	    this.role = MASTER;
	    
	    this.numOfQuery = 0;
	}

	/**
	 * Construct a tree node with a serialized string value.
	 * 
	 * @param serializeData the serialized string value
	 */
	public TreeNode(String serializeData)
	{
	    String[] arrData = serializeData.split("-");
	    try
	    {
	    	this.logicalInfo = new LogicalInfo(arrData[0]);
	    	if (arrData[1].equals("null"))
	    		this.parentNode = null;
	    	else
	    		this.parentNode = new ParentNodeInfo(arrData[1]);
	    	
	    	if (arrData[2].equals("null"))
	    		this.leftChild = null;
	    	else
	    		this.leftChild = new ChildNodeInfo(arrData[2]);
	    	
	    	if (arrData[3].equals("null"))
	    		this.rightChild = null;
	    	else
	    		this.rightChild = new ChildNodeInfo(arrData[3]);
	    	
	    	if (arrData[4].equals("null"))
	    		this.leftAdjacent = null;
	    	else
	    		this.leftAdjacent = new AdjacentNodeInfo(arrData[4]);
	    	
	    	if (arrData[5].equals("null"))
	    		this.rightAdjacent = null;
	    	else
	    		this.rightAdjacent = new AdjacentNodeInfo(arrData[5]);
	    	
	    	this.leftRoutingTable  = new RoutingTableInfo(arrData[6]);
	    	this.rightRoutingTable = new RoutingTableInfo(arrData[7]);
	    	
	    	this.workList = new Vector<String>();
	    	if (!arrData[8].equals("null"))
	    	{
	    		String[] arrWork = arrData[8].split("=");
	    		String tempt;
	    		for (int i = 0; i < arrWork.length; i++)
	    		{
	    			tempt = arrWork[i].replaceAll(";",":");
	    			this.workList.add(tempt);
	    		}
	    	}
	    	this.coOwnerList = new Vector<PhysicalInfo>();
	    	if (!arrData[9].equals("null"))
	    	{
	    		String[] arrCoOwner = arrData[9].split("=");
	    		for (int i = 0; i < arrCoOwner.length; i++)
	    			this.coOwnerList.add(new PhysicalInfo(arrCoOwner[i]));
	    	}
	    }
	    catch (Exception e)
	    {
	    	e.printStackTrace();
	    	System.out.println("Incorrect serialize data at TreeNode:" + serializeData);
	    	/*
	    	 * commented by Xu Linhao on 2006-2-7
	    	 * System.out.println(e.getMessage());
	    	 */
	    }
	}

	/**
	 * Get the logical information of the node.
	 * 
	 * @return the logical information of the node
	 */
	public LogicalInfo getLogicalInfo()
	{
	    return this.logicalInfo;
	}

	/**
	 * Set the parent node of the node.
	 * 
	 * @param parentNode the parent node of the node
	 */
	public void setParentNode(ParentNodeInfo parentNode)
	{
	    this.parentNode = parentNode;
	}

	/**
	 * Get the parent node of the node.
	 * 
	 * @return the parent node of the node
	 */
	public ParentNodeInfo getParentNode()
	{
	    return this.parentNode;
	}

	/**
	 * Set the left child node of the node.
	 * 
	 * @param leftChild the left child nod of the node
	 */
	public void setLeftChild(ChildNodeInfo leftChild)
	{
	    this.leftChild = leftChild;
	}

	/**
	 * Get the left child node of the node.
	 * 
	 * @return the left child node of the node
	 */
	public ChildNodeInfo getLeftChild()
	{
	    return this.leftChild;
	}

	/**
	 * Set the right child node of the node.
	 * 
	 * @param rightChild the right child node of the node
	 */
	public void setRightChild(ChildNodeInfo rightChild)
	{
	    this.rightChild = rightChild;
	}

	/**
	 * Get the right child node of the node
	 * 
	 * @return the right child node of the node
	 */
	public ChildNodeInfo getRightChild()
	{
	    return this.rightChild;
	}

	/**
	 * Set the left adjacent node of the node.
	 * 
	 * @param leftAdjacent the left adjacent node of the node
	 */
	public void setLeftAdjacentNode(AdjacentNodeInfo leftAdjacent)
	{
	    this.leftAdjacent = leftAdjacent;
	}

	/**
	 * Get the left adjacent node of the node.
	 * 
	 * @return the left adjacent node of the node
	 */
	public AdjacentNodeInfo getLeftAdjacentNode()
	{
	    return this.leftAdjacent;
	}

	/**
	 * Set the right adjacent node of the node.
	 * 
	 * @param rightAdjacent the right adjacent node of the node
	 */
	public void setRightAdjacentNode(AdjacentNodeInfo rightAdjacent)
	{
	    this.rightAdjacent = rightAdjacent;
	}

	/**
	 * Get the right adjacent node of the node.
	 * 
	 * @return the right adjacent node of the node
	 */
	public AdjacentNodeInfo getRightAdjacentNode()
	{
	    return this.rightAdjacent;
	}

	/**
	 * Get the left routing table of the node.
	 * 
	 * @return the left routing table of the node
	 */
	public RoutingTableInfo getLeftRoutingTable()
	{
	    return this.leftRoutingTable;
	}

	/**
	 * Get the right routing table of the node.
	 * 
	 * @return the right routing table of the node
	 */
	public RoutingTableInfo getRightRoutingTable()
	{
	    return this.rightRoutingTable;
	}

	/**
	 * Set the index key set.
	 * 
	 * @param content the index key set
	 */
	public void setContent(ContentInfo content)
	{
	    this.content = content;
	}

	/**
	 * Get the index key set.
	 * 
	 * @return the index key set
	 */
	public ContentInfo getContent()
	{
	    return this.content;
	}

	/**
	 * Set the number of expected replies for constructing routing table.
	 * 
	 * @param numOfExpectedRTReply the number of expected replies for constructing routing table
	 */
	public synchronized void setNumOfExpectedRTReply(int numOfExpectedRTReply)
	{
	    this.numOfExpectedRTReply = numOfExpectedRTReply;
	}

	/**
	 * Get the number of expected replies for constructing routing table.
	 * 
	 * @return the number of expected replies for constructing routing table
	 */
	public synchronized int getNumOfExpectedRTReply()
	{
	    return this.numOfExpectedRTReply;
	}

	/**
	 * Set the status of the workload at node.
	 * 
	 * @param status the status of the workload at node
	 */
	public void setStatus(int status)
	{
	    this.status = status;
	}

	/**
	 * Get the status of the workload at node.
	 * 
	 * @return the status of the workload at node
	 */
	public int getStatus()
	{
	    return this.status;
	}

	/**
	 * Store the incoming data objects to be handled
	 * into a container.
	 * 
	 * @param incomingData the incoming data objects to be handled
	 */
	public void putWork(String incomingData)
	{
	    workList.add(incomingData);
	}

	/**
	 * Determine if there still have some incoming data objects to be handled.
	 * 
	 * @return <code>true</code> if still have; otherwise, <code>false</code>
	 */
	public boolean hasWork()
	{
	    return !workList.isEmpty();
	}

	/**
	 * Get the first incoming data object to be handled.
	 * 
	 * @return the first incoming data object to be handled
	 */
	public String getWork()
	{
	    String result = (String)workList.remove(0);
	    return result;
	}

	/**
	 * Get the incoming data object to be handled on a specified position.
	 * 
	 * @return the first incoming data object to be handled on a specified position
	 */
	public String getWork(int idx)
	{
	    return workList.get(idx);
	}

	/**
	 * Get the number of incoming data object to be handled.
	 * 
	 * @return the number of incoming data object to be handled
	 */
	public int getWorkSize()
	{
		return workList.size();
	}
	
	/**
	 * Remove all incoming data objects to be handled.
	 */
	public void deleteAllWork()
	{
	    workList.removeAllElements();
	}

	/**
	 * Get the remainder of the incoming data objects to be handled.
	 * 
	 * @return the remainder of the incoming data objects to be handled 
	 */
	public String getRemainWork()
	{
	    String outMsg = "Remain work of node: " + this.logicalInfo.toString() + "\n";
	    for (int i = 0; i < workList.size(); i++)
	    {
	    	outMsg += "\t" + (String)workList.get(i) + "\n";
	    }
	    return outMsg;
	}
	
	/**
	 * Determine if the imbalance status is notified to other nodes.
	 * 
	 * @return <code>true</code> if notify other nodes;
	 * 			otherwise, <code>false</code>
	 */
	public boolean isNotifyImbalance()
	{
		return this.isImbalanceNotification;
	}
	
	/**
	 * Set the status of the notification of the imbalance status.
	 * 
	 * @param bool the status of the notification of the imbalance status
	 */
	public void notifyImbalance(boolean bool)
	{
		this.isImbalanceNotification = bool;
	}
	
	/**
	 * Get the missing node when processing reply messages.
	 * 
	 * @return the missing node when processing reply messages
	 */
	public LogicalInfo getMissingNode()
	{
		return this.missingNode;
	}
	
	/**
	 * Set the missing node when processing reply messages.
	 * 
	 * @param info the missing node when processing reply messages
	 */
	public void setMissingNode(LogicalInfo info)
	{
		this.missingNode = info;
	}
	
	/**
	 * 
	 * 
	 * @return the number of left neighbors
	 */
	public int getLNElement()
	{
		return this.numOfLNElement;
	}
	
	/**
	 * 
	 * 
	 * @param num the number of left neighbors
	 */
	public void setLNElement(int num)
	{
		this.numOfLNElement = num;
	}
	
	/**
	 * 
	 * 
	 * @return the number of right neighbors
	 */
	public int getRNElement()
	{
		return this.numOfRNElement;
	}
	
	/**
	 * 
	 * 
	 * @param num the number of right neighbors
	 */
	public void setRNElement(int num)
	{
		this.numOfRNElement = num;
	}
	
	/**
	 * 
	 * 
	 * @return the balance status of the left neighbor
	 */
	public int getLNOrder()
	{
		return this.orderOfLN;
	}
	
	/**
	 * 
	 * 
	 * @param order the balance status of the left neighbor
	 */
	public void setLNOrder(int order)
	{
		this.orderOfLN = order;
	}
	
	/**
	 * 
	 * 
	 * @return the balance status of the right neighbor
	 */
	public int getRNOrder()
	{
		return this.orderOfRN;
	}
	
	/**
	 * 
	 * 
	 * @param order the balance status of the right neighbor
	 */
	public void setRNOrder(int order)
	{
		this.orderOfRN = order;
	}
	
	/**
	 * Determine if is processing the load balance.
	 * 
	 * @return <code>true</code> if is processing;
	 * 			otherwise, <code>false</code>
	 */
	public boolean isProcessLoadBalance()
	{
		return this.isInLoadBalanceProcess;
	}
	
	/**
	 * Set the status of the process of the load balance at node.
	 * 
	 * @param bool the status of the process of the load balance at node
	 */
	public void processLoadBalance(boolean bool)
	{
		this.isInLoadBalanceProcess = bool;
	}
	
	/**
	 * Get the role of the node.
	 * 
	 * @return the role of the node
	 */
	public int getRole()
	{
		return this.role;
	}
	
	/**
	 * Set the role of the node.
	 * 
	 * @param role the role of the node
	 */
	public void setRole(int role)
	{
		switch (role)
		{
		case MASTER:
			this.role = role;
			break;
			
		case SLAVE:
			this.role = role;
			break;
			
		default:
			throw new IllegalArgumentException("Unknown role type!");
		}
	}
	
	/**
	 * Put an instance of <code>PhysicalInfo</code> into
	 * the coOwner list.
	 * 
	 * @param info the instance of <code>PhysicalInfo</code>
	 */
	public void addCoOwnerList(PhysicalInfo info)
	{
		this.coOwnerList.add(info);
	}
	
	/**
	 * Get the idx-th of member of the coOwner list.
	 * 
	 * @param idx the position of the member
	 * @return if index position is valid, return the instance of <code>PhysicalInfo</code>;
	 * 			otherwise, throw exception
	 */
	public PhysicalInfo getCoOwnerList(int idx)
	{
		if ((idx < 0) || (idx >= coOwnerList.size()))
			throw new IllegalArgumentException("Out of index bound");
		return coOwnerList.get(idx);
	}
	
	/**
	 * Get the size of the coOwner list.
	 * 
	 * @return the size of the coOwner list
	 */
	public int getCoOwnerSize()
	{
		return this.coOwnerList.size();
	}
	
	/**
	 * Remove an item from the coOwner list.
	 * 
	 * @param idx the position of the item to be removed
	 * @return the instance of <code>PhysicalInfo</code> to be removed
	 */
	public PhysicalInfo removeCoOwnerList(int idx)
	{
		if ((idx < 0) || (idx >= coOwnerList.size()))
			throw new IllegalArgumentException("Out of index bound");
		
		return coOwnerList.remove(idx);
	}
	
	/**
	 * Clear the coOwner list.
	 */
	public void clearCoOwnerList()
	{
		this.coOwnerList.clear();
	}
	
	/**
	 * Increase the number of queries to be processed.
	 * 
	 * @param num the number of queries to be processed
	 */
	public void incNumOfQuery(int num)
	{
		this.numOfQuery += num;
	}
	
	/**
	 * Get the number of queries to be processed.
	 * 
	 * @return the number of queries to be processed
	 */
	public int getNumOfQuery()
	{
		return this.numOfQuery;
	}
	
	/**
	 * Clear the number of queries that the node has processed.
	 */
	public void clearNumOfQuery()
	{
		this.numOfQuery = 0;
	}

	@Override
	public String toString()
	{
	    String outMsg;
	    int i;

	    outMsg = logicalInfo.toString();
	    if (parentNode == null)
	    	outMsg += "-null";
	    else
	    	outMsg += "-" + parentNode.toString();

	    if (leftChild == null)
	    	outMsg += "-null";
	    else
	    	outMsg += "-" + leftChild.toString();

	    if (rightChild == null)
	    	outMsg += "-null";
	    else
	    	outMsg += "-" + rightChild.toString();

	    if (leftAdjacent == null)
	    	outMsg += "-null";
	    else
	    	outMsg += "-" + leftAdjacent.toString();

	    if (rightAdjacent == null)
	    	outMsg += "-null";
	    else
	    	outMsg += "-" + rightAdjacent.toString();

	    outMsg += "-" + leftRoutingTable.toString();
	    outMsg += "-" + rightRoutingTable.toString();

	    if (workList.size() == 0)
	    	outMsg += "-null";
	    else
	    {
	    	String tempt;
	    	outMsg += "-";
	    	for (i=0; i<workList.size() - 1; i++)
	    	{
	    		tempt = (String)workList.get(i);
	    		tempt = tempt.replaceAll(":",";");
	    		outMsg += tempt + "=";
	    	}
	    	tempt = (String)workList.get(workList.size()-1);
	    	tempt = tempt.replaceAll(":",";");
	    	outMsg += tempt;
	    }
	    if (coOwnerList.size() == 0)
	    	outMsg += "-null";
	    else
	    {
	    	outMsg += "-";
	    	for (i=0; i<coOwnerList.size() - 1; i++)
	    		outMsg += ((PhysicalInfo)coOwnerList.get(i)).toString() + "=";
	    	outMsg += ((PhysicalInfo)coOwnerList.get(i)).toString();
	    }
	    return outMsg;
	}

	public String formatedInfo() {
		StringBuilder sb = new StringBuilder();
		sb.append("Logical Info: ").append("Level ").append(logicalInfo.getLevel()).append(" Number ").append(logicalInfo.getNumber()).append("\n");
		sb.append("Parent Node：");
		if (parentNode == null) {
			sb.append("No Parent\n");
		} else {
			sb.append(parentNode.formatedInfo()).append("\n");
		}
		sb.append("Left Child: ");
		if (leftChild == null) {
			sb.append("No Left Child\n");
		} else {
			sb.append(leftChild.formatedInfo()).append("\n");
		}
		sb.append("Right Child: ");
		if (rightChild == null) {
			sb.append("No Right Child\n");
		} else {
			sb.append(rightChild.formatedInfo()).append("\n");
		}
		sb.append("Left Adjacent: ");
		if (leftAdjacent == null) {
			sb.append("No Left Adjacent\n");
		} else {
			sb.append(leftAdjacent.formatedInfo()).append("\n");
		}
		sb.append("Right Adjacent: ");
		if (rightAdjacent == null) {
			sb.append("No Right Adjacent\n");
		} else {
			sb.append(rightAdjacent.formatedInfo()).append("\n");
		}
		// TODO: 输出左右路由表
		sb.append("Left Routing Table: \n" );
		sb.append(leftRoutingTable.toString());
		sb.append("\nRight Routing Table: \n");
		sb.append(rightRoutingTable.toString());
		sb.append("\nRange: ");
		sb.append("(").append(content.getMinValue().toString()).append(", ").append(content.getMaxValue().toString()).append(")");
		// TODO: 输出subtree范围
		sb.append("\nSubtree Range: ");
		sb.append("(").append(content.getSubtreeRangeL().toString()).append(", ").append(content.getSubtreeRangeR().toString()).append(")");
		return sb.toString();
	}
}
