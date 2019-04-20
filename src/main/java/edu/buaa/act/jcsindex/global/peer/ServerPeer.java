/*
 * @(#) ServerPeer.java 1.0 2006-2-1
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.peer;

import edu.buaa.act.jcsindex.global.AbstractInstance;
import edu.buaa.act.jcsindex.global.logging.LogManager;
import edu.buaa.act.jcsindex.global.peer.event.ActivateActiveStatus;
import edu.buaa.act.jcsindex.global.peer.event.ActivateStablePosition;
import edu.buaa.act.jcsindex.global.peer.info.*;
import edu.buaa.act.jcsindex.global.peer.management.PeerMaintainer;
import edu.buaa.act.jcsindex.global.peer.management.ServerEventManager;
import edu.buaa.act.jcsindex.global.peer.request.ServerPnPServer;
import edu.buaa.act.jcsindex.global.peer.request.ServerRequestManager;
import edu.buaa.act.jcsindex.global.protocol.Head;
import edu.buaa.act.jcsindex.global.protocol.Message;
import edu.buaa.act.jcsindex.global.protocol.MsgType;
import edu.buaa.act.jcsindex.global.protocol.body.ForceOutBody;
import edu.buaa.act.jcsindex.global.utils.Inet;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.*;
import java.util.LinkedList;
import java.util.Vector;

/**
 * Implement a super peer.
 * 
 * @author Xu Linhao
 * @version 1.0 2006-2-1
 */

public class ServerPeer extends AbstractPeer 
{

	// public members
	/**
	 * The list of the bootstraps.
	 */
	public static String BOOTSTRAP_SERVER_LIST;
	/**
	 * The IP address of the bootstrap server.
	 */
	public static String BOOTSTRAP_SERVER;
	/**
	 * The port of the bootstrap server used for monitoring network events. 
	 */
	public static int BOOTSTRAP_SERVER_PORT = 30010;
	/**
	 * The signal used for determine whether allows to print debug information. 
	 */
	public static boolean DEBUG = true;
	// for baton stabilization operation
	/**
	 * Time for a node to become stable once it joins/rejoins the system 
	 */
	public static int TIME_TO_STABLE_STATE = 100;
	/**
	 * Time for a node to become stable once it changes the posision
	 */
	public static int TIME_TO_STABLE_POSITION = 200;
	/**
	 * Time for a failed node to be recovered to a stable position
	 */
	public static int TIME_TO_RECOVER_FAILED_NODE = 200;
	/**
	 * Interval time of checking imbalance at a node
	 */
	public static int TIME_TO_CHECK_IMBALANCE = 300;

	// TODO: 先行添加一个serverPort替换 LOCAL_SERVER_PORT,这么做是为了方便调试
	private int serverPort;

	
	// protected members
	/** 
	 * Define the requester manager who is responsible for sending 
	 * all possible requests, e.g., login, logout, and stabilize,
	 * to other super peers or client peers. All request operations
	 * simply invoke the implementation of the <code>ServerRequestManager</code>. 
	 */
	protected ServerRequestManager requestManager;

	
	/* variables used in BATON */
	private int order;
	private LinkedList<TreeNode> nodeList = null;
	private ActivateStablePosition activateStablePosition = null;
	private ActivateActiveStatus activateActiveStatus = null;
	
	/**
	 * Load system-defined value.
	 */
	public static void load()
	{
		BOOTSTRAP_SERVER_LIST = Inet.getInetAddress() + ":spade.ddns.comp.nus.edu.sg";
		BOOTSTRAP_SERVER_PORT = 30010;
		LOCAL_SERVER_PORT = 40000;
		CAPACITY = 10;
		DEBUG = true;
		TIME_TO_STABLE_STATE = 100;
		TIME_TO_STABLE_POSITION = 200;
		TIME_TO_RECOVER_FAILED_NODE = 200;
		TIME_TO_CHECK_IMBALANCE = 300;
	}

	// TODO: 覆盖父类相应方法,便于调试
	public PhysicalInfo getPhysicalInfo() throws UnknownHostException
	{
		String ip = Inet.getInetAddress();
		if (ip == null)
		{
			throw new UnknownHostException("Cannot obtain IP address");
		}
		return (new PhysicalInfo(ip, this.serverPort));
	}
	
	/**
	 * Write user-defined values to file. Notice that this function must be
	 * called after user applies the change.
	 */
	public static void write()
	{
	}

	public ServerPeer(AbstractInstance instance, int serverPort, String peerType)
	{
		super(instance, peerType);
		this.serverPort = serverPort;
		this.order = 10;
		/* init online peer manager now */
		this.peerMaintainer = PeerMaintainer.getInstance();
		/* init request manager */
		this.requestManager = new ServerRequestManager(this);

		// TODO: 添加TreeNode。其中TreeNode的作用还成疑问？我以为每个Instance只需要掌握一个就可以了，但是这里居然是一个List
		// 这里直接把最小值直接设置为0L,最大值设置为Integer.MAX_VALUE
		ContentInfo content = new ContentInfo(new BoundaryValue(IndexValue.MIN_KEY.getString(), 0L), new BoundaryValue(IndexValue.MAX_KEY.getString(), Integer.MAX_VALUE), 10, new Vector<IndexValue>());
		TreeNode treeNode = new TreeNode(new LogicalInfo(0, 1), null, null, null, null, null, new RoutingTableInfo(0), new RoutingTableInfo(0), content, 0, 1);
		addListItem(treeNode);
	}
	
//	public boolean startIndexManager()
//	{
//		try
//		{
//			this.indexManager = new ServerIndexManager(this, new StandardAnalyzer());
//			new Thread(indexManager, "Index Service Manager").start();
//			return true;
//		}
//		catch (Exception e)
//		{
//			return false;
//		}
//	}
	
//	public boolean stopIndexManager()
//	{
//		try
//		{
//			if (this.indexManager != null)
//			{
//				this.indexManager.stop();
//			}
//			return true;
//		}
//		catch (Exception e)
//		{
//			return false;
//		}
//	}
	
//	/**
//	 * Returns the index manager of this peer.
//	 * 
//	 * @return returns the index manager of this peer
//	 */
//	public ServerIndexManager getIndexManager()
//	{
//		return this.indexManager;
//	}

	// ------------ for tcp service ------------------
	
	@Override
	public boolean startEventManager(int port, int capacity) 
	{
		if (!isEventManagerAlive())
		{
			try
			{
				eventManager = new ServerEventManager(instance, port, capacity);
				new Thread(eventManager).start();
				return true;
			}
			catch (Exception e)
			{
				e.printStackTrace();
				return false;
			}
		}
		return true;
	}
	
	// -------------- for udp service --------------------
	
	@Override
	public boolean startUDPServer(int port, int capacity, long period) 
	{
		if (!this.isUDPServerAlive())
		{
			try
			{
				this.udpServer = new ServerPnPServer(instance, port, capacity, period);
				new Thread(udpServer, "UDP Server").start();
				return true;
			}
			catch (Exception e)
			{
				e.printStackTrace();
				return false;
			}
		}
		return true;
	}

	// -------------- for stabilization -----------------
	
	@Override
	public void troubleshoot(boolean toBoot, boolean toServer, boolean toClient) 
	{
//		//otherwise, send troubleshoot message out
//		PeerType type = PeerType.SUPERPEER;
//		DatagramPacket trouble = this.troubleshoot(type, pid);
//		DatagramSocket socket = this.udpServer.getDatagramSocket();
//
//		try
//		{
//			// send trouble shoot to bootstrapper
//			if (toBoot)
//			{
//	            // Added by Quang Hieu, send meaningful messages when possible
//				// Comment these below lines if wanna test failure
//				if (this.performLogoutRequest())
//				{
//					//return;
//				}
//
//				trouble.setAddress(InetAddress.getByName(BOOTSTRAP_SERVER));
//				trouble.setPort(BOOTSTRAP_SERVER_PORT);
//				for (int i = 0; i < TRY_TIME; i++)
//				{
//					socket.send(trouble);
//					if (debug)
//						System.out.println("case 7: send troubleshoot out to " + trouble.getAddress().getHostAddress() + " : " + trouble.getPort());
//				}
//			}
//			// send trouble shoot to server peers
//			if (toServer)
//			{
//				TreeNode node = null;
//				TreeNode[] nodes = getTreeNodes();
//				if (nodes != null)
//				{
//					NodeInfo nodeInfo = null;
//					int size = nodes.length;
//					for (int i = 0; i < size; i++)
//					{
//						node = nodes[i];
//						if (node.getRole() == 1)			// if tree node is self
//						{
//							// ping parent node
//							nodeInfo = node.getParentNode();
//							if (nodeInfo != null) { notify(nodeInfo.getPhysicalInfo(), type, pid); }
//							// ping left child node
//							nodeInfo = node.getLeftChild();
//							if (nodeInfo != null) { notify(nodeInfo.getPhysicalInfo(), type, pid); }
//							// ping right child node
//							nodeInfo = node.getRightChild();
//							if (nodeInfo != null) {	notify(nodeInfo.getPhysicalInfo(), type, pid); }
//							// ping left adjacent node
//							nodeInfo = node.getLeftAdjacentNode();
//							if (nodeInfo != null) {	notify(nodeInfo.getPhysicalInfo(), type, pid); }
//							// ping right adjacent node
//							nodeInfo = node.getRightAdjacentNode();
//							if (nodeInfo != null) {	notify(nodeInfo.getPhysicalInfo(), type, pid); }
//							// ping left routing table
//							RoutingItemInfo rtItem = null;
//							RoutingTableInfo table = node.getLeftRoutingTable();
//							int tsize = 0;
//							if (table != null)
//							{
//								tsize = table.getTableSize();
//								for (int j = 0; j < tsize; j++)
//								{
//									rtItem = table.getRoutingTableNode(j);
//									if (rtItem != null)
//										this.notify(rtItem.getPhysicalInfo(), type, pid);
//								}
//							}
//							// ping right routing table
//							table = node.getRightRoutingTable();
//							if (table != null)
//							{
//								tsize = table.getTableSize();
//								for (int j = 0; j < tsize; j++)
//								{
//									rtItem = table.getRoutingTableNode(j);
//									if (rtItem != null)
//										this.notify(rtItem.getPhysicalInfo(), type, pid);
//								}
//							}
//							// exit loop
//							break;
//						}
//					}
//				}
//			}
//			// send trouble shoot to client peers
//			if (toClient)
//			{
//				PeerInfo info = null;
//				int size = 0;
//				if (this.peerMaintainer.hasClient())
//				{
//					PeerInfo[] clientList = this.peerMaintainer.getClients();
//					size = clientList.length;
//					for (int i = 0; i < size; i++)
//					{
//						info = clientList[i];
//						trouble.setAddress(InetAddress.getByName(info.getInetAddress()));
//						trouble.setPort(info.getPort());
//						for (int j = 0; j < TRY_TIME; j++)
//						{
//							socket.send(trouble);
//							if (debug)
//								System.out.println("case 7: send troubleshoot out to " + trouble.getAddress().getHostAddress() + " : " + trouble.getPort());
//						}
//					}
//				}
//			}
//		}
//		catch (UnknownHostException e)
//		{ /* ignore it */
//		}
//		catch (SocketException e)
//		{ /* ignore it */
//		}
//		catch (IOException e)
//		{ /* ignore it */
//		}
	}
	
	@Override
	public void forceOut(String ip, int port) 
	{
		try
		{
			/* init socket connection with super peer */
			Socket socket = new Socket(ip, port);

			Head head = new Head();
			head.setMsgType(MsgType.FORCE_OUT.getValue());
			Message message = new Message(head, new ForceOutBody());

			/* send force out message to super peer */
			ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
			oos.writeObject(message);
			
			/* add log event */
//			((Servernstance) instance).log(LogEventType.INFORMATION.getValue(), MsgType.description(head.getMsgType()) + " request is sent out...",	socket.getLocalAddress().getHostName(),	socket.getLocalAddress().getHostAddress() + ":" + socket.getLocalPort(), System.getProperty("user.name"));
			LogManager.getLogger(this).info(MsgType.description(head.getMsgType()) + " request is sent out from " + socket.getLocalAddress().getHostAddress() + ":" + socket.getLocalPort());
			
			/* close stream */
			oos.close();
			socket.close();
		}
		catch (Exception e)
		{
			// record exception event
//			((Serverinstance) instance).log(LogEventType.WARNING.getValue(), "Fail to open connection with client peer because:\r\n" + Tools.getException(e), Inet.getInetAddress2().getHostName(), Inet.getInetAddress(), System.getProperty("user.name"));
			LogManager.getLogger(this).error("Fail to open connection to client peer", e);
		}
	}


	private void notify(PhysicalInfo physical, PeerType type, String pid)
	{
		try
		{
			DatagramSocket socket  = this.udpServer.getDatagramSocket();
			DatagramPacket trouble = this.troubleshoot(type, pid);
			trouble.setAddress(InetAddress.getByName(physical.getIP()));
			trouble.setPort(physical.getPort());
			for (int j = 0; j <TRY_TIME; j++)
			{
				socket.send(trouble);
				if (debug)
					System.out.println("case 7: send troubleshoot out to " + trouble.getAddress().getHostAddress() + " : " + trouble.getPort());
			}
		}
		catch (UnknownHostException e) 
		{	/* if error happens, ignore it */
		}
		catch (IOException e)
		{	/* if error happens, ignore it */
		}
	}

	/**
	 * Get the order that is used for determining if perform load balance operation.
	 * 
	 * @return the order
	 */
	public int getOrder()
	{
		return this.order;
	}
	
	/**
	 * Get the size of the container of the tree nodes.
	 * 
	 * @return the size of the container of the tree nodes
	 */
	public synchronized int getListSize()
	{
		return this.nodeList.size();
	}
	
	/**
	 * Returns all tree nodes maintained by the server peer.
	 * 
	 * @return all tree nodes maintained by the server peer
	 */
	public synchronized TreeNode[] getTreeNodes()
	{
		if (nodeList != null)
		{
			TreeNode[] result = new TreeNode[nodeList.size()];
			return (TreeNode[]) nodeList.toArray(result);
		}
		return null;
	}
	
	/**
	 * Add a <code>TreeNode</code> into the nodeList for retrieving.
	 * 
	 * @param treeNode the <code>TreeNode</code>
	 */
	//public synchronized void addListItem(TreeNode treeNode)
	public void addListItem(TreeNode treeNode)
	{
		if (this.nodeList == null) 
			this.nodeList = new LinkedList<TreeNode>();
		this.nodeList.add(treeNode);
		
		this.activateActiveStatus = new ActivateActiveStatus(treeNode, TIME_TO_STABLE_STATE);
	}
	
	/**
	 * Get an instance of <code>TreeNode</code> from the container of the tree nodes.
	 * 
	 * @return the instance of <code>TreeNode</code>
	 */
	public synchronized TreeNode getListItem(int idx)
	{
		if ((idx < 0) || (idx >= nodeList.size()))
			throw new IllegalArgumentException("Out of bound index");
		
		return this.nodeList.get(idx);
	}
	
	/**
	 * Remove an instance of <code>TreeNode from the container of the tree node.
	 * 
	 * @param idx the position of the item to be removed
	 * @return the instance of <code>TreeNode</code> to be removed
	 */
	public synchronized TreeNode removeListItem(int idx)
	{
		if ((idx < 0) || (idx >= nodeList.size()))
			throw new IllegalArgumentException("Out of bound index");
		
		return this.nodeList.remove(idx);
	}
	
	/**
	 * Get the tree node whose logical information equals to
	 * the specified logical information.
	 * 
	 * @param dest the logical information
	 * @return if find a tree node whose logical information equals to
	 * 			the specified logical information, return the <code>TreeNode</code>;
	 * 			otherwise, return <code>null</code>
	 */
	public synchronized TreeNode getTreeNode(LogicalInfo dest)
	{
		/* loop */
		while (nodeList == null)
		{
			//waiting for a new tree node is created			
		}
		while (nodeList.size() == 0)
		{
			//double check
		}
		
		if (dest == null)
		{
			return nodeList.get(0);
		}
		else
		{
			for (int i = 0; i < nodeList.size(); i++)
			{
				TreeNode treeNode = nodeList.get(i);
				if (dest.equals(treeNode.getLogicalInfo()))              
				{
					return treeNode;            
				}
			}
			return null;
		}
	}
	
	/**
	 * Get the instance of <code>ActivateStablePosition</code>.
	 * 
	 * @return the instance of <code>ActivateStablePosition</code>
	 */
	public ActivateStablePosition getActivateStablePosition()
	{
		return this.activateStablePosition;
	}
	
	/**
	 * Set the instance of <code>ActivateStablePosition</code>.
	 * 
	 * @param activateStablePosition
	 */
	public void setActivateStablePosition(ActivateStablePosition activateStablePosition)
	{
		this.activateStablePosition = activateStablePosition;
	}
	
	/**
	 * Stop stabilizing.
	 */
	public void stopActivateStablePosition()
	{
		if (activateStablePosition != null)
		{
			activateStablePosition.stop();
			activateStablePosition = null;
	    }
	}

	
	/**
	 * Perform a SP_JOIN request to the bootstrap server. The method simply
	 * invokes the same function of the <code>ServerRequestManager</code>.
	 *
	 * @param ip the IP address of an online super peer
	 * @param port the port of the online super peer
	 * @return if success, return <code>true</code>;
	 * 			otherwise, return <code>false</code>
	 */
	public boolean performJoinRequest(String ip, int port)
	{
		return requestManager.performJoinRequest(ip, port);
	}
	
	/**
	 * Perform a JOIN_SUCCESS request to the bootstrap server. The method simply 
	 * invokes the same function of the <code>ServerRequestManager</code>.
	 * 
	 * @return if join operation is success, return <code>true</code>;
	 * 			otherwise, return <code>false</code>
	 */
//	public boolean performSuccessJoinRequest()
//	{
//		return requestManager.performSuccessJoinRequest(BOOTSTRAP_SERVER, BOOTSTRAP_SERVER_PORT);
//	}
	
	/**
	 * Perform a JOIN_FAILURE request to the bootstrap server. The method simply 
	 * invokes the same function of the <code>ServerRequestManager</code>.
	 * 
	 * @return if join operation is canceled, return <code>true</code>;
	 * 			otherwise, return <code>false</code>
	 */
//	public boolean performCancelJoinRequest()
//	{
//		return requestManager.performCancelJoinRequest(BOOTSTRAP_SERVER, BOOTSTRAP_SERVER_PORT);
//	}
	
	/**
	 * Perform the SP_STABILIZE request to for all other super peers,
	 * who are in its routing table, to perform stabilization operation.
	 * 
	 */
	public void performStabilizeRequest() 
	{
		// TODO: do a loop here for each super peer
		//requestManager.performStabilizeRequest();
	}

	/**
	 * Perform the REFRESH request to update the status of all
	 * super peers who are in its routing table. If any inconsistency
	 * happens, the super peer will perform statilization operation
	 * to maintain the correctness of the super peer network. 
	 * 
	 */
	public void performRefreshRequest() 
	{
		// TODO: do a loop here for each super peer
		//requestManager.performRefreshRequest();
	}
	
	public ServerRequestManager getServerRequestManager()
	{
		return this.requestManager;
	}
	
//	/**
//	 * added my mihai, june 27th
//	 * @param sourceColumn
//	 * @param targetColumn
//	 * @return
//	 * @see Serverinstance.matchColumns
//	 */
//	public int performColumnMatch(String sourceColumn, String targetColumn){
//		return requestManager.performColumnMatch(sourceColumn, targetColumn);
//	}
//	
//	/**
//	 * added my mihai, june 27th
//	 * @param sourceColumn
//	 * @param targetColumn
//	 * @return
//	 * @see Serverinstance.unmatchColumns
//	 */
//	public int performColumnUnmatch(String sourceColumn, String targetColumn){
//		return requestManager.performColumnUnmatch(sourceColumn, targetColumn);
//	}

	/**
	 * @return the requestManager
	 */
	public ServerRequestManager getRequestManager() {
		return requestManager;
	}

	/**
	 * @param requestManager the requestManager to set
	 */
	public void setRequestManager(ServerRequestManager requestManager) {
		this.requestManager = requestManager;
	}
}