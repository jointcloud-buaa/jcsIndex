/*
 * @(#) ServerPnPSender.java 1.0 2006-10-16
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */
package edu.buaa.act.jcsindex.global.peer.request;

import edu.buaa.act.jcsindex.global.AbstractInstance;
import edu.buaa.act.jcsindex.global.ServerInstance;
import edu.buaa.act.jcsindex.global.peer.PeerType;
import edu.buaa.act.jcsindex.global.peer.ServerPeer;
import edu.buaa.act.jcsindex.global.peer.info.*;
import edu.buaa.act.jcsindex.global.peer.management.PeerMaintainer;
import edu.buaa.act.jcsindex.global.protocol.MsgType;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The <code>ServerPnPSender</code> is responsible for sending UDP packets with the type of 
 * <code>{@link MsgType#PING}</code> out to bootstrapper, server peers
 * in its routing table and all its client peers.
 * 
 * <p>The method <code>{@link #sendPacket()}</code> must be implemented to send UDP
 * packages to remote peers. To obtain the IP address list of the remote peers to be
 * contacted, the sender needs to use <code>{@link ServerPeer#BOOTSTRAP_SERVER}</code>
 * and <code>{@link ServerPeer#BOOTSTRAP_SERVER_PORT}</code> to find bootstrapper,
 * and <code>TreeNode</code> structure of <code>{@link ServerPeer#getListItem(int)}</code>
 * to find both server peers in its routing table and all its client peers.
 * 
 * @author Xu Linhao
 * @version 1.0 2006-10-9
 * 
 * @see ServerPnPServer
 * @see UDPSender
 */

public class ServerPnPSender extends UDPSender implements PnPListener
{

	// private member
	private final ReentrantLock lock = new ReentrantLock();
	private ServerInstance serverinstance;
	private ServerPeer server;
	private TreeNode treeNode;
	private PeerMaintainer maintainer = PeerMaintainer.getInstance();
	private List<PeerInfo> peerList;
	private List<PeerInfo> backupList;
	
	/**
	 * Construct the UDP sender with specified parameters.
	 * 
	 * @param instance the reference of the <code>AbstractInstance</code>
	 * @param port the port to be used for initializing <code>DatagramSocket</code>
	 * @throws SocketException if cannot initialize <code>DatagramSocket</code> on the specified port
	 */
	public ServerPnPSender(AbstractInstance instance, int port) throws SocketException
	{
		super(instance, port);
		serverinstance = (ServerInstance) instance;
		server = (ServerPeer )serverinstance.peer();
		peerList = Collections.synchronizedList(new ArrayList<PeerInfo>());
		backupList = Collections.synchronizedList(new ArrayList<PeerInfo>());
	}

	/**
	 * Send UDP packets to bootstrapper, all server peers in routing table 
	 * and its client peers.
	 * 
	 * <li>Within system-defined timeout, if no pong message is received from
	 * a peer <code>P</code>, then the following actions should be performed:
	 * 	<ul>
	 * 	<li>action 1: if <code>P</code> is bootstrapper, then the server peer regards 
	 * 		<code>P</code> as failure and broadcasts a troubleshoot message 
	 * 		to all server peers in its routing table and all its client peers, 
	 * 		and finally leaves the system.
	 * 	</li>
	 *  <li>action 2: if <code>P</code> is a server peer, then the server peer removes 
	 *  	<code>P</code>'s information from its <code>PeerMaintainer</code>, 
	 *  	starts network maintenance operation and updates the instance component.
	 *  </li>
	 *  <li>action 3: if <code>P</code> is a client peer, then the server peer removes 
	 *  	<code>P</code>'s information from its <code>PeerMaintainer</code> 
	 *  	and udpates the instance componet.
	 *  </li>
	 *  </ul>
	 * </li>
	 */
	protected void sendPacket() 
	{
		if (debug)
			System.out.println("######################### new round of heartbeat ########################");
		this.removeFailureNodes();
		this.broadcast();
	}

	/**
	 * Remove all nodes who are regarded as failure since
	 * they do not respond within system-defined timeout.
	 */
	private void removeFailureNodes()
	{
		lock.lock();
		try
		{
			if (peerList == null || peerList.isEmpty())
				return;

			PeerInfo info = null;
			int size = peerList.size();
			for (int i = 0; i < size; i++)
			{
				info = peerList.get(i);
				String type = info.getPeerType();
				
				if (debug)
					System.out.println("case 1: " + info.toString() + " is failure because no pong is received within timeout");
//				Serverinstance.inputLog.WriteLog(info.toString() + " is failure because no pong is received within timeout");

				// case 1: if server peer in routing table failure
				if (type.equals(PeerType.SUPERPEER.getValue()))
				{
					//Please get servernode, treeNode to put into this call
					LogicalInfo logicalInfo = lookupLogicalInfo(treeNode, info);
					/*
					 * should test the method in extensive mannner
					 */
//					SPGeneralAction.startRecoveryProcess(this.server, this.treeNode, logicalInfo);
				}
				// case 2: if owned client peer failure
				else if (type.equals(PeerType.CLIENTPEER.getValue()))
				{
					PeerInfo temp = this.maintainer.remove(info);
					//if (temp != null)
						//this.serverinstance.getPane().firePeerTableRowRemoved(temp);
				}
				// case 3: if bootstrapper failure
				else if (type.equals(PeerType.BOOTSTRAP.getValue()))
				{
//					instance.logout(false, false, true);
				}
			}
			peerList.clear();
			backupList.clear();
		}
		finally
		{
			lock.unlock();
		}
	}
	
	/**
	 * Broadcast UDP packets to all server peers and client peers
	 * that are assumed online currently.
	 */
	private void broadcast()
	{
		lock.lock();
		try
		{
			// packet for bootstrapper 
			String pid = PeerType.BOOTSTRAP.getValue(); 
			PeerInfo bootstrapper = new PeerInfo(pid, ServerPeer.BOOTSTRAP_SERVER, ServerPeer.BOOTSTRAP_SERVER_PORT, pid);

			DatagramPacket ping = this.ping();
			ping.setAddress(InetAddress.getByName(ServerPeer.BOOTSTRAP_SERVER));
			ping.setPort(ServerPeer.BOOTSTRAP_SERVER_PORT);
			peerList.add(bootstrapper);
			backupList.add(bootstrapper);
			for (int i = 0; i < TRY_TIME; i++)
			{
				this.datagramSocket.send(ping);
				if (debug)
					System.out.println("case 2: send ping out to " + ping.getAddress().getHostAddress() + " : " + ping.getPort());
			}
			
			// packets for server peers
			// first, get tree nodes of the server peer
			TreeNode node = null;
			TreeNode[] nodes = server.getTreeNodes();
			if (nodes != null)
			{
				NodeInfo nodeInfo = null;
				int size = nodes.length;
				for (int i = 0; i < size; i++)
				{
					node = nodes[i];
					if (node.getRole() == 1)			// if tree node is self
					{
						this.treeNode = node;
						
						// ping parent node
						if (treeNode.getLogicalInfo().getLevel() == 1)
						{
							//if the root has two children, it is responsble of 
							//the left child to detect failure
							if (treeNode.getLogicalInfo().getNumber() == 1)
							{
								nodeInfo = node.getParentNode();
								this.notify(nodeInfo.getPhysicalInfo());
							}
							else
							{
								//the right child only takes this responsibility
								//if the left child of the root doesn't exist
								if (treeNode.getLeftRoutingTable().getRoutingTableNode(0) == null)
								{
									nodeInfo = node.getParentNode();
									this.notify(nodeInfo.getPhysicalInfo());
								}
							}
						}
						
						// ping left child node
						nodeInfo = node.getLeftChild();
						if (nodeInfo != null) { this.notify(nodeInfo.getPhysicalInfo()); }
						
						// ping right child node
						nodeInfo = node.getRightChild();
						if (nodeInfo != null) {	this.notify(nodeInfo.getPhysicalInfo()); }
						
						/* 
						 * It is not necessary to ping every link since the
						 * parent node of the failed node is the one who is 
						 * in charge of failure recovery. It is better to let 
						 * only the parent node detect this failure to simplify 
						 * the system.						 
						// ping left routing table						
						RoutingItemInfo rtItem = null;
						RoutingTableInfo table = node.getLeftRoutingTable();
						int tsize = 0;
						if (table != null)
						{
							tsize = table.getTableSize();
							for (int j = 0; j < tsize; j++)
							{
								rtItem = table.getRoutingTableNode(j);
								if (rtItem != null)
									this.notify(rtItem.getPhysicalInfo());
							}
						}
						// ping right routing table
						table = node.getRightRoutingTable();
						if (table != null)
						{
							tsize = table.getTableSize();
							for (int j = 0; j < tsize; j++)
							{
								rtItem = table.getRoutingTableNode(j);
								if (rtItem != null)
									this.notify(rtItem.getPhysicalInfo());
							}
						}
						*/
						// exit loop
						break;
					}
				}
			}
			
			// packets for client peers
			PeerInfo info = null;
			PeerInfo[] list = null;
			if (maintainer.hasClient())	
			{
				list = maintainer.getClients();
				int size = list.length;
				for (int i = 0; i < size; i++)
				{
					info = list[i];
					peerList.add(info);
					backupList.add(info);
					ping.setAddress(InetAddress.getByName(info.getInetAddress()));
					ping.setPort(info.getPort());
					for (int j = 0; j < TRY_TIME; j++)
					{
						this.datagramSocket.send(ping);
						if (debug)
							System.out.println("case 2: send ping out to " + ping.getAddress().getHostAddress() + " : " + ping.getPort());
					}
				}
			}
		}
		catch (UnknownHostException e) 
		{	/* if error happens, ignore it */
		}
		catch (IOException e)
		{	/* if error happens, ignore it */
		}
		finally
		{
			lock.unlock();
		}
	}
	
	private void notify(final PhysicalInfo physical)
	{
		lock.lock();
		try
		{
			String pid = PeerType.SUPERPEER.getValue();	 // cannot obtain real id, just use type instead
			PeerInfo info = new PeerInfo(pid, physical.getIP(), physical.getPort(), pid);
			peerList.add(info);
			backupList.add(info);

			DatagramPacket ping = this.ping();
			ping.setAddress(InetAddress.getByName(physical.getIP()));
			ping.setPort(physical.getPort());
			for (int j = 0; j <TRY_TIME; j++)
			{
				this.datagramSocket.send(ping);
				if (debug)
					System.out.println("case 2: send ping out to " + ping.getAddress().getHostAddress() + " : " + ping.getPort());
			}
		}
		catch (UnknownHostException e) 
		{	/* if error happens, ignore it */
		}
		catch (IOException e)
		{	/* if error happens, ignore it */
		}
		finally
		{
			lock.unlock();
		}
	}
	
	public void nodeAlive(final PeerInfo info) 
	{
		lock.lock();
		try
		{
			if (debug)
				System.out.println("case 3: receive pong from " + info.toString());
			this.updatePeerList(info);
		}
		finally
		{
			lock.unlock();
		}
	}

	public void nodeFailure(final PeerInfo info) 
	{
		lock.lock();
		try
		{
			if (debug)
				System.out.println("case 4: receive troubleshoot from " + info.toString());
			
			// case 1: if bootstrapper failure
			// if (type.equals(PeerType.BOOTSTRAP.getValue())): sometime the type is wrong, not sure why
			// so, i do hard code here and expect to fix it in the furture
			if (info.getInetAddress().equals(ServerPeer.BOOTSTRAP_SERVER) && (info.getPort() == ServerPeer.BOOTSTRAP_SERVER_PORT))
			{
//				instance.logout(false, false, true);
//				((ServerInstance) instance).updatePane();
			}
			
			// case 2: if either a server peer or a client peer
			// then, use IP and port to get the matched PeerInfo
			this.updatePeerList(info);
			PeerInfo failure = this.match(info);
			if (failure != null)
			{
				String type = failure.getPeerType();
				// case 2.1: if server peers failure
				if (type.equals(PeerType.SUPERPEER.getValue()))
				{
					//Does nothing here, leave request has been done during logout process				
				}
				// case 2.2: if client peers failure
				else if (type.equals(PeerType.CLIENTPEER.getValue()))
				{
					this.maintainer.remove(failure);
					//this.serverinstance.getPane().firePeerTableRowRemoved(failure);
				}
				else if (type.equals(PeerType.BOOTSTRAP.getValue()))
				{
					// processed before, ignore it here
				}
				else
				{
					throw new IllegalArgumentException("Unknown peer type: " + type);
				}
			}
		}
		finally
		{
			lock.unlock();
		}
	}
	
	/**
	 * Update the peer list by removing the informatioin
	 * of the peer who is regarded as alive.
	 */
	private void updatePeerList(final PeerInfo info)
	{
		lock.lock();
		try
		{
			PeerInfo cmp = null;
			int size = peerList.size();
			for (int i = (size - 1); i >= 0; i--)
			{
				cmp = peerList.get(i);
				if (cmp.equalsIgnoreType(info))
					peerList.remove(i);
			}
		}
		finally
		{
			lock.unlock();
		}
	}
	
	/**
	 * Use backup list to find the correct peer information.
	 * 
	 * @param info
	 * @return
	 */
	private PeerInfo match(final PeerInfo info)
	{
		lock.lock();
		try
		{
			PeerInfo cmp = null;
			int size = backupList.size();
			for (int i = (size - 1); i >= 0; i--)
			{
				cmp = backupList.get(i);
				if (cmp.equalsIgnoreType(info))
					return backupList.remove(i);
			}
			return null;
		}
		finally
		{
			lock.unlock();
		}
	}
	
	/**
	 * Return LogicalInfo of a failed super peer from its physical information
	 * @param treeNode
	 * @param peerInfo
	 * @return
	 */
	private LogicalInfo lookupLogicalInfo(TreeNode treeNode, PeerInfo peerInfo)
	{
		ParentNodeInfo parent = treeNode.getParentNode();
		if (parent != null)
		{		 
			if ((parent.getPhysicalInfo().getIP().equals(peerInfo.getInetAddress()))
				&& (parent.getPhysicalInfo().getPort() == peerInfo.getPort()))
				return parent.getLogicalInfo();			
		}
		
		ChildNodeInfo leftChild = treeNode.getLeftChild();
		if (leftChild != null)
		{
			if ((leftChild.getPhysicalInfo().getIP().equals(peerInfo.getInetAddress()))
				&& (leftChild.getPhysicalInfo().getPort() == peerInfo.getPort())) 
				return leftChild.getLogicalInfo();
		}
		
		ChildNodeInfo rightChild = treeNode.getRightChild();
		if (rightChild != null)
		{
			if ((rightChild.getPhysicalInfo().getIP().equals(peerInfo.getInetAddress()))
				&& (rightChild.getPhysicalInfo().getPort() == peerInfo.getPort())) 
				return rightChild.getLogicalInfo();
		}
		
		for (int i = 0; i < treeNode.getLeftRoutingTable().getTableSize(); i++)
		{
			RoutingItemInfo tempt = treeNode.getLeftRoutingTable().getRoutingTableNode(i);
			if (tempt != null)
			{
				if ((tempt.getPhysicalInfo().getIP().equals(peerInfo.getInetAddress()))
					&& (tempt.getPhysicalInfo().getPort() == peerInfo.getPort()))
					return tempt.getLogicalInfo();
			}
		}
		
		for (int i = 0; i < treeNode.getRightRoutingTable().getTableSize(); i++)
		{
			RoutingItemInfo tempt = treeNode.getRightRoutingTable().getRoutingTableNode(i);
			if (tempt != null)
			{
				if ((tempt.getPhysicalInfo().getIP().equals(peerInfo.getInetAddress()))
					&& (tempt.getPhysicalInfo().getPort() == peerInfo.getPort()))
					return tempt.getLogicalInfo();
			}
		}
		
		return null;
	}
}