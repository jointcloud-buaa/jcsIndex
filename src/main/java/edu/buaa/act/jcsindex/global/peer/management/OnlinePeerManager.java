/*
 * @(#) OnlinePeerMgr.java 1.0 2006-1-14
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.peer.management;

import edu.buaa.act.jcsindex.global.peer.PeerType;
import edu.buaa.act.jcsindex.global.peer.info.PeerInfo;

import java.util.Vector;

/**
 * Manage all online peers, including 
 * remove offline peers and add online peers.
 * 
 * @deprecated
 * @author Xu Linhao
 * @version 1.0 2006-1-14
 */

public class OnlinePeerManager
{
	
	// private members
	private static OnlinePeerManager instance;
	
	private Vector<PeerInfo> onlineSuperPeers;
	private Vector<PeerInfo> onlineCommonPeers;
	
	/**
	 * Implement a singleton object.
	 * 
	 * @return the instance of the <code>OnlinePeerMgr</code>
	 */
	public static OnlinePeerManager getInstance()
	{
		if (instance == null)
			instance = new OnlinePeerManager();
		return instance;
	}
	
	/**
	 * Constructor.
	 */
	private OnlinePeerManager()
	{
		onlineSuperPeers  = new Vector<PeerInfo>(50);
		onlineCommonPeers = new Vector<PeerInfo>(50);
	}
	
	/**
	 * Add a peer.
	 * 
	 * @deprecated
	 * @param p a peer's information
	 */
	public synchronized void add(PeerInfo p)
	{
		if (p.getPeerType().equals(PeerType.SUPERPEER.getValue()))
		{
			onlineSuperPeers.add(p);
		}
		else if (p.getPeerType().equals(PeerType.CLIENTPEER.getValue()))
		{
			onlineCommonPeers.add(p);
		}
		else if (p.getPeerType().equals(PeerType.BOOTSTRAP.getValue()))
		{
			// do nothing
		}
		else if (p.getPeerType().equals(PeerType.CERTAUTHORITY.getValue()))
		{
			// do nothing
		}
		else
		{
			throw new IllegalArgumentException("unknown peer type");
		}
	}
	
	/**
	 * Remove a peer
	 * 
	 * @deprecated
	 * @param p a peer's information
	 */
	public synchronized int indexOf(PeerInfo p)
	{
		int idx = -1;
		String type = p.getPeerType();
		if (type.equals(PeerType.SUPERPEER.getValue()))
		{
			for (int i = 0; i < onlineSuperPeers.size(); i++)
			{
				PeerInfo q = (PeerInfo)onlineSuperPeers.get(i);
				if (q.equals(p))
				{
					idx = i;
					break;
				}
			}
			return idx;
		}
		else if (type.equals(PeerType.CLIENTPEER.getValue()))
		{
			for (int i = 0; i < onlineCommonPeers.size(); i++)
			{
				PeerInfo q = (PeerInfo)onlineCommonPeers.get(i);
				if (q.equals(p))
				{
					idx = i;
					break;
				}
			}
			return idx;
		}
		else if (type.equals(PeerType.BOOTSTRAP.getValue()))
		{
			return idx;
		}
		else if (type.equals(PeerType.CERTAUTHORITY.getValue()))
		{
			return idx;
		}
		else
		{
			throw new IllegalArgumentException("unknown peer type");
		}
	}
	
	/**
	 * 
	 * @deprecated
	 * @param p
	 */
	public synchronized void remove(PeerInfo p)
	{
		PeerInfo info;
		String type = p.getPeerType();
		if (type.equals(PeerType.SUPERPEER.getValue()))
		{
			int size = onlineSuperPeers.size();
			for (int i = size - 1; i >= 0; i--)
			{
				info = onlineSuperPeers.get(i);
				if (info.equals(p))
				{
					onlineSuperPeers.remove(i);
					break;
				}
			}
		}
		else if (type.equals(PeerType.CLIENTPEER.getValue()))
		{
			int size = onlineCommonPeers.size();
			for (int i = size - 1; i >= 0; i--)
			{
				info = onlineCommonPeers.get(i);
				if (info.equals(p))
				{
					onlineCommonPeers.remove(i);
					break;
				}
			}
		}
		else if (type.equals(PeerType.BOOTSTRAP.getValue()))
		{
			// do nothing
		}
		else if (type.equals(PeerType.CERTAUTHORITY.getValue()))
		{
			// do nothing
		}
		else
		{
			throw new IllegalArgumentException("unknown peer type");
		}
	}
	
	/**
	 * 
	 * @deprecated
	 * @param idx
	 * @param p
	 */
	public synchronized void remove(int idx, PeerInfo p)
	{
		if (idx != -1)
		{
			String type = p.getPeerType();
			if (type.equals(PeerType.SUPERPEER.getValue()))
			{
				onlineSuperPeers.remove(idx);
			}
			else if (type.equals(PeerType.CLIENTPEER.getValue()))
			{
				onlineCommonPeers.remove(idx);
			}
			else if (type.equals(PeerType.BOOTSTRAP.getValue()))
			{
				// do nothing
			}
			else if (type.equals(PeerType.CERTAUTHORITY.getValue()))
			{
				// do nothing
			}
			else
			{
				throw new IllegalArgumentException("unknown peer type");
			}
		}
	}
	
	/**
	 * 
	 * @deprecated
	 */
	public synchronized void removeAll()
	{
		onlineSuperPeers.removeAllElements();
		onlineCommonPeers.removeAllElements();
	}
	
	/**
	 * Judge whether exist online super peers.
	 * 
	 * @deprecated
	 * @return if have not, return <code>true</code>;
	 * 			otherwise, return <code>false</code>
	 */
	public synchronized boolean hasSuperPeers()
	{
		return !onlineSuperPeers.isEmpty();
	}
	
	/**
	 * Return all super peers.
	 * 
	 * @deprecated
	 * @return all super peers
	 */
	@SuppressWarnings("unchecked")
	public synchronized Vector<PeerInfo> getSuperPeers()
	{
		return (Vector<PeerInfo>) onlineSuperPeers.clone();
	}
	
	/**
	 * Judge whether exist online common peers.
	 * 
	 * @deprecated
	 * @return if have not, return <code>true</code>;
	 * 			otherwise, return <code>false</code>
	 */
	public synchronized boolean hasCommonPeers()
	{
		return !onlineCommonPeers.isEmpty();
	}
	
	/**
	 * Return all common peers.
	 * 
	 * @deprecated
	 * @return all common peers
	 */
	@SuppressWarnings("unchecked")
	public synchronized Vector<PeerInfo> getCommonPeers()
	{
		return (Vector<PeerInfo>) onlineCommonPeers.clone();
	}
	
}