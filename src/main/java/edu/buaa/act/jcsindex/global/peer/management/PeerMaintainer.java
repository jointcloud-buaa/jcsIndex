/*
 * @(#) PeerMaintainer.java 1.0 2006-10-10
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.peer.management;

import edu.buaa.act.jcsindex.global.peer.PeerType;
import edu.buaa.act.jcsindex.global.peer.info.PeerInfo;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * The <code>PeerMaintainer</code> is a singleton class. Its responsiblity
 * is to maintain a list of all online peers for the purpose of monitoring
 * the dynamic behavior of every online peer.
 * 
 * <p>The original <code>OnlinePeerManager</code> is replaced by this class
 * for the reason of efficiency. 
 * 
 * @author Xu Linhao
 * @version 1.0 2006-10-10
 */

public class PeerMaintainer
{
	
	// private member
	/* the reference of the class itself */
	private static PeerMaintainer instance;
	/* a hash table used for keeping all client peers */
	private Map<String,PeerInfo> clientList;
	/* a hash table used for keeping all server peers */
	private Map<String,PeerInfo> serverList;
	
	/**
	 * Returns the reference of the <code>PeerMaintainer</code>.
	 * 
	 * @return the reference of the <code>PeerMaintainer</code>
	 */
	public static PeerMaintainer getInstance()
	{
		if (instance == null)
			instance = new PeerMaintainer();
		return instance;
	}
	
	/**
	 * Construct a <code>PeerMaintainer</code> by avoiding
	 * directly creating the instance. 
	 */
	private PeerMaintainer()
	{
		clientList = Collections.synchronizedMap(new HashMap<String,PeerInfo>(50));
		serverList = Collections.synchronizedMap(new HashMap<String,PeerInfo>(50));
	}
	
	/**
	 * Returns <code>true</code> if a given search key can be found.
	 * 
	 * @param key the search key
	 * @return <code>true</code> if a given search key can be found; 
	 * 		   otherwise, return <code>false</code>
	 */
	public boolean containsKey(String key)
	{
		return (clientList.containsKey(key) || (serverList.containsKey(key)));
	}
	
	/**
	 * Returns <code>true</code> if the key of a given <code>PeerInfo</code>
	 * can be found.
	 * 
	 * @param info a <code>PeerInfo</code> instance
	 * @return <code>true</code> if the key of a given <code>PeerInfo</code>
	 * 		   can be found; otherwise, return <code>false</code>
	 */
	public boolean containsKey(PeerInfo info)
	{
		String key = info.getKey();
		return (clientList.containsKey(key) || serverList.containsKey(key));
	}
	
	/**
	 * Returns <code>true</code> if a given <code>PeerInfo</code> instance can
	 * be found.
	 * 
	 * @param info a <code>PeerInfo</code> instance
	 * @return <code>true</code> if a given <code>PeerInfo</code> instance can
	 * 		   be found; otherwise, return <code>false</code>
	 */
	public boolean containsValue(PeerInfo info)
	{
		return (clientList.containsValue(info) || serverList.containsValue(info));
	}
	
	/**
	 * Puts a <code>PeerInfo</code> instance into the corresponding map.
	 * 
	 * @param info a <code>PeerInfo</code> instance
	 */
	public void put(PeerInfo info)
	{
		String key  = info.getKey();
		this.put(key, info);
	}
	
	/**
	 * Puts a <code>PeerInfo</code> instance into the corresponding map
	 * with specified key.
	 * 
	 * @param key the key to identify the <code>PeerInfo</code> instance
	 * @param info the <code>PeerInfo</code> instance
	 */
	public void put(String key, PeerInfo info)
	{
		String type = info.getPeerType();
		if (type.equalsIgnoreCase(PeerType.CLIENTPEER.getValue()))
		{
			clientList.put(key, info);
		}
		else if (type.equalsIgnoreCase(PeerType.SUPERPEER.getValue()))
		{
			serverList.put(key, info);
		}
	}
	
	/**
	 * Returns a <code>PeerInfo</code> instance whose type is 
	 * client peer and key is equal to the search key.
	 * 
	 * @param key the search key
	 * @return a <code>PeerInfo</code> instance whose key is 
	 * 		   equal to the search key; <code>null</code> if
	 * 		   cannot find the key
	 */
	public PeerInfo getClient(String key)
	{
		return clientList.get(key); 
	}
	
	/**
	 * Returns a collection view of the value stored in the map that 
	 * is responsible for keeping the information of all client peers.
	 * 
	 * @return a <code>PeerInfo</code> array whose values are the view
	 * 		   of the map that stores the information of all client peers
	 */
	public synchronized PeerInfo[] getClients()
	{
		PeerInfo[] result = new PeerInfo[clientList.size()];
		return (PeerInfo[]) clientList.values().toArray(result);
	}

	/**
	 * Returns a <code>PeerInfo</code> instance whose type is 
	 * super peer and key is equal to the search key.
	 * 
	 * @param key the search key
	 * @return a <code>PeerInfo</code> instance whose key is 
	 * 		   equal to the search key; <code>null</code> if
	 * 		   cannot find the key
	 */
	public PeerInfo getServer(String key)
	{
		return serverList.get(key);
	}
	
	/**
	 * Returns a collection view of the value stored in the map that 
	 * is responsible for keeping the information of all super peers.
	 * 
	 * @return a <code>PeerInfo</code> array whose values are the view
	 * 		   of the map that stores the information of all super peers
	 */
	public synchronized PeerInfo[] getServers()
	{
		PeerInfo[] result = new PeerInfo[serverList.size()];
		return serverList.values().toArray(result);
	}
	
	/**
	 * Returns <code>true</code> if the map used for keeping client peers
	 * is not empty.
	 * 
	 * @return <code>true</code> if the map used for keeping client peers
	 * 		   is not empty; otherwise, return <code>false</code>
	 */
	public boolean hasClient()
	{
		return !clientList.isEmpty();
	}
	
	/**
	 * Returns <code>true</code> if the map used for keeping server peers
	 * is not empty.
	 * 
	 * @return <code>true</code> if the map used for keeping server peers
	 * 		   is not empty; otherwise, return <code>false</code>
	 */
	public boolean hasServer()
	{
		return !serverList.isEmpty();
	}
	
	/**
	 * Removes a <code>PeerInfo</code> instance from the map.
	 * 
	 * @param info the <code>PeerInfo</code> instance
	 */
	public PeerInfo remove(PeerInfo info)
	{
		String key = info.getKey();
		String type = info.getPeerType();
		PeerInfo cmp;
		if (type.equalsIgnoreCase(PeerType.CLIENTPEER.getValue()))
		{
			cmp = clientList.get(key);
			if (cmp!= null && cmp.equals(info))
				return clientList.remove(key);
		}
		else if (type.equalsIgnoreCase(PeerType.SUPERPEER.getValue()))
		{
			cmp = serverList.get(key);
			if (cmp != null && cmp.equals(info))
				return serverList.remove(key);
		}
		return null;
	}
	
	/**
	 * Removes all elements in the map. 
	 */
	public void removeAll()
	{
		clientList.clear();
		serverList.clear();
	}
	
}