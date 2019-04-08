/*
 * @(#) SPPassClientBody.java 1.0 2006-3-5
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.protocol.body;

import edu.buaa.act.jcsindex.global.peer.info.LogicalInfo;
import edu.buaa.act.jcsindex.global.peer.info.PeerInfo;
import edu.buaa.act.jcsindex.global.peer.info.PhysicalInfo;

import java.io.Serializable;

/**
 * Implement the message body used for passing client peers
 * when a node leaves the network 
 * 
 * @author Vu Quang Hieu
 * @version 1.0 2006-3-5
 */

public class SPPassClientBody extends Body implements Serializable 
{

	// private members	
	private static final long serialVersionUID = 7496678283095420550L;
	
	private PhysicalInfo physicalSender;
	private LogicalInfo logicalSender;
	private PeerInfo[] attachedPeers;
	private LogicalInfo logicalDestination;

	/**
	 * Construct the message body with specified parameters.
	 * 
	 * @param physicalSender physical address of the sender
	 * @param logicalSender logical address of the sender
	 * @param attachedPeers attached client peers wants to pass
	 * @param logicalDestination logical address of the receiver
	 */
	public SPPassClientBody(PhysicalInfo physicalSender, LogicalInfo logicalSender,
							PeerInfo[] attachedPeers,	LogicalInfo logicalDestination) 
	{
		this.physicalSender = physicalSender;
		this.logicalSender = logicalSender;
		this.attachedPeers = attachedPeers;
		this.logicalDestination = logicalDestination;
	}
	
	/**
	 * Get physical address of the sender
	 * 
	 * @return physical address of the sender
	 */
	public PhysicalInfo getPhysicalSender()
	{
		return this.physicalSender;
	}

	/**
	 * Get logical address of the sender
	 * 
	 * @return logical address of the sender
	 */
	public LogicalInfo getLogicalSender()
	{
		return this.logicalSender;
	}

	/**
	 * Get attached peers
	 * 
	 * @return a vector of attached peers 
	 */
	public PeerInfo[] getAttachedPeers()
	{
		return this.attachedPeers;
	}
	
	/**
	 * Get logical address of the receiver
	 * 
	 * @return logical address of the receiver
	 */
	public LogicalInfo getLogicalDestination()
	{
		return this.logicalDestination;
	}

	/**
	 * Return a readable string for testing or writing in the log file 
	 * 
	 * @return a readable string
	 */
	public String getString()
	{
		String outMsg;

		outMsg = "PASSCLIENT";
		outMsg += "\n\t Physical Sender:" + physicalSender.toString();
		outMsg += "\n\t Logical Sender:" + logicalSender.toString();
		outMsg += "\n\t Attached Peers:";
		int size = attachedPeers.length;
		for (int i = 0; i < size - 1; i++)
		{
			outMsg += attachedPeers[i] + " ";
		}
		outMsg += attachedPeers[size - 1];
		outMsg += "\n\t ";
		outMsg += "\n\t Logical Destination:" + logicalDestination.toString();

		return outMsg;
	}

	@Override
	public String toString()
	{
		String outMsg;

		outMsg = "PASSCLIENT";
		outMsg += ":" + physicalSender.toString();
		outMsg += ":" + logicalSender.toString();
		outMsg += ":";
		int size = attachedPeers.length;
		for (int i = 0; i < size - 1; i++)
		{
			outMsg += attachedPeers[i] + "%";
		}
		outMsg += attachedPeers[size - 1];
		outMsg += ":" + logicalDestination.toString();

		return outMsg;
	}
	
}
