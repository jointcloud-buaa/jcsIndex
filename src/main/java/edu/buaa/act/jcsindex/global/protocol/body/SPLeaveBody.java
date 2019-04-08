/*
 * @(#) SPLeaveBody.java 1.0 2006-2-22
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.protocol.body;

import edu.buaa.act.jcsindex.global.peer.info.AdjacentNodeInfo;
import edu.buaa.act.jcsindex.global.peer.info.ContentInfo;
import edu.buaa.act.jcsindex.global.peer.info.LogicalInfo;
import edu.buaa.act.jcsindex.global.peer.info.PhysicalInfo;

import java.io.Serializable;
import java.util.Vector;

/**
 * Implement the message body used for notify the parent node
 * when a child leaves the network 
 * 
 * @author Vu Quang Hieu
 * @version 1.0 2006-2-22
 */

public class SPLeaveBody extends Body implements Serializable 
{

	// private members
	private static final long serialVersionUID = 2353502829416738203L;
		
	private PhysicalInfo physicalSender;
	private LogicalInfo logicalSender;
	private ContentInfo content;
	private AdjacentNodeInfo adjacentInfo;
	private boolean direction;
	private Vector<String> workList;
	private LogicalInfo logicalDestination;

	/**
	 * Construct the message body with specified parameters.
	 * 
	 * @param physicalSender physical address of the sender
	 * @param logicalSender logical address of the sender
	 * @param content content of the leaving node
	 * @param adjacentInfo information of the leaving node's adjacent 
	 * @param direction direction of sending request
	 * @param workList remaining work list of the leaving node
	 * @param logicalDestination logical address of the receiver
	 */
	public SPLeaveBody(PhysicalInfo physicalSender, LogicalInfo logicalSender,
						ContentInfo content, AdjacentNodeInfo adjacentInfo,
						boolean direction, Vector<String> workList, 
						LogicalInfo logicalDestination) 
	{
		this.physicalSender = physicalSender;
		this.logicalSender = logicalSender;
		this.content = content;
		this.adjacentInfo = adjacentInfo;
		this.direction = direction;
		this.workList = workList;
		this.logicalDestination = logicalDestination;
	}

	/**
	 * Construct the message body with a string value.
	 * 
	 * @param serializeData the string value that contains 
	 * the serialized message body
	 */
	public SPLeaveBody(String serializeData)
	{
		String[] arrData = serializeData.split(":");
		try
		{
			this.physicalSender = new PhysicalInfo(arrData[1]);
			this.logicalSender = new LogicalInfo(arrData[2]);
			this.content = new ContentInfo(arrData[3]);
			if (arrData[4].equals("null"))
			{
				this.adjacentInfo = null;
			}
			else
			{
				this.adjacentInfo = new AdjacentNodeInfo(arrData[4]);
			}
			
			this.direction = Boolean.valueOf(arrData[5]).booleanValue();
			this.workList = new Vector<String>();
			if (!arrData[6].equals("null"))
			{
				String[] arrWork = arrData[6].split("=");
				String temp;
				for (int i=0; i<arrWork.length; i++)
				{
					temp = arrWork[i].replaceAll(";",":");
					this.workList.add(temp);
				}
			}
			this.logicalDestination = new LogicalInfo(arrData[7]);
		}
		catch(Exception e)
		{
			System.out.println("Incorrect serialize data at Leave:" + serializeData);
			System.out.println(e.getMessage());
		}
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
	 * Get content of the leaving node
	 * 
	 * @return content of the leaving node
	 */
	public ContentInfo getContent()
	{
		return this.content;
	}

	/**
	 * Get information of the leaving node's adjacent
	 * 
	 * @return information of the leaving node's adjacent
	 */
	public AdjacentNodeInfo getAdjacentInfo()
	{
		return this.adjacentInfo;
	}

	/**
	 * Get direction of sending request
	 * 
	 * @return direction of sending request
	 */
	public boolean getDirection()
	{
		return this.direction;
	}

	/**
	 * Get remaining work list of the leaving node
	 * 
	 * @return remaining work list of the leaving node
	 */
	public Vector<String> getWorkList()
	{
		return this.workList;
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

		outMsg = "LEAVE";
		outMsg += "\n\t Physical Sender:" + physicalSender.toString();
		outMsg += "\n\t Logical Sender:" + logicalSender.toString();
		outMsg += "\n\t Content:" + content.toString();
		if (adjacentInfo == null)
		{
			outMsg += "\n\t Adjacent Info: null";
		}
		else
		{
			outMsg += "\n\t Adjacent Info:" + adjacentInfo.toString();
		}
		
		outMsg += "\n\t Direction:" + String.valueOf(direction);
		if (workList.size() == 0)
		{
			outMsg += "\n\t Work List: null";
		}
		else
		{
			String temp;
			outMsg += "\n\t Work List: ";
			for (int i = 0; i < workList.size() - 1; i++)
			{
				temp = (String)workList.get(i);
				temp = temp.replaceAll(":",";");
				outMsg += temp + "=";
			}
			temp = (String)workList.get(workList.size()-1);
			temp = temp.replaceAll(":",";");
			outMsg += temp;
		}
		outMsg += "\n\t Logical Destination:" + logicalDestination.toString();

		return outMsg;
	}

	@Override
	public String toString()
	{
		String outMsg;

		outMsg = "LEAVE";
		outMsg += ":" + physicalSender.toString();
		outMsg += ":" + logicalSender.toString();
		outMsg += ":" + content.toString();
		if (adjacentInfo == null)
		{
			outMsg += ":null";
		}
		else
		{
			outMsg += ":" + adjacentInfo.toString();
		}
		
		outMsg += ":" + String.valueOf(direction);
		if (workList.size() == 0)
		{
			outMsg += ":null";
		}
		else
		{
			String temp;
			outMsg += ":";
			for (int i = 0; i < workList.size() - 1; i++)
			{
				temp = (String)workList.get(i);
				temp = temp.replaceAll(":",";");
				outMsg += temp + "=";
			}
			temp = (String)workList.get(workList.size()-1);
			temp = temp.replaceAll(":",";");
			outMsg += temp;
		}
		outMsg += ":" + logicalDestination.toString();

		return outMsg;
	}
	
}
