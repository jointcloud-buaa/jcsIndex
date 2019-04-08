/*
 * @(#) SPSearchExactBundleBody.java 1.0 2006-3-7
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.protocol.body;

import edu.buaa.act.jcsindex.global.peer.info.IndexValue;
import edu.buaa.act.jcsindex.global.peer.info.LogicalInfo;
import edu.buaa.act.jcsindex.global.peer.info.PhysicalInfo;

import java.io.Serializable;
import java.util.Vector;

/**
 * Implement the message body used for performing a bundle of exact query search.
 * 
 * @author Vu Quang Hieu
 * @version 1.0 2006-3-7
 */

public class SPSearchExactBundleBody extends Body implements Serializable 
{

	// private members
	private static final long serialVersionUID = 5545577609074290263L;

	private PhysicalInfo physicalSender;
	private LogicalInfo logicalSender;
	private PhysicalInfo physicalRequester;
	private LogicalInfo logicalRequester;
	private Vector<IndexValue> data;
	private LogicalInfo logicalDestination;

	/**
	 * Construct the message body with specified parameters.
	 * 
	 * @param physicalSender physical address of the sender
	 * @param logicalSender logical address of the sender
	 * @param physicalRequester physical address of the requester
	 * @param logicalRequester logical address of the requester
	 * @param data searched data value
	 * @param logicalDestination logical address of the receiver
	 */
	public SPSearchExactBundleBody(PhysicalInfo physicalSender, LogicalInfo logicalSender,
							 PhysicalInfo physicalRequester, LogicalInfo logicalRequester,
							 Vector<IndexValue> data, LogicalInfo logicalDestination)
	{
		this.physicalSender = physicalSender;
		this.logicalSender = logicalSender;
		this.physicalRequester = physicalRequester;
		this.logicalRequester = logicalRequester;
		this.data = data;
		this.logicalDestination = logicalDestination;
	}

	/**
	 * Construct the message body with a string value.
	 * 
	 * @param serializeData the string value that contains 
	 * the serialized message body
	 */
	public SPSearchExactBundleBody(String serializeData)
	{
		String[] arrData = serializeData.split(":");
		try
		{
			this.physicalSender = new PhysicalInfo(arrData[1]);
			if (arrData[2].equals("null"))
			{
				this.logicalSender = null;
			}
			else
			{
				this.logicalSender = new LogicalInfo(arrData[2]);
			}
			
			this.physicalRequester = new PhysicalInfo(arrData[3]);
			if (arrData[4].equals("null"))
			{
				this.logicalRequester = null;
			}
			else
			{
				this.logicalRequester = new LogicalInfo(arrData[4]);
			}
			this.data = new Vector<IndexValue>();
			String[] arrTransferedData = arrData[5].split("%");
			for (int i = 0; i < arrTransferedData.length; i++)
			{
				this.data.add(new IndexValue(arrTransferedData[i]));
			}
			if (arrData[6].equals("null"))
			{
				this.logicalDestination = null;
			}
			else
			{
				this.logicalDestination = new LogicalInfo(arrData[6]);
			}
		}
    	catch(Exception e)
    	{
    		System.out.println("Incorrect serialize data at SearchExactBundle:" + serializeData);
    		System.out.println(e.getMessage());
    	}
	}

	/**
	 * Update physican address of the sender
	 * 
	 * @param physicalSender physical address of the sender
	 */
	public void setPhysicalSender(PhysicalInfo physicalSender)
	{
		this.physicalSender = physicalSender;
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
	 * Update logical address of the sender
	 * 
	 * @param logicalSender logical address of the sender
	 */
	public void setLogicalSender(LogicalInfo logicalSender)
	{
		this.logicalSender = logicalSender;
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
	 * Update physical address of the requester
	 * 
	 * @param physicalRequester physical address of the requester
	 */
	public void setPhysicalRequester(PhysicalInfo physicalRequester)
	{
		this.physicalRequester = physicalRequester;	
	}

	/**
	 * Get physical address of the requester
	 * 
	 * @return physical address of the requester
	 */
	public PhysicalInfo getPhysicalRequester()
	{
		return this.physicalRequester;
	}

	/**
	 * Update logical address of the requester
	 * 
	 * @param logicalRequester logical address of the requester
	 */
	public void setLogicalRequester(LogicalInfo logicalRequester)
	{
		this.logicalRequester = logicalRequester;
	}

	/**
	 * Get logical address of the requester
	 * 
	 * @return logical address of the requester
	 */
	public LogicalInfo getLogicalRequester()
	{
		return this.logicalRequester;
	}

	/**
	 * Get searched data
	 * 
	 * @return searched data
	 */
	public Vector<IndexValue> getData()
	{
		return this.data;
	}

	/**
	 * Update logical address of the receiver
	 * 
	 * @param logicalDestination logical address of the receiver
	 */
	public void setLogicalDestination(LogicalInfo logicalDestination)
	{
		this.logicalDestination = logicalDestination;	
	}

	/**
	 * Get logical address of the receiver
	 * 
	 * @return logical addrss of the receiver
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

		outMsg = "SEARCHEXACTBUNDLE";
		outMsg += "\n\t Physical Sender:" + physicalSender.toString();
		if (logicalSender == null)
		{
			outMsg += "\n\t Logical Sender:null";
		}
		else
		{
			outMsg += "\n\t Logical Sender:" + logicalSender.toString();
		}
		
		outMsg += "\n\t Physical Requester:" + physicalRequester.toString();
		if (logicalRequester == null)
		{
			outMsg += "\n\t Logical Requester:null";
		}
		else
		{
			outMsg += "\n\t Logical Requester:" + logicalRequester.toString();
		}
		outMsg += "\n\t Data:";
		for (int i = 0; i < data.size() - 1; i++)
		{
			outMsg += data.get(i) + " ";
		}
		outMsg += data.get(data.size() - 1);
		if (logicalDestination == null)
		{
			outMsg += "\n\t Logical Destination:null";
		}
		else
		{
			outMsg += "\n\t Logical Destination:" + logicalDestination.toString();
		}

		return outMsg;
	}

	@Override
	public String toString()
	{
		String outMsg;

		outMsg = "SEARCHEXACTBUNDLE";
		outMsg += ":" + physicalSender.toString();
		if (logicalSender == null)
		{
			outMsg += ":null";
		}
		else
		{
			outMsg += ":" + logicalSender.toString();
		}
		
		outMsg += ":" + physicalRequester.toString();
		if (logicalRequester == null)
		{
			outMsg += ":null";
		}
		else
		{
			outMsg += ":" + logicalRequester.toString();
		}
		outMsg += ":";
		for (int i = 0; i < data.size() - 1; i++)
		{
			outMsg += data.get(i) + "%";
		}
		outMsg += data.get(data.size() - 1);
		if (logicalDestination == null)
		{
			outMsg += ":null";
		}
		else
		{
			outMsg += ":" + logicalDestination.toString();
		}

		return outMsg;  
	}
	
}
