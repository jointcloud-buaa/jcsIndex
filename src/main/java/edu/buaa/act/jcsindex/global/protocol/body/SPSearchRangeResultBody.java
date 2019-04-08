/*
 * @(#) SPSearchRangeResultBody.java 1.0 2006-2-22
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
 * Implement the message body used for returning the result of the range search.
 * 
 * @author Vu Quang Hieu
 * @version 1.0 2006-2-22
 */

public class SPSearchRangeResultBody extends Body implements Serializable
{

	// private members
	private static final long serialVersionUID = 8743136446386377486L;
	
	private PhysicalInfo physicalSender;
	private LogicalInfo logicalSender;
	private Vector<IndexValue> result;
	private LogicalInfo logicalDestination;

	/**
	 * Construct the message body with specified parameters.
	 * 
	 * @param physicalSender physical address of the sender
	 * @param logicalSender logical address of the sender
	 * @param result result found
	 * @param logicalDestination
	 */
	public SPSearchRangeResultBody(PhysicalInfo physicalSender, LogicalInfo logicalSender,
									Vector<IndexValue> result, LogicalInfo logicalDestination) 
	{
		this.physicalSender = physicalSender;
		this.logicalSender = logicalSender;
		this.result = result;
		this.logicalDestination = logicalDestination;
	}

	/**
	 * Construct the message body with a string value.
	 * 
	 * @param serializeData the string value that contains 
	 * the serialized message body
	 */
	public SPSearchRangeResultBody(String serializeData)
	{
		String[] arrData = serializeData.split(":");
		try
		{
			this.physicalSender = new PhysicalInfo(arrData[1]);
			this.logicalSender = new LogicalInfo(arrData[2]);
			this.result = new Vector<IndexValue>();
			if (!arrData[3].equals("null"))
			{
				String[] arrResult = arrData[3].split("_");
				for (int i = 0; i < arrResult.length; i++)
					this.result.add(new IndexValue(arrResult[i]));
			}
			this.logicalDestination = new LogicalInfo(arrData[4]);
		}
		catch(Exception e)
		{
			System.out.println("Incorrect serialize data at SearchExactResult:" + serializeData);
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
	 * Get result found
	 * 
	 * @return result found
	 */
	public Vector getResult()
	{
		return this.result;
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

		outMsg = "SEARCHRANGERESULT";
		outMsg += "\n\t Physical Sender:" + physicalSender.toString();
		outMsg += "\n\t Logical Sender:" + logicalSender.toString();
		outMsg += "\n\t Result:";
		for (int i = 0; i < this.result.size() - 1; i++)
		{
			outMsg += this.result.get(i) + "_";
		}
		outMsg += ((IndexValue)this.result.get(this.result.size()-1)).toString();
		outMsg += "\n\t Logical Destination:" + logicalDestination.toString();

		return outMsg;
	}

	@Override
	public String toString()
	{
		String outMsg;

		outMsg = "SEARCHRANGERESULT";
		outMsg += ":" + physicalSender.toString();
		outMsg += ":" + logicalSender.toString();
		outMsg += ":";
		for (int i = 0; i < this.result.size() - 1; i++)
		{
			outMsg += this.result.get(i) + "_";
		}
		outMsg += ((IndexValue)this.result.get(this.result.size()-1)).toString();
		outMsg += ":" + logicalDestination.toString();

		return outMsg;
	}
	
}
