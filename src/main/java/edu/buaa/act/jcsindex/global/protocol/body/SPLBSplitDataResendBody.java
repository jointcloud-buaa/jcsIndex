/*
 * @(#) SPLBSplitDataResendBody.java 1.0 2006-2-22
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.protocol.body;

import edu.buaa.act.jcsindex.global.peer.info.BoundaryValue;
import edu.buaa.act.jcsindex.global.peer.info.IndexValue;
import edu.buaa.act.jcsindex.global.peer.info.LogicalInfo;
import edu.buaa.act.jcsindex.global.peer.info.PhysicalInfo;

import java.io.Serializable;
import java.util.Vector;

/**
 * Implement the message body used for feedback the requester
 * to resend the SPLBSplitData request since the current  
 * receiver is not its adjacent
 * 
 * @author Vu Quang Hieu
 * @version 1.0 2006-2-22
 */

public class SPLBSplitDataResendBody extends Body implements Serializable
{

	// private members
	private static final long serialVersionUID = 865464147039585757L;
	
	private PhysicalInfo physicalSender;
	private boolean direction;
	private BoundaryValue minValue;
	private BoundaryValue maxValue;
	private Vector<IndexValue> data;
	private LogicalInfo logicalDestination;

	/**
	 * Construct the message body with speicified parameters.
	 * 
	 * @param physicalSender physical address of the sender
	 * @param direction direction of sending request
	 * @param minValue min value of splitted range
	 * @param maxValue max value of splitted range
	 * @param data splited data
	 * @param logicalDestination logical address of the receiver
	 */
	public SPLBSplitDataResendBody(PhysicalInfo physicalSender, boolean direction, BoundaryValue minValue, 
			BoundaryValue maxValue,	Vector<IndexValue> data, LogicalInfo logicalDestination) 
	{
		this.physicalSender = physicalSender;
		this.direction = direction;
		this.minValue = minValue;
		this.maxValue = maxValue;
		this.data = data;
		this.logicalDestination = logicalDestination;
	}

	/**
	 * Construct the message body with a string value.
	 * 
	 * @param serializeData the string value that contains 
	 * the serialized message body
	 */
	public SPLBSplitDataResendBody(String serializeData)
	{
		String[] arrData = serializeData.split(":");
		try
		{
			this.physicalSender = new PhysicalInfo(arrData[1]);
			this.direction = Boolean.valueOf(arrData[2]).booleanValue();
			this.minValue = new BoundaryValue(arrData[3]);
			this.maxValue = new BoundaryValue(arrData[4]);
			this.data = new Vector<IndexValue>();
			String[] arrTransferedData = arrData[5].split("%");
			for (int i = 0; i < arrTransferedData.length; i++)
			{
				this.data.add(new IndexValue(arrTransferedData[i]));
			}
			this.logicalDestination = new LogicalInfo(arrData[6]);
		}
		catch(Exception e)
		{
			System.out.println("Incorrect serialize data at LBSplitDataResend:" + serializeData);
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
	 * Get direction of sending request 
	 * 
	 * @return direction of sending request
	 */
	public boolean getDirection()
	{
		return this.direction;
	}

	/**
	 * Get min value of splited range
	 * 
	 * @return min value of splited range
	 */
	public BoundaryValue getMinValue()
	{
		return this.minValue;
	}

	/**
	 * Get max value of splited range
	 * 
	 * @return max value of splited range
	 */
	public BoundaryValue getMaxValue()
	{
		return this.maxValue;
	}

	/**
	 * Get splited data
	 * 
	 * @return splited data
	 */
	public Vector<IndexValue> getData()
	{
		return this.data;
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

		outMsg = "LBSPLITDATARESEND";
		outMsg += "\n\t Physical Sender:" + physicalSender.toString();
		outMsg += "\n\t Direction:" + direction;
		outMsg += "\n\t Min Value:" + minValue;
		outMsg += "\n\t Max Value:" + maxValue;
		outMsg += "\n\t Data:";
		for (int i = 0; i < data.size() - 1; i++)
		{
			outMsg += data.get(i) + " ";
		}
		outMsg += data.get(data.size() - 1);
		outMsg += "\n\t Logical Destination:" + logicalDestination.toString();

		return outMsg;
	}

	@Override
	public String toString()
	{
		String outMsg;

		outMsg = "LBSPLITDATARESEND";
		outMsg += ":" + physicalSender.toString();
		outMsg += ":" + direction;
		outMsg += ":" + minValue;
		outMsg += ":" + maxValue;
		outMsg += ":";
		for (int i = 0; i < data.size() - 1; i++)
		{
			outMsg += data.get(i) + "%";
		}
		outMsg += data.get(data.size() - 1);
		outMsg += ":" + logicalDestination.toString();

		return outMsg;
	}
	
}
