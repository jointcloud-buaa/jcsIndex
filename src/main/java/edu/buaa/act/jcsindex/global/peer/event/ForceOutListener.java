/*
 * @(#) ForceOutListener.java 1.0 2006-2-1
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.peer.event;

import edu.buaa.act.jcsindex.global.AbstractInstance;
import edu.buaa.act.jcsindex.global.peer.management.EventHandleException;
import edu.buaa.act.jcsindex.global.protocol.Message;
import edu.buaa.act.jcsindex.global.protocol.MsgType;

import java.io.ObjectOutputStream;

/**
 * Implement a listener for processing FORCE_OUT message.
 * 
 * @author Xu Linhao
 * @version 1.0 2006-2-1
 */

public class ForceOutListener extends ActionAdapter
{

	public ForceOutListener(AbstractInstance instance)
	{
		super(instance);
	}

	public void actionPerformed(ObjectOutputStream oos, Message msg) throws EventHandleException
	{
		try
		{
//			instance.logout(false, true, true);
		}
		catch (Exception e)
		{
		}
	}

	public boolean isConsumed(Message msg) throws EventHandleException 
	{
		if (msg.getHead().getMsgType() == MsgType.FORCE_OUT.getValue())
			return true;
		return false;
	}

}