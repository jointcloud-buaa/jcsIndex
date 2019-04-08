/*
 * @(#) ActionAdapter.java 1.0 2006-1-25
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.peer.event;

import edu.buaa.act.jcsindex.global.AbstractInstance;
import edu.buaa.act.jcsindex.global.peer.management.EventHandleException;
import edu.buaa.act.jcsindex.global.protocol.Message;

import java.io.ObjectOutputStream;

/**
 * An abstract adapter class for receiving network events.
 * The methods in this class are empty. This class exists as
 * convenience for creating listener objects.
 * 
 * @author Xu Linhao
 * @version 1.0 2006-1-25
 */

public class ActionAdapter implements ActionListener
{
	
	// protected member
	protected static final boolean debug = true;
	protected AbstractInstance instance;
	
	public ActionAdapter(AbstractInstance instance)
	{
		this.instance = instance;
	}
	
	public void actionPerformed(ObjectOutputStream oos, Message msg) throws EventHandleException
	{
		// do nothing now
		//System.out.println("IN" + msg);
	}

	public boolean isConsumed(Message msg) throws EventHandleException
	{
		return false;
	}

}