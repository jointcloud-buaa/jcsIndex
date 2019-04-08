/*
 * @(#) EventDispatcherAdapter.java 1.0 2006-1-7
 * 
 * Copyright 2006, National University of Singapore.
 * All right reserved.
 */

package edu.buaa.act.jcsindex.global.peer.management;

import edu.buaa.act.jcsindex.global.utils.AbstractPooledSocketHandler;

/**
 * An empty class is convenient for creating
 * a concrete event dispatcher.
 * 
 * @author Xu Linhao
 * @version 1.0 2006-1-7
 */

public class EventDispatcherAdapter extends AbstractPooledSocketHandler implements EventDispatcher
{

	public void registerActionListeners() 
	{
	}

	public void handleConnection() 
	{
	}
	
}
