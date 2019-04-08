/*
 * @(#) EventHandleException.java 1.0 2006-1-7
 * 
 * Copyright 2006, National University of Singapore.
 * All right reserved.
 */

package edu.buaa.act.jcsindex.global.peer.management;

/**
 * Exception thrown by the <code>EventDispatcher</code>.
 * 
 * @author Xu Linhao
 * @version 1.0 2006-1-7
 */

public class EventHandleException extends Exception 
{

	// private members
	private static final long serialVersionUID = -2583397419323429665L;

	public EventHandleException() 
	{
        super();
    }

    public EventHandleException(String arg0) 
    {
        super(arg0);
    }

    public EventHandleException(String arg0, Throwable arg1) 
    {
        super(arg0, arg1);
    }

    public EventHandleException(Throwable arg0) 
    {
        super(arg0);
    }
    
}
