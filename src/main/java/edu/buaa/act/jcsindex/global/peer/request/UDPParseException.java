/*
 * @(#) UDPParseException.java 1.0 2006-3-9
 * 
 * Copyright 2006, National University of Singapore.
 * All right reserved.
 */

package edu.buaa.act.jcsindex.global.peer.request;

/**
 * Exception thrown when parse datagram packet eorror.
 * 
 * @author Xu Linhao
 * @version 1.0 2006-3-9
 */

public class UDPParseException extends Exception 
{

	// private members
	private static final long serialVersionUID = 1L;

	public UDPParseException() 
	{
        super();
    }

    public UDPParseException(String arg0) 
    {
        super(arg0);
    }

    public UDPParseException(String arg0, Throwable arg1) 
    {
        super(arg0, arg1);
    }

    public UDPParseException(Throwable arg0) 
    {
        super(arg0);
    }
    
}
