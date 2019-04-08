/*
 * @(#) AbstractPooledSocketServer.java 1.0 2005-12-29
 * 
 * Copyright 2005, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.utils;

import javax.swing.*;
import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * This class defines the default implementation of the <code>PooledSocketServer</code>,
 * by wrapping the <code>java.net.ServerSocket</code>.
 * 
 * <p>The <code>AbstractPooledSocketServer</code> is responsible for initializing a set of
 * <code>AbstractPooledSocketHandler</code>s, each of which is responsible for processing
 * the incoming socket requests.
 * 
 * <p>An example for creating your own pooled socket server:
 * <pre>
 * public class MyPooledSocketServer extends AbstractPooledSocketServer
 * {
 * 	// declare socket handlers
 * 	protected MyPooledSocketHandler[] eventHandler;
 * 	
 *	// constructor
 * 	public MyPooledSocketServer(int port, int maxConn)
 * 	{
 * 		super(port, maxConn);
 * 		this.instance = instance;
 * 	}
 * 
 * 	// setup handlers
 * 	public void setupHandlers()
 * 	{
 * 		this.eventHandlers = new MyPooledSocketHandler[this.maxConn];
 * 		for (int i = 0; i < maxConn; i++)
 * 		{
 * 			eventHandlers[i] = new MyPooledSocketHandler();
 * 			eventHandlers[i].registerActionListeners();
 * 			new Thread(eventHandlers[i], "Event Handler " + i).start();
 * 		}
 * 	}
 * 
 * 	// stop server
 * 	public void stop()
 * 	{
 * 		....
 * 	}
 * }
 * </pre>
 * 
 * @author Xu Linhao
 * @version 1.0 2005-12-29
 * 
 * @see PooledSocketServer
 * @see AbstractPooledSocketHandler 
 */

public abstract class AbstractPooledSocketServer implements Runnable, PooledSocketServer
{

	// protected members
	
	/**
	 * The port used for <code>java.net.ServerSocket</code>.
	 */
	protected int port;
	
	/**
	 * The maximum number of socket connections that can be handled simultaneously. 
	 */
	protected int maxConn;
	
	/**
	 * The signal used to determine whether stops the service of accepting the incoming socket requests.
	 */
	protected volatile boolean stop;
	
	/**
	 * The <code>java.net.ServerSocket</code> instance that is responsible for processing socket requests.
	 */
	protected ServerSocket serverSocket;
	
	/**
	 * Construct <code>AbstractPooledSocketServer</code> with specified parameters.
	 * 
	 * @param port the port used for running <code>ServerSocket</code>
	 * @param maxConn the maximum connections that can be handled simultaneously
	 */
	public AbstractPooledSocketServer(int port, int maxConn)
	{
		this.port = port;
		this.maxConn = maxConn;
		this.stop = false;
	}
	
	public void acceptConnections()
	{
		try
		{
			Socket incomingConnection = null;
			while (!stop)
			{
				incomingConnection = serverSocket.accept();
				
				this.showConnections(incomingConnection);
				this.handleConnection(incomingConnection);
				
				if (stop) return;
			}
		}
        catch (BindException e) 
        {
        	if (!stop)
            {
        	    JOptionPane.showMessageDialog(null, "Unable to bind port " + port);
            }
       		return;
        } 
        catch (IOException e) 
        {
            if (!stop)
            {
            	JOptionPane.showMessageDialog(null, "I/O error happens at port: " + port);
            }
           	return;
        }
	}
	
	/**
	 * Initiate a set of <code>AbstractPooledSocketHandler</code>s, the number of which
	 * is specified by the parameter <code>maxConn</code>. 
	 */
	public abstract void setupHandlers(); 
	
	/**
	 * Accept any incoming socket request and then pass it to 
	 * <code>AbstractPooledSocketHandler</code> for processing.
	 * 
	 * @param incomingConnection the incoming socket connection
	 */
	protected void handleConnection(Socket incomingConnection)
	{
		AbstractPooledSocketHandler.processRequest(incomingConnection);
	}
	
	/**
	 * Show the information of the incoming socket request.
	 * 
	 * @param incomingConnection the incoming socket request
	 */
	protected abstract void showConnections(Socket incomingConnection);
	
	/**
	 * Returns <code>true</code> if <code>AbstractPooledSocketServer</code> is still running.
	 * 
	 * @return <code>true</code> if <code>AbstractPooledSocketServer</code> is still running; 
	 * 			otherwise, return <code>false</code>
	 */
	public boolean isAlive()
	{
		if (serverSocket == null)
			return false;
		return !serverSocket.isClosed();
	}

	/**
	 * Initiate <code>ServerSocket</code> that are prepared for accepting 
	 * any incoming socket request and all <code>AbstractPooledSocketHandler</code>s
	 * that are prepared for processing the socket request.
	 */
	public void run()
	{
		try
		{
			/* init the server socket */
			serverSocket = new ServerSocket(port, maxConn);
			
			/* init handlers and accept incoming connections */
			this.setupHandlers();
			this.acceptConnections();
		}
		catch (BindException e)
		{
        	if (!stop)
            {
        	    JOptionPane.showMessageDialog(null, "Unable to bind port " + port);
            }
		}
		catch (IOException e)
		{
            if (!stop)
            {
            	JOptionPane.showMessageDialog(null, "I/O error happens at port: " + port);
            }
		}
	}
	
}