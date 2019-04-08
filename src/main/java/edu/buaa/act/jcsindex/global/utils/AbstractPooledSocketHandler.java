/*
 * @(#) AbstractPooledSocketHandler.java 1.0 2005-12-29
 * 
 * Copyright 2005, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.utils;

import java.io.IOException;
import java.net.Socket;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * The class defines the default implementation of <code>AbstractPooledSocketHandler</code>.
 * 
 * <p>All <code>AbstractPooledSocketHandler</code> will be blocked when there is no socket
 * request in the pool of the socket request; and will be woke up when any socket request
 * comes in and is inserted into the pool. To stop <code>AbstractPooledSocketHandler</code>, 
 * just set the stop signal as <code>true</code>.
 * 
 * <p><code>AbstractPooledSocketServer</code> is responsible for listening network requests
 * from remote users and then passing the socket request to the pool.
 * 
 * <p>An example to creating your own socket handler:
 * <pre>
 * public class MyPooledSocketHandler extends AbstractPooledSocketHandler
 * {
 * 	// used for getting input and output stream of the socket request
 * 	private ObjectInputStream  ois;
 * 	private ObjectOutputStream oos;
 * 
 * 	// handle socket requests
 * 	public void handleConnection()
 * 	{
 * 		// get input stream
 * 		ois = new ObjectInputStream(connection.getInputStream());
 * 		oos = new ObjectOutputStream(connection.getOutputStream());
 * 		
 * 		// process data stream
 * 		...		
 * 
 * 		// close input and output stream
 * 		ois.close();
 * 		oos.close();
 * 	}
 * }
 * </pre> 
 *  
 * @author Xu Linhao
 * @version 1.0 2006-1-12
 * 
 * @see PooledSocketHandler
 * @see AbstractPooledSocketServer 
 */

public abstract class AbstractPooledSocketHandler implements Runnable, PooledSocketHandler
{

	// protected members
	
	/**
	 * The container (or pool) for keeping all incoming socket requests,
	 * which are waiting for being processed by <code>AbstractPooledSocketHandler</code>.
	 */
	protected static List<Socket> pool = Collections.checkedList(new LinkedList<Socket>(), Socket.class);
	
	/**
	 * The signal to determine whether stop the service of <code>AbstractPooledSocketHandler</code>. 
	 */
	protected boolean stop;
	
	/**
	 * The incoming socket request to be processed.
	 */
	protected Socket connection;
	
	/**
	 * Construct <code>AbstractPooledSocketHandler</code>.
	 */
	public AbstractPooledSocketHandler()
	{
		this.stop = false;
	}
	
	/**
	 * Accept any socket request from <code>AbstractPooledSocketServer</code>, 
	 * and then insert the socke request to the end of the pool and notify all
	 * sleeped <code>AbstractPooledSocketHandler</code> to process it.  
	 * 
	 * @param requestToHandle the incoming socket request to be processed
	 */
	public static void processRequest(Socket requestToHandle)
	{
		synchronized (pool)
		{
			pool.add(pool.size(), requestToHandle);
			pool.notifyAll();
		}
	}
	
	/**
	 * To stop all <code>AbstractPooledSocketHandler</code>s, 
	 * the <code>PooledSocketServer</code> needs to close all socket requests
	 * that are stored in the <code>pool</code>.
	 */
	public static void stopAllHandlers()
	{
		synchronized (pool)
		{
			Iterator<Socket> it = pool.iterator();
			while (it.hasNext())
			{
				try
				{
					it.next().close();
				}
				catch (IOException e)
				{
					/* ignore it */
				}
			}
			pool.notifyAll();
		}
	}
	
	/**
	 * When a socket request comes in, all sleeped <code>AbstractPooledSocketHandler</code>
	 * will be woken up to handle with it. If no socket request arrives and the stop flag
	 * is false, then <code>AbstractPooledSocketHandler</code> will be blocked to wait for
	 * a socket request appearing.
	 */
	public void run()
	{
		while (!stop)
		{
			synchronized (pool)
			{
				while (pool.isEmpty())
				{
					try
					{
						pool.wait();
					}
					catch (InterruptedException e)
					{
						/* ignore it */
					}
					
					if (stop) return;
				}
				
				/* get a socket connection to handle */
				connection = pool.remove(0);
			}
			
			/* handle socket now */
			this.handleConnection();
		}
	}
	
	/**
	 * Set the stop flag as <code>true</code>.
	 */
	public void stop()
	{
		this.stop = true;
	}
	
}