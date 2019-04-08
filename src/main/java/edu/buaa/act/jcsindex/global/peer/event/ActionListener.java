/*
 * @(#) ActionListener.java	1.0 2006-1-24
 *
 * Copyright 2006, National University of Singapore. 
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.peer.event;

import edu.buaa.act.jcsindex.global.peer.management.EventHandleException;
import edu.buaa.act.jcsindex.global.protocol.Message;

import java.io.ObjectOutputStream;
import java.util.EventListener;

/**
 * The listener interface for receiving networking events. 
 * The class that is interested in processing a networking event
 * implements this interface, and the object created with that
 * class is registered by using the object's 
 * <code>addActionListener</code> method. When the networking event occurs, 
 * that object's <code>actionPerformed</code> method is invoked.
 *
 * @author Xu Linhao
 * @version 1.0 2006-1-24
 */
public interface ActionListener extends EventListener
{

	/**
     * Invoked when a networking event occurs.
     * <p>
     * Note: the consumed state should be reset before
     * return a response message.
     * 
     * @param oos the output stream used for replying a message
     * 				to the request peer.
     * @param msg the message to be processed
     */
    void actionPerformed(ObjectOutputStream oos, Message msg) throws EventHandleException;
    
    /**
     * Determine if the listener can consume the network event.
     * 
     * @param msg the network event
     * @return if the listener can consume the event, return <code>true</code>;
     * 			otherwise, return <code>false</code>
     */
    boolean isConsumed(Message msg) throws EventHandleException;
    
}
