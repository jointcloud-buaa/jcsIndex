/*
 * @(#) Logger.java 1.0 2006-7-17
 * 
 * Copyright 2006 National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.logging;

import java.util.logging.FileHandler;
import java.util.logging.Level;

/**
 * A simple wrapper of <code>java.util.logging.Logger</code>.
 * 
 * @author Xu Linhao
 * @version 1.0 2006-7-17
 */

public final class Logger
{
	
	// wrap the java.util.logging.Logger
	private final java.util.logging.Logger logger;

	/**
	 * Create a new logger for a named subsystem. 
	 * 
	 * @param name A name for the logger. This should be a dot-separated name 
	 * 				and should normally be based on the package name 
	 * 				or class name of the subsystem, such as <code>java.net</code>
	 */
	public Logger(String name)
	{
		logger = java.util.logging.Logger.getLogger(name);
	}
	
	/**
	 * Find or create a logger for a named subsystem. If a logger has already 
	 * been created with the given name it is returned. 
	 * Otherwise a new logger is created. 
	 * 
	 * @param name A name for the logger. This should be a dot-separated name 
	 * 				and should normally be based on the package name 
	 * 				or class name of the subsystem, such as <code>java.net</code>
	 */
	public static Logger getLogger(String name)
	{
		return new Logger(name);
	}
	
	/**
	 * Add a file handler to logger.
	 * 
	 * @param handler the file handler
	 */
	public void addHandler(FileHandler handler)
	{
		logger.addHandler(handler);
	}
	
	/**
	 * Wrap the <code>log</code> method of the <code>java.util.logging.Logger</code>
	 * by setting the <code>Level</code> as <code>INFO</code>.
	 * 
	 * @param message the string message (or a key in the message catalog)
	 */
	public void info(String message)
	{
		logger.log(Level.INFO, message);
	}
	
	/**
	 * Wrap the <code>log</code> method of the <code>java.util.logging.Logger</code>
	 * by setting the <code>Level</code> as <code>INFO</code>.
	 * 
	 * @param message the string message (or a key in the message catalog)
	 * @param thrown throwable associated with log message
	 */
	public void info(String message, Throwable thrown)
	{
		logger.log(Level.INFO, message, thrown);
	}
	
	/**
	 * Wrap the <code>log</code> method of the <code>java.util.logging.Logger</code>
	 * by setting the <code>Level</code> as <code>FINER</code>.
	 * 
	 * @param message the string message (or a key in the message catalog)
	 */
	public void debug(String message)
	{
		logger.log(Level.FINER, message);
	}
	
	/**
	 * Wrap the <code>log</code> method of the <code>java.util.logging.Logger</code>
	 * by setting the <code>Level</code> as <code>FINER</code>.
	 * 
	 * @param message the string message (or a key in the message catalog)
	 * @param thrown throwable associated with log message
	 */
	public void debug(String message, Throwable thrown)
	{
		logger.log(Level.FINER, message, thrown);
	}
	
	/**
	 * Wrap the <code>log</code> method of the <code>java.util.logging.Logger</code>
	 * by setting the <code>Level</code> as <code>WARNING</code>.
	 * 
	 * @param message the string message (or a key in the message catalog)
	 */
	public void warn(String message)
	{
		logger.log(Level.WARNING, message);
	}
	
	/**
	 * Wrap the <code>log</code> method of the <code>java.util.logging.Logger</code>
	 * by setting the <code>Level</code> as <code>WARNING</code>.
	 * 
	 * @param message the string message (or a key in the message catalog)
	 * @param thrown throwable associated with log message
	 */
	public void warn(String message, Throwable thrown)
	{
		logger.log(Level.WARNING, message, thrown);
	}

	/**
	 * Wrap the <code>logp</code> method of the <code>java.util.logging.Logger</code>
	 * by setting the <code>Level</code> as <code>SEVERE</code>.
	 * 
	 * @param message the string message (or a key in the message catalog)
	 */
	public void error(String message)
	{
		logger.log(Level.SEVERE, message);
	}
	
	/**
	 * Wrap the <code>log</code> method of the <code>java.util.logging.Logger</code>
	 * by setting the <code>Level</code> as <code>SEVERE</code>.
	 * 
	 * @param message the string message (or a key in the message catalog)
	 * @param thrown throwable associated with log message
	 */
	public void error(String message, Throwable thrown)
	{
		logger.log(Level.SEVERE, message, thrown);
	}

	/**
	 * Judge if the logger enables the debug model.
	 * 
	 * @return <code>true</code> if the debug model is enabled
	 */
	public boolean isDebugEnabled()
	{
		return logger.isLoggable(Level.FINER);
	}
	
	/**
	 * Enable the logger to record the a fairly detailed tracing message. 
	 * By default logging calls for entering, returning, 
	 * or throwing an exception are traced at this level.
	 */
	public void enableDebug()
	{
		logger.setLevel(Level.FINER);
	}
	
}