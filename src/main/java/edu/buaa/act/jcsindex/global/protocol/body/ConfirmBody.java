/*
 * @(#) ConfirmBody.java 1.0 2006-1-4
 * 
 * Copyright 2005, National University of Singapore.
 * All rights reserved.
 */
package edu.buaa.act.jcsindex.global.protocol.body;

/**
 * Implement confirmation data type defined by SpADE specification.
 * 
 * <p>The data value in confirmation body is empty.
 * 
 * @author Xu Linhao
 */
public class ConfirmBody extends Body
{
	/**
	 * The serialVersionUID is used for serializing and de-serializing
	 * this class and be SURE NOT CHANGE THIS VALUE!
	 */
	private static final long serialVersionUID = -79589139208560390L;

	/**
	 * Override <code>toString()</code> function of <code>java.lang.Object</code>.
	 * 
	 * @return A string that describes the content of the body.
	 */
	public String toString()
	{
		String result = "Body is empty";
		return result;
	}
}