/*
 * @(#) ForceOutBody.java 1.0 2007-5-11
 */

package edu.buaa.act.jcsindex.global.protocol.body;

/**
 * Used for sending a signal to either super peers or common peers
 * to force them out the system.
 * 
 * @author Xu Linhao
 * @version 1.0 2007-5-11
 */

public class ForceOutBody extends Body
{

	private static final long serialVersionUID = 1163354805136589491L;
	
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