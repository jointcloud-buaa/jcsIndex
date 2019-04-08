package edu.buaa.act.jcsindex.global.peer.info;

import java.io.Serializable;

/**
 * This class contains a unit of a data index at a peer node
 * 
 * @author Quang Hieu Vu
 * @version 1.0 2008-05-29
 */

public class LocalDataIndex implements Serializable 
{

	private static final long serialVersionUID = 1137066528580182988L;
	
	private String term;
	private String bitmapValue;
		
	public LocalDataIndex(String term, String bitmapValue)
	{
		this.term = term;
		this.bitmapValue = bitmapValue;
	}
	
	public String getTerm()
	{
		return this.term;
	}
	
	public String getBitmapValue()
	{
		return this.bitmapValue;
	}
	
	@Override
	public String toString(){
		String outMsg;
		
		outMsg = this.term;
		outMsg += "$" + this.bitmapValue;
			
		return outMsg;
	}
}
