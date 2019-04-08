package edu.buaa.act.jcsindex.global.peer.info;

import java.io.Serializable;

/**
 * this class contains the necessary info to be stored in the index
 * 
 * @author Vu Quang Hieu
 * @version 1.0 2006-3-22
 */

public class IndexInfo implements Serializable {

	private static final long serialVersionUID = 6564152697562874898L;
	
	String value;
	
	public IndexInfo(String value)
	{
		this.value = value;
	}
	
	/**
	 * two index informations are equal if they represent the same document:
	 * the same peer and the same path
	 */
	public boolean equals(Object o){
		if (o == null) return false;
		if (!(o instanceof IndexInfo)) return false;
		IndexInfo oo = (IndexInfo)o;
		return oo.value.equals(this.value);
	}

	/**
	 * Get document identifier.
	 * 
	 * @return the document identifier, a 160-bits character string
	 */
	public String getValue()
	{
		return this.value;
	}
	
	@Override
	public String toString(){
		String outMsg;
		
		outMsg = this.value;
		
		return outMsg;
	}
}

