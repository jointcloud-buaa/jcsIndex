package edu.buaa.act.jcsindex.global.peer.info;

import java.io.Serializable;

/**
 * Implement a pair of index: keyword and field.
 * 
 * @author Vu Quang Hieu
 * @version 1.0 2006-3-22
 */

public class IndexPair implements Serializable {

	private static final long serialVersionUID = 8358627612268619718L;

	private String keyword;
	private String fieldID;
	
	/**
	 * Construct the Index Pair with keyword and fieldID.
	 * 
	 * @param keyword
	 * @param fieldID
	 */
	public IndexPair(String keyword, String fieldID){
		this.keyword = keyword;
		this.fieldID = fieldID;
	}
	
	/**
	 * Construct the message body from a string value.
	 * 
	 * @param serializeData the string value that contains 
	 * the serialized message body
	 */
	public IndexPair(String serializeData)
	{
		String[] arrData = serializeData.split(":");
		try
		{
			this.keyword = arrData[1];
			this.fieldID = arrData[2];
			
		}
		catch(Exception e)
		{
			System.out.println("Incorrect serialize data at IndexPair:" + serializeData);
			System.out.println(e.getMessage());
		}
	}
	
	/**
	 * Get keyword
	 * @return keyword
	 */
	public String getKeyword()
	{
		return this.keyword;
	}
	
	/**
	 * Get field ID
	 * @return fieldID
	 */
	public String getFieldID()
	{
		return this.fieldID;
	}
	
	@Override
	public String toString(){
		String outMsg;
				
		outMsg = keyword;
		outMsg += "$" + fieldID;
		
		return outMsg;
	}
}

