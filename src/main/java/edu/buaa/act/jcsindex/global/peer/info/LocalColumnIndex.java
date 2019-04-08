package edu.buaa.act.jcsindex.global.peer.info;

import java.io.Serializable;
import java.util.Vector;

/**
 * This class contains a unit of a column index at a peer node
 * 
 * @author Quang Hieu Vu
 * @version 1.0 2008-05-29
 */

public class LocalColumnIndex implements Serializable
{

	private static final long serialVersionUID = 4521475443682205426L;

	private String columnName;
	private Vector<String> listOfTables;

	public LocalColumnIndex(String columnName, Vector<String> listOfTables)
	{
		this.columnName = columnName;
		this.listOfTables = listOfTables;
	}
	
	public String getColumnName()
	{
		return this.columnName;
	}
	
	public Vector<String> getListOfTables(){
		return this.listOfTables;
	}
	
	@Override
	public String toString(){
		String outMsg;
		
		outMsg = this.columnName;
		for (int i = 0; i < this.listOfTables.size(); i++)
			outMsg += "$" + this.listOfTables.get(i);
			
		return outMsg;
	}
}
