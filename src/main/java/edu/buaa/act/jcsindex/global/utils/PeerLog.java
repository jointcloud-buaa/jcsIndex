package edu.buaa.act.jcsindex.global.utils;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;

/**
 * Implement logging function for peers
 *
 */
public class PeerLog 
{
	private BufferedWriter writer;

	/**
	 * Constructor
	 * @param FileName log file name
	 */
	public PeerLog(String FileName)
	{
		try
		{
			writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(FileName)));
		}
		catch(Exception e)
		{
			System.out.println("PeerLog-Constructor: Error while creating file");
			e.printStackTrace();
		}
	}

	/**
	 * Log network message 
	 * @param Data message
	 */
	public synchronized void WriteLog(String data)
	{
		try
		{
			System.out.println(data);
			writer.write(data);
			writer.newLine();
			writer.flush();
		}
		catch(Exception e)
		{
			System.out.println("Log-WriteNetworkLog: Error while writing network log files:");
			e.printStackTrace();
		}
	}
	
	/**
	 * Finish, close the log file
	 */
	public void Exit()
	{
		try{
			writer.close();			
		}
		catch(Exception e)
		{
			System.out.println("PeerLog-Exit: Error while closing files");
			e.printStackTrace();
		}
	}
}
