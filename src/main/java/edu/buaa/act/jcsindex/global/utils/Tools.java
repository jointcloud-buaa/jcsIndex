/*
 * @(#) Tools.java 1.0 2006-7-5
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.utils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

/**
 * A utility that defines a set of methods used for general purpose, 
 * such as <code>Date</code> transformation, string description of
 * <code>Throwable</code>, and numeric type conversion.
 * 
 * @author Xu Linhao
 * @version 1.0 2006-7-5
 */

public class Tools
{
	
	// ---------- generate digest -------------
	
	/* do byte-to-hexchar conversion */
	private static final char[] HEX = 
	{
		'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'
	};
	
	/**
	 * Returns the digest of the byte array.
	 * 
	 * @param input the byte array
	 * @return the digest of the input byte array
	 */
	public static String getDigest(byte[] input)
	{
		MessageDigest md;
		try 
		{
			md = MessageDigest.getInstance("SHA");
		} 
		catch (NoSuchAlgorithmException e) 
		{
			return null;
		}

		StringBuffer buf = new StringBuffer();
		byte[] digest = md.digest(input);
		for (int i = 0; i < digest.length; i++)
		{
			int b = digest[i];
			buf.append(HEX[(b >> 4) & 0xf]);
			buf.append(HEX[b & 0xf]);
		}
		
		return buf.toString();
	}

	// --------------- for date conversion ------------------
	
	/**
	 * Returns the time of the date.
	 * 
	 * @param f the date format used for describing the time of the date
	 * @param d the date
	 * @return the time of the date
	 */
	public static long getTime(String f, Date d)
	{
		DateFormat formatter = new SimpleDateFormat(f, Locale.US);
		formatter.setTimeZone(TimeZone.getDefault());
		return Long.parseLong(formatter.format(d));
	}
	
	/**
	 * Returns the hour of the date.
	 * 
	 * @param d the date
	 * @return the hour of the date
	 */
    public static int getHour(Date d)
    {
		DateFormat formatter = new SimpleDateFormat("HH", Locale.US);
		formatter.setTimeZone(TimeZone.getDefault());
		return Integer.parseInt(formatter.format(d));
    }

    /**
     * Returns the minute of the date.
     * 
     * @param d the date
     * @return the minute of the date
     */
    public static int getMinute(Date d)
    {
		DateFormat formatter = new SimpleDateFormat("mm", Locale.US);
		formatter.setTimeZone(TimeZone.getDefault());
		return Integer.parseInt(formatter.format(d));
    }
    
    /**
     * Returns the AM/PM of the date.
     * 
     * @param d the date
     * @return the AM/PM of the date
     */
    public static String getAM(Date d)
    {
		DateFormat formatter = new SimpleDateFormat("a", Locale.US);
		formatter.setTimeZone(TimeZone.getDefault());
		return formatter.format(d);
    }
    
    // ------------------- for exception ------------------
    
    /**
     * Returns the string description of a <code>Throwable</code> instance.
     * 
     * @param e the <code>Throwable</code> instance
     * @return the string description of a <code>Throwable</code> instance
     */
    public static String getException(Throwable e)
    {
    	String result = new String();
    	
    	StackTraceElement[] elements = e.getStackTrace();
    	for (int i = 0; i < elements.length; i++)
    		result += "\tat " + elements[i] + "\r\n";

    	return result;
    }
    
    // ----------------- for data type conversion -----------------------
    
    /**
     * Convert an integer value into 4 bytes and keep the bytes
     * into an array by specifying the start position to be used
     * for storing.
     * 
     * @param val the integer value to be converted
     * @param buf the byte array used for storing converted bytes
     * @param start the position to store the converted bytes
     * 
     * @see #byteArrayToInt
     */ 
    public static void intToByteArray(int val, byte[] buf, int start)
    {
        int i, j;
        for (i = 0; i < 4; i++)
        {
            j = val & 255;
           	buf[start + i] = (byte) j;
           	val >>= 8;
        }
	}
    
    /**
     * Convert a 4-bytes array to an integer value from the specified 
     * start position of the array.
     * 
     * @param buf the byte array to be used for transformation
     * @param start	the position to start conversion
     * @return an integer value converted from the array
     * 
     * @see #intToByteArray
     */
    public static int byteArrayToInt(byte buf[], int start)
	{	    
	    int j = 0;
	    for (int i = 3; i >= 0; i--)
	    {
	        j <<= 8;
	        j += ((int) buf[i + start]) & 255;
	    }
	    return j;
	}

}