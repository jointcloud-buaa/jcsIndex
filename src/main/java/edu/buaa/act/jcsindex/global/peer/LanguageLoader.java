/*
 * @(#) SystemManager.java 1.0 2007-7-2
 */

package edu.buaa.act.jcsindex.global.peer;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Properties;

/**
 * This class defines a factory method to initiate
 * language-specific strings used by system.  
 * 
 * @author Xu Linhao
 * @version 1.0 2007-7-2
 */

public abstract class LanguageLoader
{
	
	/* system supported language */
	public static final int english = 0;
	public static final int chinese = 1;
	
	private static final String cnPropFile = "./property_cn.ini";
	private static final String enPropFile = "./property_en.ini";
	
	/* store language-specific strings */
	private static Properties keys = null;

	public static int locale = -1;
	
	/**
	 * Initate a system property with the specified language, and
	 * system can get the property value with the corresponding 
	 * property key.
	 *
	 * @param lan the language option
	 */
	public static void newLanguageLoader(int lan)
	{
		try
		{
			/* load corresponding configuration file */
			FileInputStream fin = null;
			switch (lan)
			{
			case english:
				fin = new FileInputStream(enPropFile);
				break;
				
			case chinese:
				fin = new FileInputStream(cnPropFile);
				break;
				
			default:
				fin = new FileInputStream(enPropFile);
			}
			
			/* load items into memory */
			if (fin != null)
			{
				keys = new Properties();
				keys.load(fin);
				
				fin.close();
			}
			
			locale = lan;
			
			//
			//AccCtrlLanguageLoader.newLanguageLoader(lan);
		}
		catch (IOException e)
		{
			throw new RuntimeException("Cannot find system property file", e);
		}
	}
	
	/**
	 * Searches for the property with the specified key in this property list. 
	 * The method returns null if the property is not found.
	 * 
	 * @param key the property key 
	 * @return returns the value in this property list with the specified key value
	 */
	public static String getProperty(String key)
	{
		if (keys == null)
			throw new RuntimeException("Language loader is not initiated");
		
		String tmp = keys.getProperty(key);
		try 
		{
			return new String(tmp.getBytes("ISO-8859-1"), "GBK");
		} 
		catch (UnsupportedEncodingException e) 
		{
			return tmp;
		}
	}
	
}