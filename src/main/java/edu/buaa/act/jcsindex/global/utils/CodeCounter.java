/*
 * @(#) CodeCounter.java 1.0 2006-2-6
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * Statistics the total number of lines of the source code
 * in a specified directory.
 * 
 * @author Xu Linhao
 * @version 1.0 2006-2-6
 */

public class CodeCounter
{
	// public members
	/**
	 * The number of lines of the source code.
	 */
	public static long codeLines = 0;
	
	private static final String usage = 
		"Usage: CodeCounter [dir]\r\n" +
			"   dir\t the source code directory\r\n";

	private static void usage()
	{
		System.out.println();
		System.out.print(usage);
		System.exit(1);
	}

	private static void getCodeLine(File file)
	{
		File[] files = file.listFiles();
		for (int i = 0; i < files.length; i++)
		{
			if (files[i].isDirectory())
			{
				getCodeLine(files[i]);
			}
			else if (files[i].getName().indexOf(".java") != -1)
			{
				try
				{
					BufferedReader in = new BufferedReader(new FileReader(files[i]));
					String line = new String();
					while ((line = in.readLine()) != null)
					{
						if (!line.trim().equalsIgnoreCase(""))
						{
							codeLines++;
						}
					}
				}
				catch (IOException e)
				{
					e.printStackTrace();
				}
			}	
		}
	}
	
	/**
	 * Statistics the total number of lines of the source code
	 * in a specified directory.
	 * 
	 * @param dir the directory where the source code lies in
	 */
	public CodeCounter(String dir)
	{
		File file = new File(dir);
		if (file.isDirectory())
		{
			getCodeLine(file);
		}

		System.out.println(codeLines);
	}

	public static void main(String[] args)
	{
		if (args.length != 1)
		{
			usage();
		}
		
		new CodeCounter(args[0]);
	}
	
}