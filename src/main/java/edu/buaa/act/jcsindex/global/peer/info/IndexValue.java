/*
 * @(#) IndexValue.java 1.1 2006-3-6
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.peer.info;

import edu.buaa.act.jcsindex.global.utils.PeerMath;

import java.io.Serializable;

/**
 * Implement an index key.
 * 
 * @author Vu Quang Hieu
 * @author (Modified by) Xu Linhao
 * @author (Modified by) Vu Quang Hieu
 * @version 1.1 2006-3-6
 */

public class IndexValue implements Comparable, Serializable
{

	// public members
	/**
	 * The index key is numeric value type.
	 */
	public final static int NUMERIC_TYPE = 0;
	
	/** 
	 * The index key is character type.
	 */
	public final static int STRING_TYPE  = 1;
	
	/**
	 * The tuple index
	 */
	public final static int TUPLE_INDEX_TYPE = 2;
	
	/**
	 * The range index
	 */
	public final static int RANGE_INDEX_TYPE = 3;
	
	//added by Quang Hieu	
	public final static int TABLE_INDEX = 4;
	public final static int COLUMN_INDEX = 5;
	public final static int DATA_INDEX = 6;
	
	/**
	 * The minimum character value and minimum index key.
	 */
	private final static char MIN_CHARACTER = 'a';
	public final static IndexValue MIN_KEY  = new IndexValue(STRING_TYPE, String.valueOf(MIN_CHARACTER));
	
	/**
	 * The maximum character value and the maximum index key.
	 */
	private final static char MAX_CHARACTER = 'z';
	public final static IndexValue MAX_KEY  = new IndexValue(STRING_TYPE, String.valueOf("zzzzzzzzzzzzzzzzzzzz"));
	
	/**
	 * Between the upper case and lower case, there are six additional
	 * characters, and hence when calculating the average value of two
	 * <code>String</code> we simply use this OFFSET value to adjust
	 * the median value of two characters.
	 */
	public final static int OFFSET = 3;
	
	// private members
	private static final long serialVersionUID = 2193787517910194014L;
	
	private int type;
	private int timeIndex;
	// value = key
	private Object value;	
	public IndexInfo indexInfo = null;
	
	/**
	 * Construct an index key with specified value.
	 * 
	 * @param type the type of the index key
	 * @param newValue the value of the index key
	 */
	public IndexValue(int type, String newValue)
	{	
		parse(type, newValue);
		this.indexInfo = null;
	}
	
	public IndexValue(int type, String newValue, IndexInfo indexInfo)
	{
		parse(type, newValue);
		this.indexInfo = indexInfo;
	}

	// TODO：临时增加一个构造函数
	public IndexValue(int type, int timeIndex, String newValue, IndexInfo indexInfo) {
		parse(type, newValue);
		this.timeIndex = timeIndex;
		this.indexInfo = indexInfo;
	}

	// TODO: 增加一个get函数
	public int getTimeIndex() {
		return timeIndex;
	}
	
	/**
	 * Construct an index value with a serialized string value.
	 * 
	 * @param serializeData the serialized string value
	 */
	public IndexValue(String serializeData)
	{
	    String[] arrData = serializeData.split("&");
		try
		{
			this.type  = Integer.parseInt(arrData[0]);
			parse(type, arrData[1]);
			if (arrData[2].equals("null"))
				this.indexInfo = null;
			else
				this.indexInfo = new IndexInfo(arrData[2]);
		}
		catch (Exception e)
		{
			e.printStackTrace();
	    	System.out.println("Incorrect serialize data for IndexValue:" + serializeData);
		}
	}
	
	public int getType()
	{
		return this.type;
	}
	
	/**
	 * Returns the key value.
	 * 
	 * @return returns the key value
	 */
	public String getKey()
	{
		String key = null;
		switch (type)
		{
		case NUMERIC_TYPE:
			this.type = NUMERIC_TYPE;
			key = ((Integer) this.value).toString();
			break;
			
		case STRING_TYPE:
			this.type  = STRING_TYPE;
			key = (String) this.value;
			break;
        
		case TUPLE_INDEX_TYPE:
			this.type = TUPLE_INDEX_TYPE;
			TupleIndexInfo info = (TupleIndexInfo)this.value;
			String value = info.getData()[info.getIdx()];
			key = value;
			break;
		
		case RANGE_INDEX_TYPE:
			this.type = RANGE_INDEX_TYPE;
			RangeIndexInfo range = (RangeIndexInfo)this.value;
			key = range.getMinValue();
			break;

		case TABLE_INDEX:
			key = (String) this.value;
			break;
		
		case COLUMN_INDEX:
			key = (String) this.value;
			break;	
		
		case DATA_INDEX:
			key = (String) this.value;
			break;
			
		default:
			throw new IllegalArgumentException("Unknown type!");
		}
		return key;
	}
	
	/**
	 * Parse the value to be assigned to the index key.
	 * 
	 * @param type the type of the index key
	 * @param value the value of the index key
	 */
	private void parse(int type, String value)
	{
		try
		{
			switch (type)
			{
			case NUMERIC_TYPE:
				this.type  = NUMERIC_TYPE;
				this.value = new Integer(Integer.parseInt(value));
				break;				
			case STRING_TYPE:
				this.type  = STRING_TYPE;
				this.value = new String(value);
				break;
			case TUPLE_INDEX_TYPE:
				this.type = TUPLE_INDEX_TYPE;
				this.value = new TupleIndexInfo(value);
				break;
			case RANGE_INDEX_TYPE:
				this.type = RANGE_INDEX_TYPE;
				this.value = new RangeIndexInfo(value);
				break;
			case TABLE_INDEX:
				this.type = TABLE_INDEX;	
				this.value = value;
				break;
			case COLUMN_INDEX:
				this.type = COLUMN_INDEX;
				this.value = value;
				break;
			case DATA_INDEX:
				this.type = DATA_INDEX;
				this.value = value;
				break;
			default:
				throw new IllegalArgumentException("Unknown type!");
			}
		}
		catch (NumberFormatException e)
		{
			e.printStackTrace();
		}
	}
	
	/**
	 * Compares this object with the specified object for order. 
	 * Returns a negative integer, zero, or a positive integer 
	 * as this object is less than, equal to, or greater than 
	 * the specified object.
	 * 
	 * @param compareObject the object to be compared
	 * @return a negative integer, zero, or a positive integer 
	 * 			as this object is less than, equal to, or greater than 
	 * 			the specified object.
	 * @throws ClassCastException
	 */
	public int compareTo(Object compareObject) throws ClassCastException
	{	
		if (compareObject instanceof IndexValue)
		{
			IndexValue compareValue = (IndexValue) compareObject;
			switch (compareValue.type)
			{
				case NUMERIC_TYPE:
					return ((Integer) this.value).compareTo((Integer) compareValue.value);
					
				case STRING_TYPE:
					return ((String)this.value).compareTo((String)compareValue.value);
				
				case TUPLE_INDEX_TYPE:
					TupleIndexInfo info = (TupleIndexInfo)compareValue.value;
					String key1 = compareValue.getKey();
					String key2 = this.getKey();
					
					if(info.getType()==Schema.INTEGER_TYPE){
						return (new Integer(key2)).compareTo(new Integer(key1));
					}
					else return key2.compareTo(key1);
				
				case RANGE_INDEX_TYPE:
					RangeIndexInfo range1 = (RangeIndexInfo)compareValue.value;
					RangeIndexInfo range2 = (RangeIndexInfo)this.value;
					if(range1.getType()==Schema.INTEGER_TYPE){
						return (new Integer(range2.getMinValue())).compareTo(new Integer(range1.getMinValue()));
					}
					else return range2.getMinValue().compareTo(range1.getMinValue());
				default:
					throw new ClassCastException("Object is invalid");
			}
		}
		else if (compareObject instanceof BoundaryValue)
		{
			BoundaryValue compareValue = (BoundaryValue) compareObject;
			return ((String)this.value).compareTo((String)compareValue.getStringValue());			
		}
		throw new ClassCastException("Object is invalid");		
	}
	
	/**
	 * Return the minimum integer value of the index key.
	 * 
	 * @return the minimum integer value of the index key
	 */
	public static int getMinIntValue()
	{
		return Integer.MIN_VALUE;
	}
	
	/**
	 * Return the maximum integer value of the index key.
	 * 
	 * @return the maximum integer value of the index key
	 */
	public static int getMaxIntValue()
	{
		return Integer.MAX_VALUE;
	}
	
	/**
	 * Calculate the median value of two given <code>IndexValue</code>s.
	 * 
	 * @param value1 the first index key
	 * @param value2 the second index key
	 * @return the median value
	 * @throws ClassCastException
	 */
	public static IndexValue average(IndexValue value1, IndexValue value2) throws ClassCastException
	{
		if ((value1.type == NUMERIC_TYPE) && (value2.type == NUMERIC_TYPE))
		{
			int result = (((Integer) value1.value).intValue() + ((Integer) value2.value).intValue()) / 2;			
			return new IndexValue(NUMERIC_TYPE, String.valueOf(result));
		}
		else if ((value1.type == STRING_TYPE) && (value2.type == STRING_TYPE))
		{
			String str1 = (String) value1.value;
			String str2 = (String) value2.value;
			
			/* 
			 * let the shorter string is str1, 
			 * and the longer one is str2 
			 */
			if (str1.length() > str2.length())
			{
				str1 = (String) value2.value;
				str2 = (String) value1.value;
			}
			
			int len = str1.length(); 
			int maxLen = str2.length();
			
			String result = new String();
			char c1;
			char c2;
			int mid = 0;
			for (int i = 0; i < len ; i++)
			{
				c1 = str1.charAt(i);
				c2 = str2.charAt(i);
				
				/* swap c1 and c2, if c1 is bigger than c2 */
				if (c1 > c2)
				{
					c1 = str2.charAt(i);
					c2 = str1.charAt(i);
				}
				
				/* if c1 is not equal to c2 */
				if (c1 != c2)
				{
					mid = (c1 + c2) / 2;	// calculate median value first
					
					/* if the minimal value is lower case */
					if (PeerMath.isLowerCase(c1))
					{
						if (!PeerMath.isLowerCase(c2))
						{
							/* 
							 * since mid is bigger than c1 and c1 is a lower case,
							 * the operation 'mid--' is safe
							 */
							while (!PeerMath.isLowerCase((char) mid) && (mid > c1))
							{
								mid--;
							}
						}
					}
					
					/* if the minimal value is upper case */
					else if (PeerMath.isUpperCase(c1))
					{
						if (PeerMath.isUpperCase(c2))
						{
							// do nothing
						}
						else if (PeerMath.isLowerCase(c2))
						{
							mid = mid - OFFSET;	// should minus the OFFSET
							while (!PeerMath.isUpperCase((char) mid) && (mid > c1))
							{
								mid--;
							}
						}
						else
						{
							/* 
							 * since mid is bigger than c1 and c1 is a upper case,
							 * the operation 'mid--' is safe
							 */
							while (!PeerMath.isUpperCase((char) mid) && !PeerMath.isLowerCase((char) mid) 
									&& (mid > c1)) 
							{
								mid--;
							}
						}
					}
					
					/* if the minimal value is numeric value */
					else if (PeerMath.isNumeric(c1))
					{
						if (PeerMath.isNumeric(c2))
						{
							// do nothing
						}
						else if (PeerMath.isUpperCase(c2))
						{
							while (!PeerMath.isUpperCase((char) mid) && (mid < c2))
							{
								mid++;
							}
						}
						else if (PeerMath.isLowerCase(c2))
						{
							while (!PeerMath.isUpperCase((char) mid) && !PeerMath.isLowerCase((char) mid)	
									&& (mid < c2))
							{
								mid++;
							}
						}
						else
						{
							while (!PeerMath.isUpperCase((char) mid) && !PeerMath.isLowerCase((char) mid))
							{
								if (mid < 'A')
									mid++;
								if (mid > 'z')
									mid--;
							}
						}
					}
					
					/* if neither a char nor a numeric value */
					else
					{
						throw new IllegalArgumentException("Invalid character!");
					}
				}
				
				/* if two characters, c1 equals to c2 */
				else
				{
					mid = c1;
				}
				
				/* if two characters are equal, then add it directly */
				result += ((char) mid);
			}
			
			/* concatenate the remainder substring */
			if (len < maxLen)
			{
				result += str2.substring(len + 1, str2.length());
			}
			return new IndexValue(STRING_TYPE, result);
		}

		throw new ClassCastException("Objects are invalid");		
	}
	
	/**
	 * return index information
	 * 
	 * @return indexInfo
	 */
	public IndexInfo getIndexInfo()
	{
		return this.indexInfo;
	}
	
	/**
	 * Return a readable string for testing or writing in the log file 
	 * 
	 * @return a readable string
	 */	
	public String getString()
	{
		String outMsg;
		
		outMsg = String.valueOf(this.value);
		if (this.indexInfo != null)
			outMsg += "&" + this.indexInfo.toString();
		
		return outMsg;
	}
	
	@Override
	public String toString()
	{
		String outMsg;
		
		outMsg = String.valueOf(this.type + "&" + this.value);
		if (this.indexInfo != null)
			outMsg += "&" + this.indexInfo.toString();
		else
			outMsg += "&null";
				
		return outMsg;		  
	}	
}
