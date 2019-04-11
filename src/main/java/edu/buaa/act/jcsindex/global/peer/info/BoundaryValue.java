package edu.buaa.act.jcsindex.global.peer.info;

import edu.buaa.act.jcsindex.global.utils.PeerMath;

import java.io.Serializable;

/**
 * Implement a bound value for a range
 * 
 *
 */
public class BoundaryValue implements Comparable, Serializable
{

	private static final long serialVersionUID = 2522267626691282176L;
	
	/**
	 * Between the upper case and lower case, there are six additional
	 * characters, and hence when calculating the average value of two
	 * <code>String</code> we simply use this OFFSET value to adjust
	 * the median value of two characters.
	 */
	public final static int OFFSET = 3;
	
	private String stringValue;
	private long longValue;
	
	public BoundaryValue(String stringValue, long longValue)
	{
		this.stringValue = stringValue;
		this.longValue = longValue;
	}

	// TODO: 增加一个构造函数
	public BoundaryValue(BoundaryValue that) {
		this.stringValue = that.getStringValue();
		this.longValue = that.getLongValue();
	}
	
	public BoundaryValue(String serializeData)
	{
		String[] arrData = serializeData.split("$");
	    try
	    {
	    	this.stringValue = arrData[0];
	    	this.longValue = Long.parseLong(arrData[1]);
	    }
	    catch(Exception e)
	    {
	    	e.printStackTrace();
	    	System.out.println("Incorrect serialize data at BoundaryValue:" + serializeData);
	    }
	}
	
	public void setStringValue(String stringValue)
	{
		this.stringValue = stringValue;
	}
	
	public String getStringValue()
	{
		return this.stringValue;
	}
	
	public void setLongValue(long longValue)
	{
		this.longValue = longValue;
	}
	
	public long getLongValue()
	{
		return this.longValue;
	}
	
	public static BoundaryValue average(BoundaryValue value1, BoundaryValue value2)
	{
		// TODO: 有溢出风险，需要修改
		// long midLongValue = (long) ((value1.getLongValue() + value2.getLongValue()) / 2);
		long midLongValue = (value1.getLongValue() & value2.getLongValue()) + ((value1.getLongValue() ^ value2.getLongValue()) >> 1);
		
		String str1 = value1.getStringValue();
		String str2 = value2.getStringValue();
			
		/* 
		 * let the shorter string is str1, 
		 * and the longer one is str2 
		 */
		if (str1.length() > str2.length())
		{
			str1 = value2.getStringValue();
			str2 = value1.getStringValue();
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
		
		return new BoundaryValue(result, midLongValue);				
	}
	
	public int compareTo(IndexValue compareValue) throws ClassCastException
	{	
		if (compareValue.getType() == IndexValue.NUMERIC_TYPE)
			return (Long.valueOf(this.longValue)).compareTo(Long.valueOf(compareValue.getString()));
		else if (compareValue.getType() == IndexValue.STRING_TYPE)
			return (this.stringValue).compareTo(compareValue.getString());

		throw new ClassCastException("Object is invalid");		
	}
	
	public int compareTo(Object compareObject) throws ClassCastException
	{	
		if (compareObject instanceof BoundaryValue)
		{
			BoundaryValue compareValue = (BoundaryValue) compareObject;
			if (this.longValue != compareValue.longValue)
				return (Long.valueOf(this.longValue)).compareTo(Long.valueOf(compareValue.longValue));
			else
				return (this.stringValue).compareTo(compareValue.stringValue);			
		} else if (compareObject instanceof JcsTuple) {
			// TODO: 增加与JcsTuple的比较
			return ((Long ) this.longValue).compareTo(((JcsTuple ) compareObject).getLeftBound());
		}
		throw new ClassCastException("Object is invalid");		
	}
	
	public String getString()
	{
		String outMsg;
		
		outMsg = this.stringValue;
		outMsg += ":" + this.longValue;
		
		return outMsg;
	}
	
	@Override
	public String toString()
	{
		String outMsg;
		
		outMsg = this.stringValue;
		outMsg += "$" + this.longValue;
		
		return outMsg;
	}
}
