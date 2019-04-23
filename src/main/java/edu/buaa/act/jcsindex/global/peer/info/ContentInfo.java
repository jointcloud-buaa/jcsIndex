/*
 * @(#) ContentInfo.java 1.0 2006-2-7
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.peer.info;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implement the index key set of each node in the BATON tree.
 * 
 * @author Quang Hieu Vu
 * @author (Modified by) Xu Linhao
 * @author (Modified by) Quang Hieu Vu
 * @author (Modified by) David Jiang
 * @version 1.0 2006-2-7
 */

public class ContentInfo implements Serializable 
{
	
	// public members
	/**
	 * If the index key to be inserted lies in the range of 
	 * the min and max index key.
	 */
	public final static int INSERT_NORMALLY   = 0;
	
	/**
	 * If the index key to be inserted is smaller than 
	 * the old min index key.
	 */
	public final static int INSERT_AS_MIN_KEY = 1;
	
	/**
	 * If the index key to be inserted is bigger than
	 * the old max index key.
	 */
	public final static int INSERT_AS_MAX_KEY = 2;
	
	// private members
	private static final long serialVersionUID = 3676792785906596644L;

	private BoundaryValue minValue;
	private BoundaryValue maxValue;

	// TODO: 添加subtree range L and R(测试节点加入和退出，数据是正常的)
	private BoundaryValue subtreeRangeL;
	private BoundaryValue subtreeRangeR;
	
	private int order;
	private Vector<IndexValue> data;

	// TODO: 增加一个Map用来存jcsindex的数据，后续可以在此基础上改进
	private ConcurrentHashMap<Integer, Vector<JcsTuple>> jcsData = new ConcurrentHashMap<>();

	// TODO：增加一个属性表示是否有必要向上搜索
	// 按照我的理解，只需要记录从该节点走的数据的rightBound即可(不一定很有效率)
	private ConcurrentHashMap<Integer, Long> tagLeftSets = new ConcurrentHashMap<>();
	private ConcurrentHashMap<Integer, Long> tagRightSets = new ConcurrentHashMap<>();

	/**
	 * Contruct the set of the index keys at each super node.
	 * 
	 * @param minValue the minimum index value
	 * @param maxValue the maximum index value
	 * @param order the load balance ratio
	 * @param data the set of the index keys
	 */
	public ContentInfo(BoundaryValue minValue, BoundaryValue maxValue, int order, Vector<IndexValue> data) 
	{
	    this.minValue = minValue;
	    this.maxValue = maxValue;
	    this.order = order;
	    this.data  = new Vector<IndexValue>();
	    //Collections.copy(this.data, data);
	    this.data = data;

	    // TODO: 设置默认值
		this.subtreeRangeL = new BoundaryValue(minValue);
		this.subtreeRangeR = new BoundaryValue(maxValue);

		for (int i = 0; i < 8784; i++) {
			jcsData.put(i, new Vector<>());
		}
	}

	// TODO：子树范围一般在节点加入时就已经固定了，因此可以在构造函数中直接生成
	public ContentInfo(BoundaryValue minValue, BoundaryValue maxValue, int order, Vector<IndexValue> data, BoundaryValue srl, BoundaryValue srr) {
		this(minValue, maxValue, order, data);
		this.subtreeRangeL = srl;
		this.subtreeRangeR = srr;
	}

	public BoundaryValue getSubtreeRangeL() {
		return subtreeRangeL;
	}
	public BoundaryValue getSubtreeRangeR() {
		return subtreeRangeR;
	}

	public void setSubtreeRangeL(BoundaryValue subtreeRangeL) {
		if (this.subtreeRangeL.getLongValue() > subtreeRangeL.getLongValue())
			this.subtreeRangeL = subtreeRangeL;
	}

	public void setSubtreeRangeR(BoundaryValue subtreeRangeR) {
		this.subtreeRangeR = subtreeRangeR;
	}

	/**
	 * Construct the set of the index keys with
	 * a serialized string value.
	 * 
	 * @param serializeData the serialized string value
	 */
	public ContentInfo(String serializeData)
	{
	    String[] arrData = serializeData.split("_");
	    try
	    {
	    	this.minValue = new BoundaryValue(arrData[0]);
	    	this.maxValue = new BoundaryValue(arrData[1]);
	    	this.order = Integer.parseInt(arrData[2]);
	    	this.data  = new Vector<IndexValue>();
	    	if (!arrData[3].equals("null"))
	    	{
	    		String[] arrStoredData = arrData[3].split("%");
	    		for (int i = 0; i < arrStoredData.length; i++)
	    		{
	    			this.data.add(new IndexValue(arrStoredData[i]));
	    		}
	    	}
	    }
	    catch (Exception e)
	    {
	    	e.printStackTrace();
	    	System.out.println("Incorrect serialize data at ContentInfo:" + serializeData);
	    }
	}

	// TODO: 添加tagValue的set和get
	public long getLeftTagSets(int timeIndex) {
		// TODO: 设置默认值为0L
		return tagLeftSets.getOrDefault(timeIndex, 0L);
	}

	public long getRightTagSets(int timeIndex) {
		return tagRightSets.getOrDefault(timeIndex, 0L);
	}

	public void setTagLeftSets(JcsTuple tuple) {
		long ans = Math.max(tagLeftSets.getOrDefault(tuple.getTimeIndex(), 0L), tuple.getRightBound());
		tagLeftSets.put(tuple.getTimeIndex(), ans);
	}

	public void setTagRightSets(JcsTuple tuple) {
		long ans = Math.max(tagRightSets.getOrDefault(tuple.getTimeIndex(), 0L), tuple.getLeftBound());
		tagRightSets.put(tuple.getTimeIndex(), ans);
	}

	public boolean isLeftChildOverlap(long leftBound, long rightBound) {
		if (Math.max(leftBound, subtreeRangeL.getLongValue()) < Math.min(rightBound, minValue.getLongValue())) {
			return true;
		} else {
			return false;
		}
	}

	public boolean isRightChildOverlap(long leftBound, long rightBound) {
		if (Math.max(leftBound, maxValue.getLongValue()) < Math.min(rightBound, subtreeRangeR.getLongValue())) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Set the minimum index value.
	 * 
	 * @param minValue the minimum index value
	 */
	public void setMinValue(BoundaryValue minValue)
	{
	    this.minValue = minValue;
	}

	/**
	 * Get the minimum index value.
	 * 
	 * @return the minimum index value
	 */
	public BoundaryValue getMinValue()
	{
	    return this.minValue;
	}

	/**
	 * Set the maximum index value.
	 * 
	 * @param maxValue the maximum index value
	 */
	public void setMaxValue(BoundaryValue maxValue)
	{
	    this.maxValue = maxValue;
	}

	/**
	 * Get the maximum index value.
	 * 
	 * @return the maximum index value
	 */
	public BoundaryValue getMaxValue()
	{
	    return this.maxValue;
	}

	/**
	 * Set the ratio of the load balance.
	 * 
	 * @param order the ratio of the load balance
	 */
	public void setOrder(int order)
	{
	    this.order = order;
	}

	/**
	 * Get the ratio of the load balance.
	 * 
	 * @return the ratio of the load balance
	 */
	public int getOrder()
	{
	    return this.order;
	}

	/**
	 * Get the set of the index value.
	 * 
	 * @return the set of the index key
	 */
	public Vector<IndexValue> getData()
	{
	    return this.data;
	}

	/**
	 * Get the set of jcs index value
	 * @param timeIndex time index
	 * @return
	 */
	public Vector<JcsTuple> getJcsData(int timeIndex) {
		return this.jcsData.get(timeIndex);
	}

	public void setData(Vector<IndexValue> data)
	{
		this.data = data;
	}
	
	/**
	 * Remove all index keys.
	 */
	public void deleteAll()
	{
	    this.data.removeAllElements();
	}

	/**
	 * Insert an index key into the local key set.
	 * 
	 * @param dataValue the index key to be inserted
	 * @param mode  if INSERT_NORMALLY, then directly insert this key;
	 * 				if INSERT_AS_MIN_KEY, then insert this key as the new min key;
	 * 				if INSERT_AS_MAX_KEY, then insert this key as the new max key
	 */
	public void insertData(IndexValue dataValue, int mode)
	{
		System.out.println("HELLO SAM: insertData");
    	IndexValue temptValue, sentinelValue;

    	switch (mode)
	    {
	    case INSERT_NORMALLY:
	    	/* FALL THROUGH */
	    	
	    case INSERT_AS_MIN_KEY:
	    	sentinelValue = dataValue;
	    	if (this.data.size() != 0) 
	    	{
	    		int i = 0;
	    		while (i < this.data.size()) 
	    		{
	    			if (((IndexValue) this.data.get(i)).compareTo(sentinelValue) > 0) 
	    			{
	    				temptValue = (IndexValue)this.data.get(i);
	    				this.data.setElementAt(sentinelValue, i);
	    				sentinelValue = temptValue;
	    			}
	    			i++;
	    		}
	    	}
	    	this.data.add(sentinelValue);

	    	// TODO: 暂时不理解为什么要这么做
	    	/* change min key value */
//	    	if (dataValue.getType() == IndexValue.NUMERIC_TYPE)
//	    		this.minValue.setLongValue(Long.parseLong(dataValue.getKey()));
//	    	else if (dataValue.getType() == IndexValue.STRING_TYPE)
//	    		this.minValue.setStringValue(dataValue.getKey());
	    	
	    	break;
	    	
	    case INSERT_AS_MAX_KEY:
	    	this.data.add(dataValue);
	    	
	    	/* change max key value */
	    	if (dataValue.getType() == IndexValue.NUMERIC_TYPE)
	    		this.maxValue.setLongValue(Long.parseLong(dataValue.getKey()));
	    	else if (dataValue.getType() == IndexValue.STRING_TYPE)
	    		this.maxValue.setStringValue(dataValue.getKey());
	    	
	    	break;
	    }
	}

	public boolean satisfyRange(JcsTuple tupleValue) {
		//　TODO: just for test
		// System.out.println("L: " + subtreeRangeL.getLongValue() + " R: " + subtreeRangeR.getLongValue() + " tuple: " + tupleValue.toString());
		return subtreeRangeL.getLongValue() <= tupleValue.getLeftBound() &&  tupleValue.getRightBound() <= subtreeRangeR.getLongValue();
	}

	// TODO：插入数据，插入一个Map<Integer, List<>>里面
	public void insertJcsTuple(JcsTuple tupleValue)
	{
		// TODO: 输出语句，便于调试
		JcsTuple temptValue, sentinelValue;
		sentinelValue = tupleValue;
		Vector arr = this.jcsData.get(sentinelValue.getTimeIndex());
		if (arr != null && arr.size() != 0)
		{
			int i = 0;
			while (i < arr.size())
			{
				// 后续考虑是否把Vector改成LinkedList，这里执行的操作实际上是执行一个有序的插入，使用数组影响效率
				if (((JcsTuple) arr.get(i)).compareTo(sentinelValue) > 0)
				{
					temptValue = (JcsTuple) arr.get(i);
					arr.setElementAt(sentinelValue, i);
					sentinelValue = temptValue;
				}
				i++;
			}
		} else if (arr == null) {
			// 变成ConcurrentHashMap, 优先初始化后，理论上不会执行这一行
			jcsData.put(sentinelValue.getTimeIndex(), new Vector<>());
			arr = jcsData.get(sentinelValue.getTimeIndex());
		}
		arr.add(sentinelValue);
	}

	public Set<String> localSearch(int timeIndex, long leftBound, long rightBound) {
		Set<String> ans = new HashSet<>();
		Vector<JcsTuple> selected = jcsData.getOrDefault(timeIndex, null);
		if (selected == null) {
			return ans;
		}
		for (int i = 0; i < selected.size(); i++) {
			JcsTuple tuple = selected.get(i);
			if (Math.max(leftBound, tuple.getLeftBound()) < Math.min(rightBound, tuple.getRightBound())) {
				ans.add(tuple.getDest());
			}
		}
		return ans;
	}

	public void deleteJcs(JcsTuple tuple) {
		int timeIndex = tuple.getTimeIndex();
		Vector<JcsTuple> arr = jcsData.get(timeIndex);
		for (int i = 0; i < arr.size(); i++) {
			if (tuple.equals(arr.get(i))) {
				arr.remove(i);
				return;
			}
		}
	}


	/**
	 * Delete an index key from the key set.
	 * 
	 * @param dataValue the index key to be removed
	 */
	public void deleteData(IndexValue dataValue)
	{
		for (int i = 0; i < this.data.size(); i++)
		{
			if (dataValue.compareTo((IndexValue)this.data.get(i)) == 0)
			{
				this.data.remove(i);
				return;
			}
	    }
	}

	/**
	 * Check if the value is in the range
	 */
	public boolean isInRange(String value){
		if (value.compareTo(minValue.getStringValue()) >= 0 &&
				value.compareTo(maxValue.getStringValue()) < 0)
			return true;
		return false;
	}
	
	/**
	 * Check if the value is less than the range
	 * A value is less than the range if it is less than the lower
	 * bound of that range
	 */
	public boolean isLessThanRange(String value){
		if(value.compareTo(minValue.getStringValue()) < 0)
			return true;
		return false;
	}
	
	/**
	 * Check if the value is greater than the range
	 */
	public boolean isGreaterThanRange(String value){
		if(value.compareTo(maxValue.getStringValue()) >= 0)
			return true;
		return false;
	}
	
	/**
	 * Compare the value with the range
	 * >0 for greater
	 * =0 for equal
	 * <0 for less than
	 */
	public int compareTo(String value){
		if(value.compareTo(minValue.getStringValue()) < 0)
			return -1;
		if(value.compareTo(maxValue.getStringValue()) < 0)
			return 0;
		return 1;
	}
	/**
	 * FIXME: Determine if the node is overload.
	 * 
	 * @return if overload, return <code>true</code>;
	 * 			otherwise, return <code>false</code>
	 */
	public boolean isOverloaded()
	{
	    /*
	     * personal comments:
	     * I suggest that this method should be
	     * changed to the class ServerPeer. On
	     * the other side, isUnderloaded() method
	     * should be provided.
	     */
		return this.data.size() >= 2 * this.order;
	}
	
	@Override
	public String toString()
	{
	    String outMsg;
	    outMsg = this.minValue + "_" + this.maxValue + "_" + this.order;
	    if (this.data.size() == 0)
	    	outMsg += "_null";
	    else
	    {
	    	outMsg += "_";
	    	for (int i = 0; i < this.data.size() - 1; i++)
	    	{
	    		outMsg += ((IndexValue)this.data.get(i)).toString() + "%";
	    	}
	    	outMsg += ((IndexValue)this.data.get(this.data.size()-1)).toString();
	    }
	    return outMsg;
	}

	public String formatedString() {
		StringBuilder sb = new StringBuilder();
		sb.append("(").append(this.minValue).append(", ").append(this.maxValue).append(")\n");
		if (this.data.size() == 0) {
			sb.append("No Data");
		} else {
			sb.append("Items: \n");
			for (int i = 0; i < this.data.size(); i++){
				sb.append("(").append(this.data.get(i).getKey()).append(", ").append(this.data.get(i).getIndexInfo().getValue()).append(")");
				if (i != this.data.size() - 1) {
					sb.append("\n");
				}
			}
		}
		return sb.toString();
	}

	public String formatTupleString() {
		String outMsg = "";
		for (Map.Entry<Integer, Vector<JcsTuple>> entry : jcsData.entrySet()) {
			outMsg += "TimeIndex: " + entry.getKey() + "\n";
			for (JcsTuple tuple : entry.getValue()) {
				outMsg += tuple.toString() + "\n";
			}
		}
		return outMsg;
	}

	/**
	 * Get the serialized representation of the <code>ContentInfo</code>.
	 * 
	 * @return the serialized representation of the <code>ContentInfo</code>
	 */
	public String serialize()
	{
	    String outMsg;
	    outMsg = "MinValue=" + this.minValue + ",MaxValue=" + this.maxValue + ",Order=" + this.order;
	    if (this.data.size() == 0)
	    	outMsg += ",Data=null";
	    else
	    {
	    	outMsg += ",Data=";
	    	for (int i = 0; i < this.data.size() - 1; i++)
	    	{
	    		outMsg += ((IndexValue)this.data.get(i)).toString() + ":";
	    	}
	    	outMsg += ((IndexValue)this.data.get(this.data.size()-1)).toString();
	    }
	    return outMsg;
	}
	
}