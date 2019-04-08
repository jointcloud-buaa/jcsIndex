/**
 * Created on Sep 1, 2008
 */
package edu.buaa.act.jcsindex.global.utils;

import edu.buaa.act.jcsindex.global.peer.info.BoundaryValue;

/**
 * Implement a range for range query
 * @author David Jiang
 *
 */
public class Range {
	private BoundaryValue minValue;
	private BoundaryValue maxValue;

	public Range(BoundaryValue min, BoundaryValue max){
		setMinValue(min); 	setMaxValue(max);
	}

	/**
	 * @param minValue the minValue to set
	 */
	public void setMinValue(BoundaryValue minValue) {
		this.minValue = minValue;
	}

	/**
	 * @return the minValue
	 */
	public BoundaryValue getMinValue() {
		return minValue;
	}

	/**
	 * @param maxValue the maxValue to set
	 */
	public void setMaxValue(BoundaryValue maxValue) {
		this.maxValue = maxValue;
	}

	/**
	 * @return the maxValue
	 */
	public BoundaryValue getMaxValue() {
		return maxValue;
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

}
