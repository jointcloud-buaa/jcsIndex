/*
 * @(#) PeerMath.java 1.0 2006-2-6
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.utils;

import edu.buaa.act.jcsindex.global.peer.info.RoutingItemInfo;
import edu.buaa.act.jcsindex.global.peer.info.RoutingTableInfo;

/**
 * A utility used for constructing the BATON tree 
 * and checking the correctness of the BATON tree.
 * 
 * @author Vu Quang Hieu
 * @author (Modified by) Xu Linhao
 * @version 1.0 2006-2-6
 */

public class PeerMath 
{

	/**
	 * Calculate the power of two integer numbers.
	 * 
	 * @param x the base number
	 * @param p the power number
	 * @return the result of the power of two integer number
	 */
	public static int pow(int x, int p) 
	{
		if (p == 0)
			return 1;
		
		/* if y is even */
		else if ((p % 2) == 0)
			return pow(x*x, p/2);
		
		/* if y is odd */
		else					
			return (x * pow(x*x, p/2));
	}

	/**
	 * Returns <code>true</code> if a character is a upper case.
	 * 
	 * @param c the character to be checked
	 * @return if the character is a upper case, return <code>true</code>;
	 * 			otherwise, return <code>false</code>
	 */
	public static boolean isUpperCase(char c)
	{
		if ((c >= 65) && (c <= 90))
			return true;
		return false;
	}
	
	/**
	 * Returns <code>true</code> if a character is a lower case.
	 * 
	 * @param c the character to be checked
	 * @return if the character is a lower case, return <code>true</code>;
	 * 			otherwise, return <code>false</code>
	 */
	public static boolean isLowerCase(char c)
	{
		if ((c >= 97) && (c <= 122))
			return true;
		return false;
	}
	
	/**
	 * Returns <code>true</code> if a character is a numeric value.
	 * 
	 * @param c the character to be checked
	 * @return if the character is a numeric value, return <code>true</code>;
	 * 			otherwise, return <code>false</code>
	 */
	public static boolean isNumeric(char c)
	{
		if ((c >= 48) && (c <= 57))
			return true;
		return false;
	}
	
	/**
	 * Returns the size of the routing table of a peer that is specified by
	 * the position and level of the BATON tree.
	 * 
	 * @param number the position of the node in the specified level of the tree
	 * @param level the level of the node in the tree
	 * @param direction <code>true</code> for calculating the size of the right 
	 * 					routing table; <code>false</code> for calculating the size
	 * 					of the left routing table 
	 * @return the size of the routing table
	 */
	public static int getRoutingTableSize(int number, int level, boolean direction)
	{
		int size = 1;
		int step = 1;
		if (!direction)
		{
			while ((number - step) >= 1) 
			{
				size = size + 1;
				step = step * 2;
			}
			return size - 1;
		}
		else
		{
			int maxNode = PeerMath.pow(2, level);
			while ((number + step) <= maxNode)
			{
				size = size + 1;
				step = step * 2;
			}
			return size - 1;
		}
	}

	/**
	 * Returns the power distance between two nodes that lies in the 
	 * same level of the tree.
	 * 
	 * @param number1 the number of the first node
	 * @param number2 the number of the second node
	 * @return the power distance between two nodes
	 */
	public static int getPowerDistance(int number1, int number2)
	{
		int realDistance = Math.abs(number1 - number2);
		int i = 1;
		int powerDistance = 0;
		while (i != realDistance)
		{
			powerDistance++;
			i *= 2;
		}
		return powerDistance;
	}

	/**
	 * Returns <code>true</code> if two ranges are overlapped.
	 * 
	 * @param min1 the minimum value of the first range
	 * @param max1 the maximum value of the first range
	 * @param min2 the minimum value of the second range
	 * @param max2 the maximum value of the second range
	 * @return <code>true</code> if two ranges are overlapped;
	 * 			otherwise, return <code>false</code>
	 */
	public static boolean isOverlapped(int min1, int max1, int min2, int max2)
	{
		if (max1 < min2) 
			return false;	
		else if (max2 > min1) 
			return false;
		return true;
	}

	/**
	 * Returns <code>true</code> if a node has an ancestor on the specified side.
	 * 
	 * @param number the number of the node in the level of the tree
	 * @param level the level of the node in the tree
	 * @param direction <code>true</code> indicates the right hand side;
	 * 					otherwise indicates the left hand side
	 * @return <code>true</code> if the ancestor of the node is found;
	 *			otherwise, return <code>false</code>
	 */
	public static boolean checkAncesstor(int number, int level, boolean direction)
	{
		int parentLevel = level - 1;
		int temptNumber = number;
		int parentNumber = temptNumber / 2 + temptNumber % 2;
		if (!direction)
		{
			//check left hand side
			while ( (temptNumber / 2 < parentNumber) && (parentLevel >= 0)) 
			{
				temptNumber = parentNumber;
				parentNumber = temptNumber / 2 + temptNumber % 2;
				parentLevel--;
			}
		}
		else
		{
			//check right hand side
			while ( (temptNumber / 2 == parentNumber) && (parentLevel >= 0)) 
			{
				temptNumber = parentNumber;
				parentNumber = temptNumber / 2 + temptNumber % 2;
				parentLevel--;
			}
		}
		if (parentLevel>=0)
			return true;
		else
			return false;
	}

	/**
	 * Returns <code>true</code> if a node falls in a subtree specified by 
	 * its leftmost node and its rightmost node.
	 * 
	 * @param level the level of the node to be checked
	 * @param number the number of the node to be checked
	 * @param leftmostLevel the level of the leftmost node
	 * @param leftmostNumber the number of the leftmost node
	 * @param rightmostLevel the level of the rightmost node
	 * @param rightmostNumber the number of the rightmost node
	 * @return if the node falls in the subtree, return <code>true</code>;
	 * 			otherwise, return <code>false</code>
	 */
	public static boolean checkNode(int level, int number, int leftmostLevel, int leftmostNumber, int rightmostLevel, int rightmostNumber)
	{
		int maxLevel = level;
		if (leftmostLevel > maxLevel) 
		{
			maxLevel = leftmostLevel;
		}
		
		if (rightmostLevel > maxLevel) 
		{
			maxLevel = rightmostLevel;
		}
		
		while (leftmostLevel < maxLevel)
		{
			leftmostLevel++;
			leftmostNumber = leftmostNumber * 2 - 1;
		}
		
		while (rightmostLevel < maxLevel)
		{
			rightmostLevel++;
			rightmostNumber = rightmostNumber * 2;
		}
		
		while (level < maxLevel)
		{
			level++;
			number = number * 2;
		}
		
		if ((leftmostNumber <= number) && (number <= rightmostNumber)) 
			return true;
		return false;
	}

	/**
	 * Returns the new position of a node.
	 * 
	 * @param ip the IP address of the node
	 * @param level the level of the node
	 * @param number the number of the node
  	 * @param direction <code>true</code> if the node is shifted to the left;
  	 * 					otherwise, if the node is shifted to the right
  	 * @return the new position of the node with the format "Level&Number&IPAddress"
  	 */
	public static String calculateNewPosition(String ip, int level,	int number, boolean direction)
	{
		if (direction)
		{
			//nodes shift to the left, rotate to the right
			while ((number % 2) != 0)
			{
				number = (number + 1) / 2;
				level--;
			}
			//new node position
			number = number / 2;
			level--;
		}
		else
		{
			//nodes shift to the right, rotate to the left
			while ((number % 2) == 0)
			{
				number = number / 2;
				level--;
			}
			//new node position
			number = (number + 1) / 2;
			level--;
		}
		return level + "&" + number + "&" + ip;
	}

	/**
	 * Returns the string representation of the IP address of the parent node, 
	 * the left child node or the right child node from the existing knowledge.
	 * 
	 * @param knowledge the existing knowledge of the position and IP address of all nodes
	 * @param level the level of the node
	 * @param number the number of the node
	 * @param type  1 indicates the IP address of the parent node is found;
	 * 				2 indicates the IP address of the left child node is found;
	 * 				3 indicates the IP address of the right child node is found
	 * @return if found, return the IP address; otherwise, return "-1"
	 */
	public static String findPosition(String knowledge, int level, int number, int type)
	{
		int findNodeAtLevel  = 0;
		int findNodeAtNumber = 0;	
		switch (type)
		{
		case 1:
			findNodeAtLevel = level - 1;
			findNodeAtNumber = number / 2 + number % 2;
			break;
		case 2:
			findNodeAtLevel = level + 1;
			findNodeAtNumber = number * 2 - 1;
			break;
		case 3:
			findNodeAtLevel = level + 1;
			findNodeAtNumber = number * 2;
		}
		String position = "_" + findNodeAtLevel + "&" + findNodeAtNumber + "&";
		int index = knowledge.indexOf(position);
		int indexStop = 0;
		if (index != -1)
		{
			indexStop = knowledge.indexOf("_", index + position.length());
			if (indexStop == -1) 
			{
				indexStop = knowledge.length();
			}
			return knowledge.substring(index + position.length(), indexStop);
		}
		else
		{
			position = findNodeAtLevel + "&" + findNodeAtNumber + "&";
			if (knowledge.indexOf(position) == 0)
			{
				indexStop = knowledge.indexOf("_", position.length());
				if (indexStop == -1) 
				{
					indexStop = knowledge.length();
				}
				return knowledge.substring(position.length(), indexStop);
			}
			else
				return "-1";
		}
	}

	/**
	 * Returns the string representation of all elements after a new element
	 * is added into the existing list separated by an underscore.
	 * 
	 * @param element the element to be added
	 * @param list the existing list
	 * @param special <code>false</code> if the new element is added to first position;
	 * 					otherwise, it is added to last position
	 * @return the string representation of all elements after a new element
	 * 			is added into the existing list separated by an underscore
	 */
	public static String addElementToList(String element, String list, boolean special)
	{
		if (list.equals("")) 
			return element;
		else
		{
			if (!special)
			{
				return (element + "_" + list);
			}
			else
			{
				int underscoreindex = list.indexOf("_");
				if (underscoreindex == -1)
				{
					return (list + "_" + element);
				}
				else
				{
					return list.substring(0, underscoreindex + 1) + element + "_" +
								list.substring(underscoreindex + 1, list.length());
				}
			}
		}
	}

	/**
	 * Returns <code>true</code> if the left node and the right node fall 
	 * in the leftmost branch and the rightmost branch of the same subtree.
	 * 
	 * @param leftLevel the level of the left node
	 * @param leftNumber the number of the left node
	 * @param rightLevel the level of the right node
	 * @param rightNumber the number of the right node
	 * @return if in the same subtree, return <code>true</code>;
	 * 			otherwise, return <code>false</code>
	 */
	public static boolean checkNodesSameTree(int leftLevel, int leftNumber,	int rightLevel, int rightNumber)
	{
		while ((leftLevel != rightLevel) || (leftNumber != rightNumber))
		{
			if (leftLevel < rightLevel)
			{
				if (rightNumber % 2 != 0) 
					return false;
				
				rightLevel--;
				rightNumber = rightNumber / 2;
			}
			else if (leftLevel > rightLevel)
			{
				if (leftNumber % 2 != 1) 
					return false;
				
				leftLevel--;
				leftNumber = (leftNumber + 1) / 2;
			}
			else
			{
				if ((leftNumber > rightNumber) || (leftNumber % 2 != 1)
						|| (rightNumber % 2 != 0)) 
					return false;
				
				leftLevel--;
				rightLevel--;
				leftNumber  = (leftNumber + 1) / 2;
				rightNumber = rightNumber / 2;
			}
		}
		return true;
	}

	/**
	 * Returns <code>true</code> if the first node is at the left side of the second node.
	 * 
	 * @param level1 the level of the first node
	 * @param number1 the number of the first node
	 * @param level2 the level of the second node
	 * @param number2 the number of the second node
	 * @return if the first node is at the left side of the second node, return <code>true</code>;
	 * 			otherwise, return <code>false</code>
	 */
	public static boolean compareNodePosition(int level1, int number1, int level2, int number2)
	{
		boolean flag = (level1 < level2);
		while (level1 != level2)
		{
			if (level1 < level2)
			{
				level1++;
				number1 = number1 * 2;
			}
			else
			{
				level2++;
				number2 = number2 * 2;
			}
		}
		if (number1 == number2) return flag;
		else return (number1 < number2);
	}

	/**
	 * Returns the number of the expected reply message for a new node in the course of
	 * constructing its routing table.
	 * 
	 * @param isLeftChild <code>true</code> indicates the new node is the left child;
	 * 						otherwise indicates the right child
	 * @param hasSibling  <code>true</code> indicates the new node has a sibling;
	 * 						otherwise indicates none
	 * @param leftRT the left routing table of the new node
	 * @param rightRT the right routing table of the new node
	 * @return the number of the expected reply message for a new node in the course of
	 * 			constructing its routing table
	 */
	public static int getNumberOfExpectedRTReply(boolean isLeftChild, boolean hasSibling,
                                                 RoutingTableInfo leftRT, RoutingTableInfo rightRT)
	{
		RoutingItemInfo tmpInfo = null;
		int i = 0;
		int total = 0;

		if (hasSibling) 
		{
			total++;
		}
		
		if (isLeftChild)
		{
			if (leftRT.getTableSize() > 0)
			{
				tmpInfo = leftRT.getRoutingTableNode(0);
				if (tmpInfo != null)
				{
					if (tmpInfo.getLeftChild() != null) 
						total++;
					if (tmpInfo.getRightChild() != null) 
						total++;
				}
			}
			for (i = 1; i < leftRT.getTableSize(); i++)
			{
				tmpInfo = leftRT.getRoutingTableNode(i);
				if (tmpInfo != null)
				{
					if (tmpInfo.getLeftChild() != null) 
						total++;
				}
			}
			for (i = 0; i < rightRT.getTableSize(); i++)
			{
				tmpInfo = rightRT.getRoutingTableNode(i);
				if (tmpInfo != null)
				{
					if (tmpInfo.getLeftChild() != null) 
						total++;
				}
			}
		}
		else
		{
			if (rightRT.getTableSize() > 0)
			{
				tmpInfo = rightRT.getRoutingTableNode(0);
				if (tmpInfo != null)
				{
					if (tmpInfo.getLeftChild() != null) 
						total++;
					if (tmpInfo.getRightChild() != null) 
						total++;
				}
			}
			for (i = 0; i < leftRT.getTableSize(); i++)
			{
				tmpInfo = leftRT.getRoutingTableNode(i);
				if (tmpInfo != null)
				{
					if (tmpInfo.getRightChild() != null) 
						total++;
				}
			}
			for (i = 1; i < rightRT.getTableSize(); i++)
			{
				tmpInfo = rightRT.getRoutingTableNode(i);
				if (tmpInfo != null)
				{
					if (tmpInfo.getRightChild() != null) 
						total++;
				}
			}
		}

		return total;
	}

	/**
	 * Returns <code>true</code> if two nodes are neighbor.
	 * 
	 * @param number1 the number of the first node
	 * @param number2 the number of the second node
	 * @return if they are neighbor, return <code>true</code>;
	 * 			otherwise, return <code>false</code>
	 */
	public static boolean checkNeighbor(int number1, int number2)
	{
		int distance = Math.abs(number1 - number2);
		int legaldistance = 1;
		while (legaldistance < distance)
		{
			legaldistance = legaldistance * 2;
		}
		if (legaldistance == distance) 
			return true;
		return false;
	}  
	
}
