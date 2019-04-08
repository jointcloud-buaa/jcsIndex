/*
 * @(#) SPJoinAcceptListener.java 1.0 2006-3-1
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.peer.event;

import edu.buaa.act.jcsindex.global.AbstractInstance;
import edu.buaa.act.jcsindex.global.peer.ServerPeer;
import edu.buaa.act.jcsindex.global.peer.info.ParentNodeInfo;
import edu.buaa.act.jcsindex.global.peer.info.RoutingTableInfo;
import edu.buaa.act.jcsindex.global.peer.info.TreeNode;
import edu.buaa.act.jcsindex.global.peer.management.EventHandleException;
import edu.buaa.act.jcsindex.global.protocol.Message;
import edu.buaa.act.jcsindex.global.protocol.MsgType;
import edu.buaa.act.jcsindex.global.protocol.body.SPJoinAcceptBody;
import edu.buaa.act.jcsindex.global.utils.PeerMath;

import java.io.ObjectOutputStream;

/**
 * Implement a listener for processing when peer is accepted to join the network.
 * 
 */

public class SPJoinAcceptListener extends ActionAdapter
{
	public SPJoinAcceptListener(AbstractInstance instance)
	{
		super(instance);
	}

	public void actionPerformed(ObjectOutputStream oos, Message msg) throws EventHandleException 
	{		
		super.actionPerformed(oos, msg);
		
		try
		{
			/* get the handler of the ServerPeer */
			ServerPeer serverpeer = (ServerPeer) instance.peer();
			
			/* get the message body */
			SPJoinAcceptBody body = (SPJoinAcceptBody) msg.getBody();
			
			ParentNodeInfo parentNode;
		    int nodeNumber, nodeLevel, leftRTSize, rightRTSize, status = 0;	    

		    if (body.getLogicalSender() == null) parentNode = null;
		    else parentNode = new ParentNodeInfo(body.getPhysicalSender(), body.getLogicalSender());
		    nodeNumber = body.getNewNodePosition().getNumber();
		    nodeLevel = body.getNewNodePosition().getLevel();
		    leftRTSize = PeerMath.getRoutingTableSize(nodeNumber, nodeLevel, false);
		    rightRTSize = PeerMath.getRoutingTableSize(nodeNumber, nodeLevel, true);
		    if (body.getNumOfExpectedRTReply() == 0) status = 1;

		    SPGeneralAction.saveData(body.getContent().getData());
		    
		    //create new node and add to the existing list node
		    TreeNode treeNode =
		        new TreeNode(body.getNewNodePosition(), parentNode, null, null, 
		        		body.getLeftAdjacent(), body.getRightAdjacent(),
		        		new RoutingTableInfo(leftRTSize),
		        		new RoutingTableInfo(rightRTSize), body.getContent(),		                     
		        		body.getNumOfExpectedRTReply(), status);

			serverpeer.removeListItem(0);
		    serverpeer.addListItem(treeNode);
		    
		    if (!body.getIsFake())
		    {
//			    serverpeer.performSuccessJoinRequest();
				System.out.println("***serverpeer.performSuccessJoinRequest()");
		    }
		    else{
		    	boolean result = SPGeneralAction.transferFakeNode(serverpeer, treeNode, 
		    			body.getDirection(), true, body.getPhysicalSender());
		    	ActivateActiveStatus activateActiveStatus 
		    		= new ActivateActiveStatus(treeNode, serverpeer.TIME_TO_STABLE_STATE);
		    }
		    
//		    //	examine the indexed data, and republish it, if they are not in the range
//		    Connection conn = ServerPeer.conn_bestpeerindexdb;
//		    try{
//		    	Statement stmt = conn.createStatement();
//		    	stmt.setFetchSize(1000);
//		    	Hashtable<PhysicalInfo, Vector<LocalTableIndex>> tables = new Hashtable<PhysicalInfo, Vector<LocalTableIndex>>();
//		    	ResultSet rs = stmt.executeQuery("select * from table_index");
//		    	Vector<String> delete = new Vector<String>();
//		    	while(rs.next())
//		    	{
//		    		String key = rs.getString("ind");
//		    		if(key.compareTo(treeNode.getContent().getMinValue().getStringValue())>=0 ||
//				       key.compareTo(treeNode.getContent().getMaxValue().getStringValue())<0)
//				    	continue;
//		    		String sql = "delete from table_index where ind='" + key + "'";
//		    		delete.add(sql);
//		    		String val = rs.getString("val");
//		    		String[] ips = val.split(":");
//		    		for(int i=0; i<ips.length; i++)
//		    		{
//		    			PhysicalInfo next = new PhysicalInfo(ips[i]);
//		    			if(tables.containsKey(next))
//		    			{
//		    				Vector<LocalTableIndex> nextTables = tables.get(next);
//		    				nextTables.add(new LocalTableIndex(key));
//		    			}
//		    			else
//		    			{
//		    				Vector<LocalTableIndex> nextTables = new Vector<LocalTableIndex>();
//		    				nextTables.add(new LocalTableIndex(key));
//		    				tables.put(next, nextTables);
//		    			}
//		    		}
//		    	}
//		    	rs.close();
//		    	//stmt.close();
//		    	//send out
//		    	Enumeration enu = tables.keys();
//		    	while(enu.hasMoreElements())
//		    	{
//		    		PhysicalInfo next = (PhysicalInfo)enu.nextElement();
//		    		ERPInsertTableIndexBody mybody = new ERPInsertTableIndexBody(serverpeer.getPhysicalInfo(),
//                                                         next, tables.get(next), 
//                                                         treeNode.getLogicalInfo());
//		    		Head head = new Head();
//		    		head.setMsgType(MsgType.ERP_INSERT_TABLE_INDEX.getValue());
//		    		serverpeer.sendMessage(serverpeer.getPhysicalInfo(), new Message(head, mybody));
//		    	}
//		    	tables.clear();
//		    	
//		    	for(int i=0; i<delete.size(); i++)
//		    	{
//		    		stmt.addBatch(delete.get(i));
//		    	}
//		    	stmt.executeBatch();
//		    	stmt.close();
//		    }
//		    catch(Exception e1)
//		    {
//		    	e1.printStackTrace();
//		    }
//		    
//		    
//		    try{
//		    	Statement stmt = conn.createStatement();
//		    	stmt.setFetchSize(1000);
//		    	Hashtable<PhysicalInfo, Vector<LocalColumnIndex>> columns = new Hashtable<PhysicalInfo, Vector<LocalColumnIndex>>();
//		    	ResultSet rs = stmt.executeQuery("select * from column_index");
//		    	Vector<String> delete = new Vector<String>();
//		    	while(rs.next())
//		    	{
//		    		String key = rs.getString("ind");
//		    		if(key.compareTo(treeNode.getContent().getMinValue().getStringValue())>=0 ||
//		    		   key.compareTo(treeNode.getContent().getMaxValue().getStringValue())<0)
//		    			continue;
//		    		String sql = "delete from column_index where ind='" + key + "'";
//		    		delete.add(sql);
//		    		String val = rs.getString("val");
//		    		String[] ips = val.split(":");
//		    		for(int i=0; i<ips.length; i++)
//		    		{
//		    			String[] data = ips[i].split("_");
//		    			PhysicalInfo next = new PhysicalInfo(data[0]+"_"+data[1]);
//		    			if(columns.containsKey(next))
//		    			{
//		    				Vector<LocalColumnIndex> nextColumn = columns.get(next);
//		    				boolean flag = false;
//		    				for(int j=0; j<nextColumn.size(); j++)
//		    				{
//		    					LocalColumnIndex idx = nextColumn.get(j);
//		    					if(idx.getColumnName().equals(key))
//		    					{
//		    						if(!idx.getListOfTables().contains(data[2]))
//		    							idx.getListOfTables().add(data[2]);
//		    						flag = true;
//		    						break;
//		    					}		    					
//		    				}
//		    				if(!flag)
//		    				{
//		    					LocalColumnIndex cindex = new LocalColumnIndex(key, new Vector<String>());
//		    					cindex.getListOfTables().add(data[2]);
//		    					nextColumn.add(cindex);
//		    				}
//		    			}
//		    			else
//		    			{
//		    				Vector<LocalColumnIndex> nextColumn = new Vector<LocalColumnIndex>();
//		    				LocalColumnIndex cindex = new LocalColumnIndex(key, new Vector<String>());
//	    					cindex.getListOfTables().add(data[2]);
//	    					nextColumn.add(cindex);
//	    					columns.put(next, nextColumn);
//		    			}
//		    		}
//		    	}
//		    	rs.close();
//		    	//stmt.close();
//		    	//send out
//		    	Enumeration enu = columns.keys();
//		    	while(enu.hasMoreElements())
//		    	{
//		    		PhysicalInfo next = (PhysicalInfo)enu.nextElement();
//		    		ERPInsertColumnIndexBody mybody = new ERPInsertColumnIndexBody(serverpeer.getPhysicalInfo(),
//                            							next, columns.get(next),
//                            							treeNode.getLogicalInfo());
//		    		Head head = new Head();
//		    		head.setMsgType(MsgType.ERP_INSERT_COLUMN_INDEX.getValue());
//		    		serverpeer.sendMessage(serverpeer.getPhysicalInfo(), new Message(head, mybody));
//		    	}
//		    	columns.clear();
//		    	
//		    	for(int i=0; i<delete.size(); i++)
//		    	{
//		    		stmt.addBatch(delete.get(i));
//		    	}
//		    	stmt.executeBatch();
//		    	stmt.close();
//		    }
//		    catch(Exception e1)
//		    {
//		    	e1.printStackTrace();
//		    }
//		    
//		    try{
//		    	Statement stmt = conn.createStatement();
//		    	stmt.setFetchSize(1000);
//		    	Hashtable<PhysicalInfo, Vector<LocalDataIndex>> tuples = new Hashtable<PhysicalInfo, Vector<LocalDataIndex>>();
//		    	ResultSet rs = stmt.executeQuery("select * from data_index");
//		    	Vector<String> delete = new Vector<String>();
//		    	while(rs.next())
//		    	{
//		    		String key = rs.getString("ind");
//		    		if(key.compareTo(treeNode.getContent().getMinValue().getStringValue())>=0 ||
//				       key.compareTo(treeNode.getContent().getMaxValue().getStringValue())<0)
//				    	continue;
//		    		String sql = "delete from data_index where ind='" + key + "'";
//		    		delete.add(sql);
//		    		String val = rs.getString("val");
//		    		String[] ips = val.split(":");
//		    		for(int i=0; i<ips.length; i++)
//		    		{
//		    			String[] data = ips[i].split("_");
//		    			PhysicalInfo next = new PhysicalInfo(data[0]+"_"+data[1]);
//		    			if(tuples.containsKey(next))
//		    			{
//		    				Vector<LocalDataIndex> nextColumn = tuples.get(next);
//		    				LocalDataIndex cindex = new LocalDataIndex(key, data[2]);
//	    					nextColumn.add(cindex);		    		
//		    			}
//		    			else
//		    			{
//		    				Vector<LocalDataIndex> nextColumn = new Vector<LocalDataIndex>();
//		    				LocalDataIndex cindex = new LocalDataIndex(key, data[2]);
//	    					nextColumn.add(cindex);
//	    					tuples.put(next, nextColumn);
//		    			}
//		    		}
//		    	}
//		    	rs.close();
//		    	//stmt.close();
//		    	//send out
//		    	Enumeration enu = tuples.keys();
//		    	while(enu.hasMoreElements())
//		    	{
//		    		PhysicalInfo next = (PhysicalInfo)enu.nextElement();
//		    		ERPInsertDataIndexBody mybody = new ERPInsertDataIndexBody(serverpeer.getPhysicalInfo(),
//                            							next, tuples.get(next), ERPInsertDataIndexBody.STRING_TYPE,
//                            							treeNode.getLogicalInfo());
//		    		Head head = new Head();
//		    		head.setMsgType(MsgType.ERP_INSERT_DATA_INDEX.getValue());
//		    		serverpeer.sendMessage(serverpeer.getPhysicalInfo(), new Message(head, mybody));
//		    	}
//		    	tuples.clear();
//		    	
//		    	for(int i=0; i<delete.size(); i++)
//		    	{
//		    		stmt.addBatch(delete.get(i));
//		    	}
//		    	stmt.executeBatch();
//		    	stmt.close();
//		    }
//		    catch(Exception e1)
//		    {
//		    	e1.printStackTrace();
//		    }
		    
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new EventHandleException("Super peer joins network but cannot be accepted", e);
		}
	}

	public boolean isConsumed(Message msg) throws EventHandleException 
	{
		if (msg.getHead().getMsgType() == MsgType.SP_JOIN_ACCEPT.getValue()) {
			// TODO: JUST FOR TEST
			System.out.println("***SP_JOIN_ACCEPT Consumed");
			return true;
		}
		return false;
	}
}

