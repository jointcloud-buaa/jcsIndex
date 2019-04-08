/*
 * @(#) UDPPacket.java 1.0 2006-10-11
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.peer.request;

import edu.buaa.act.jcsindex.global.peer.PeerType;
import edu.buaa.act.jcsindex.global.peer.info.PeerInfo;
import edu.buaa.act.jcsindex.global.protocol.MsgType;
import edu.buaa.act.jcsindex.global.utils.Tools;

import java.net.DatagramPacket;
import java.net.InetAddress;

/**
 * This class is used to generate various UDP packets
 * used by <code>UDPSender</code> and <code>UDPReceiver</code>.
 * 
 * @author Xu Linhao
 * @version 1.0 2006-10-11
 */

public final class UDPPacket
{
	
	/**
	 * Returns a ping packet that only includes a 4-bytes message 
	 * type without specifying the IP address and port of the 
	 * destination peer.
	 * 
	 * <p>The packet format is:
	 * <pre>
	 * 0-3: the int value of PING  
	 * </pre>
	 * 
	 * @return a <code>DatagramPacket</code> with ping message type
	 */
	public synchronized static DatagramPacket ping()
	{
		byte[] buf = new byte[4];
		int val  = MsgType.PING.getValue();
		Tools.intToByteArray(val, buf, 0);
		return new DatagramPacket(buf, 0, buf.length);
	}
	
	/**
	 * Returns a ping packet that only includes a 4-bytes message 
	 * type with specified IP address and port of the destination peer. 
	 * 
	 * <p>The packet format is:
	 * <pre>
	 * 0-3: the int value of PING  
	 * </pre>
	 *   
	 * @param ip the IP address of the destination peer
	 * @param port the port that the datagram socket is running
	 * @return a <code>DatagramPacket</code> with ping message type
	 */
	public synchronized static DatagramPacket ping(InetAddress ip, int port)
	{
		DatagramPacket packet = ping();
		packet.setAddress(ip);
		packet.setPort(port);
		return packet;
	}
	
	/**
	 * Returns a pong packet that includes a 4-bytes message type,
	 * the peer type and the peer identifier, without specifying the
	 * IP address and port of the destination peer.
	 * 
	 * <p>The packet format is:
	 * <pre>
	 * 0-3: the int value of PING;
	 * 4-7: the number of bytes used for peer type, denoted as "tsize"
	 * 8-(tsize+7): the bytes array representing peer type 
	 * (tsize+8)-(tsize+11): the number of bytes used for peer identifier, denoted as "psize"
	 * (tsize+12)-(tsize+psize+11): the bytes array representing peer identifier   
	 * </pre>
	 * 
	 * @param type the peer type
	 * @param pid the peer identifier
	 * @return a <code>DatagramPacket</code> with pong message type and data object
	 */
	public synchronized static DatagramPacket pong(PeerType type, String pid)
	{
		byte[] buf = new byte[type.getValue().length() + pid.length() + 12];
		
		// get message type
		int start = 0;
		int val  = MsgType.PONG.getValue();
		Tools.intToByteArray(val, buf, start);

		// get the size of peer type
		start = 4;
		byte[] buf1 = type.getValue().getBytes();
		int size = buf1.length;
		Tools.intToByteArray(size, buf, start);
		
		// put peer type into byte array
		start = 8;
		for (int i = 0; i < size; i++)
		{
			buf[i + start] = buf1[i];
		}
		
		// get the size of the peer identifier
		start += size;
		byte[] buf2 = pid.getBytes();
		size = buf2.length;
		Tools.intToByteArray(size, buf, start);
		
		// put peer identifier into byte array
		start += 4;
		for (int i = 0; i < size; i++)
		{
			buf[i + start] = buf2[i];
		}
		
		// return packet
		start += size;
		return new DatagramPacket(buf, 0, start);
	}
	
	/**
	 * Returns a pong packet that includes a 4-bytes message type,
	 * the peer type and the peer identifier, with specified IP
	 * address and port of the destination peer.
	 * 
	 * <p>The packet format is:
	 * <pre>
	 * 0-3: the int value of PING;
	 * 4-7: the number of bytes used for peer type, denoted as "tsize"
	 * 8-(tsize+7): the bytes array representing peer type 
	 * (tsize+8)-(tsize+11): the number of bytes used for peer identifier, denoted as "psize"
	 * (tsize+12)-(tsize+psize+11): the bytes array representing peer identifier   
	 * </pre>
	 * 
	 * @param type the peer type
	 * @param pid the peer identifier
	 * @param ip the IP address of the destination peer
	 * @param port the port that the datagram socket is running
	 * @return a <code>DatagramPacket</code> with pong message type and data object
	 */
	public synchronized static DatagramPacket pong(PeerType type, String pid, InetAddress ip, int port)
	{
		DatagramPacket packet = pong(type, pid);
		packet.setAddress(ip);
		packet.setPort(port);
		return packet;
	}
	
	/**
	 * Returns a troubleshoot packet to indicate the peer who sends the packet
	 * will leave the system due to hardware or system errors.
	 * 
	 * <p>The packet format is:
	 * <pre>
	 * 0-3: the int value of TROUBLESHOOT
	 * 4-7: the number of bytes used for peer type, denoted as "tsize"
	 * 8-(tsize+7): the bytes array representing peer type 
	 * (tsize+8)-(tsize+11): the number of bytes used for peer identifier, denoted as "psize"
	 * (tsize+12)-(tsize+psize+11): the bytes array representing peer identifier   
	 * </pre>
	 * 
	 * @param type the peer type
	 * @param pid the peer identifier
	 * @return a <code>DatagramPacket</code> with troubleshoot message type
	 */
	public synchronized static DatagramPacket troubleshoot(PeerType type, String pid)
	{
		byte[] buf = new byte[type.getValue().length() + pid.length() + 12];
		
		// get message type
		int start = 0;
		int val  = MsgType.TROUBLESHOOT.getValue();
		Tools.intToByteArray(val, buf, start);

		// get the size of peer type
		start = 4;
		byte[] buf1 = type.getValue().getBytes();
		int size = buf1.length;
		Tools.intToByteArray(size, buf, start);
		
		// put peer type into byte array
		start = 8;
		for (int i = 0; i < size; i++)
		{
			buf[i + start] = buf1[i];
		}
		
		// get the size of the peer identifier
		start += size;
		byte[] buf2 = pid.getBytes();
		size = buf2.length;
		Tools.intToByteArray(size, buf, start);
		
		// put peer identifier into byte array
		start += 4;
		for (int i = 0; i < size; i++)
		{
			buf[i + start] = buf2[i];
		}
		
		// return packet
		start += size;
		return new DatagramPacket(buf, 0, start);
	}
	
	/**
	 * Returns a troubleshoot packet to indicate the peer who sends the packet
	 * will leave the system due to hardware or system errors.
	 * 
	 * <p>The packet format is:
	 * <pre>
	 * 0-3: the int value of TROUBLESHOOT
	 * 4-7: the number of bytes used for peer type, denoted as "tsize"
	 * 8-(tsize+7): the bytes array representing peer type 
	 * (tsize+8)-(tsize+11): the number of bytes used for peer identifier, denoted as "psize"
	 * (tsize+12)-(tsize+psize+11): the bytes array representing peer identifier   
	 * </pre>
	 * 
	 * @param type the peer type
	 * @param pid the peer identifier
	 * @param ip the IP address of the destination peer
	 * @param port the port that the datagram socket is running
	 * @return a <code>DatagramPacket</code> with troubleshoot message type
	 */
	public synchronized static DatagramPacket troubleshoot(PeerType type, String pid, InetAddress ip, int port)
	{
		DatagramPacket packet = troubleshoot(type, pid);
		packet.setAddress(ip);
		packet.setPort(port);
		return packet;
	}
	
	/**
	 * Parse the content of the packet whose type is PONG
	 * and wrap all parsed data into a <code>PeerInfo</code> instance.
	 * 
	 * @param buf the byte array of the packet content
	 * @param start the start position of the byte array to be parsed
	 * @return a <code>PeerInfo</code> instance
	 * 
	 * @see #pong(PeerType, String)
	 */
	public synchronized static PeerInfo parsePong(byte[] buf, int start)
	{
		String pid = null;
		String type= null;
		
		// get the size of peer type and read data
		int len = Tools.byteArrayToInt(buf, start);
		start += 4;
		type = new String(buf, start, len);
		
		// get the size of the peer identifier and read data
		start += len;
		len = Tools.byteArrayToInt(buf, start);
		start += 4;
		pid = new String(buf, start, len);
		
		return new PeerInfo(pid, null, 0, type);
	}
	
	/**
	 * Parse the content of the packet whose type is TROUBLESHOOT
	 * and wrap all parsed data into a <code>PeerInfo</code> instance.
	 * 
	 * @param buf the byte array of the packet content
	 * @param start the start position of the byte array to be parsed
	 * @return a <code>PeerInfo</code> instance
	 * 
	 * @see #troubleshoot(PeerType, String)
	 */
	public synchronized static PeerInfo parseTroubleshoot(byte[] buf, int start)
	{
		return parsePong(buf, start);
	}
	
	/**
	 * Returns the message type of the packet.
	 * 
	 * @param buf the byte array to be parsed
	 * @param start the start position of the byte array to be parsed
	 * @return the message type of the packet
	 */
	public synchronized static int parseType(byte[] buf, int start)
	{
		return Tools.byteArrayToInt(buf, start);
	}
	
}