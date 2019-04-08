/*
 * @(#) ServerRequestManager.java 1.0 2006-2-17
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.peer.request;

import edu.buaa.act.jcsindex.global.ServerInstance;
import edu.buaa.act.jcsindex.global.logging.LogManager;
import edu.buaa.act.jcsindex.global.peer.ServerPeer;
import edu.buaa.act.jcsindex.global.peer.event.SPGeneralAction;
import edu.buaa.act.jcsindex.global.peer.info.*;
import edu.buaa.act.jcsindex.global.peer.management.PeerMaintainer;
import edu.buaa.act.jcsindex.global.protocol.Head;
import edu.buaa.act.jcsindex.global.protocol.Message;
import edu.buaa.act.jcsindex.global.protocol.MsgType;
import edu.buaa.act.jcsindex.global.protocol.body.*;

import java.io.ObjectOutputStream;
import java.net.Socket;


/**
 * Implement all request operation of the super peer.
 * 
 * @author Xu Linhao
 * @version 1.0 2007-2-17
 */

public class ServerRequestManager
{
	
	// private members
	private ServerInstance serverinstance;
	private ServerPeer serverpeer;
	private Socket socket;

	
	public ServerRequestManager()
	{
		
	}
	
	/**
	 * Construct a request manager for server peer.
	 * 
	 * @param serverpeer the handler of the <code>ServerPeer</code>
	 */
	public ServerRequestManager(ServerPeer serverpeer)
	{
		this.serverpeer = serverpeer;
		this.serverinstance  = (ServerInstance) serverpeer.getInstance();
	}

	/**
	 * Perform a LOGIN request to the bootstrap server,
	 * who is responsible for validating the correctness of
	 * the super peer's identifier and then returning all online
	 * super peers as its bootstrap in order to allow it to join
	 * the super peer network.
	 * 
	 * @param window the handler of the login window 
	 * @param user the user identifier
	 * @param pwd the password
	 * @param ip the IP address of the bootstrap server
	 * @param port the port of the bootstrap server
	 */
//	public void performLoginRequest(LoginPanel window, String user, String pwd, String ip, int port)
//	{
//		try
//		{
//			/* init socket connection with bootstrap server */
//			socket = new Socket(ip, port);
//
//			/* create message to be sent out */
//			Head head = new Head();
//			head.setMsgType(MsgType.LOGIN.getValue());
//
//			PhysicalInfo info = serverpeer.getPhysicalInfo();
//			Body body = new UserBody(user, pwd, null, info.getIP(), info.getPort(), serverpeer.getPeerType());
//
//			Message message = new Message(head, body);
//
//			/* send login message to bootstrap server */
//			ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
//			oos.writeObject(message);
//
//			/* record request */
//			serverinstance.log(LogEventType.INFORMATION.getValue(), MsgType.description(head.getMsgType()) + " request is sent out...", socket.getLocalAddress().getHostName(), socket.getLocalAddress().getHostAddress() + ":" + socket.getLocalPort(), System.getProperty("user.name"));
//			LogManager.getLogger(serverpeer).info(MsgType.description(head.getMsgType()) + " request is sent out from " + socket.getLocalAddress().getHostAddress() + ":" + socket.getLocalPort());
//
//
//			/* get reply from bootstrap server */
//			ObjectInputStream ois  = new ObjectInputStream(socket.getInputStream());
//
//			message = (Message) ois.readObject();
//
//			head = message.getHead();
//			int msgType = head.getMsgType();
//
//			/* record reply of request */
//			serverinstance.log(LogEventType.INFORMATION.getValue(), "Reply of " + MsgType.description(MsgType.LOGIN.getValue()) + ": " + MsgType.description(head.getMsgType()),	socket.getInetAddress().getHostName(), socket.getInetAddress().getHostAddress() + ":" + socket.getPort(), System.getProperty("user.name"));
//			LogManager.getLogger(serverpeer).info("Reply of " + MsgType.description(MsgType.LOGIN.getValue()) + ": " + MsgType.description(head.getMsgType()) + " from " + socket.getInetAddress().getHostAddress() + ":" + socket.getPort());
//
//			boolean loginSuccess = true;
//
//			/*
//			 * if LACK, then a list of online super peers returned
//			 */
//			if (msgType == MsgType.LACK.getValue())
//			{
//				FeedbackBody feedbackBody = (FeedbackBody)message.getBody();
//				PeerInfo[] onlineSuperPeers = feedbackBody.getOnlineSuperPeers();
//
//				serverpeer.initSession(user, pwd);
////				window.dispose();
//
//				/*
//				 * display entrance points.
//				 * Now I just pass a null value into the constructor,
//				 * in the future, here should pass a list of online
//				 * super peers from message.
//				 */
//				new EntrancePointDialog(serverinstance, onlineSuperPeers);
//			}
//
//			/*
//			 * if only the current peer is the unique super peer in the network,
//			 * and now is online
//			 */
//			else if (msgType == MsgType.UNIQUE_SUPER_PEER.getValue())
//			{
//				serverpeer.initSession(user, pwd);
////				window.dispose();
//
//				/* it is the root of the tree structure */
//				ContentInfo content = new ContentInfo(
//						new BoundaryValue(IndexValue.MIN_KEY.getString(), Long.MIN_VALUE),
//						new BoundaryValue(IndexValue.MAX_KEY.getString(), Long.MAX_VALUE),
//						10, new Vector<IndexValue>());
//				TreeNode treeNode = new TreeNode(new LogicalInfo(0, 1), null, null, null, null, null, new RoutingTableInfo(0), new RoutingTableInfo(0), content, 0, 1);
//
//			    serverpeer.addListItem(treeNode);
//			    serverinstance.updatePane(treeNode);
//
//				serverinstance.startService();
//				serverinstance.setMenuEnable(true);
//			}
//
//			/*
//			 * if no super peer is online
//			 */
//			else if (msgType == MsgType.NO_ONLINE_SUPER_PEER.getValue())
//			{
//				JOptionPane.showMessageDialog(serverinstance, "No super peer online");
//			}
//
//			/*
//			 * login failed
//			 */
//			else if (msgType == MsgType.LNCK.getValue())
//			{
//				JOptionPane.showMessageDialog(serverinstance, "Login failed");
//
//				loginSuccess = false;
//			}
//
//			/*
//			 * if user does not exist
//			 */
//			else if (msgType == MsgType.USER_NOT_EXIST.getValue())
//			{
//				JOptionPane.showMessageDialog(serverinstance, "User does not exist");
//
//				loginSuccess = false;
//			}
//
//			/*
//			 * if unknown message type
//			 */
//			else
//			{
//				JOptionPane.showMessageDialog(serverinstance, "Unknown response message type: " + msgType);
//				LogManager.getLogger(serverpeer).warn("Unknown response message type: " + msgType);
//
//				loginSuccess  = false;
//			}
//
//			//VHTam
//			//receice global schema from boostrap peer
//			//and receive role setting from bootstrap peer
//			if (loginSuccess){
////				String schema = (String)ois.readObject();
////				//JOptionPane.showMessageDialog(null, schema);
////
////				//update metadata about schema
////				MetaDataAccess.updateSchema(serverpeer.conn_metabestpeerdb, schema);
////
////				//create global schema database
////				performSchemaUpdate(schema);
//
//				// receive role setting
//				// send role setting
//				//RoleManagement roleManagement = (RoleManagement)ois.readObject();
//
//				//roleManagement.storeToDB(serverpeer.conn_metabestpeerdb);
//			}
//			//end VHTam
//
//			/*
//			 * never close reader before writer
//			 */
//			oos.close();
//			ois.close();
//			socket.close();
//		}
//		catch (Exception e)
//		{
//			// record exception event
//			serverinstance.log(LogEventType.WARNING.getValue(), "Fail to open connection with Bootstrap Server because:\r\n" + Tools.getException(e), Inet.getInetAddress2().getHostName(),	Inet.getInetAddress(), System.getProperty("user.name"));
//			LogManager.getLogger(serverpeer).error("Fail to open connection to bootstrap server", e);
//			JOptionPane.showMessageDialog(serverinstance, "Cannot connect to Bootstrap");
//		}
//	}

	/**
	 * Perform a REGISTER request to the bootstrap server, 
	 * who is responsible for registering the peer information 
	 * to the back-end database and returning all online super peer
	 * information to the newcomer for letting it to select one as
	 * the bootstrap to join the super peer network. 
	 * 
	 * @param window the handler of the login window 
	 * @param user the user identifier
	 * @param pwd the password
	 * @param ip the IP address of the bootstrap server
	 * @param port the port of the bootstrap server
	 * @param email the email address of the register user
	 */
//	public void performRegisterRequest(Window window, String user, String pwd, String ip, int port, String email)
//	{
//		try
//		{
//			/* init socket connection with bootstrap server */
//			socket = new Socket(ip, port);
//
//			/* create message to be sent out */
//			Head head = new Head();
//			head.setMsgType(MsgType.REGISTER.getValue());
//
//			PhysicalInfo info = serverpeer.getPhysicalInfo();
//			Body body = new UserBody(user, pwd, email, info.getIP(), info.getPort(), serverpeer.getPeerType());
//
//			Message message = new Message(head, body);
//
//			/* send login message to bootstrap server */
//			ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
//			oos.writeObject(message);
//
//			/* record request */
//			serverinstance.log(LogEventType.INFORMATION.getValue(), MsgType.description(head.getMsgType()) + " request is sent out...", socket.getLocalAddress().getHostName(),	socket.getLocalAddress().getHostAddress() + ":" + socket.getLocalPort(), System.getProperty("user.name"));
//			LogManager.getLogger(serverpeer).info(MsgType.description(head.getMsgType()) + " request is sent out from " + socket.getLocalAddress().getHostAddress() + ":" + socket.getLocalPort());
//
//			/* get reply from bootstrap server */
//			ObjectInputStream ois  = new ObjectInputStream(socket.getInputStream());
//			message = (Message) ois.readObject();
//
//			head = message.getHead();
//			int msgType = head.getMsgType();
//
//			/* record reply of request */
//			serverinstance.log(LogEventType.INFORMATION.getValue(), "Reply of " + MsgType.description(MsgType.REGISTER.getValue()) + ": " + MsgType.description(head.getMsgType()), socket.getInetAddress().getHostName(), socket.getInetAddress().getHostAddress() + ":" + socket.getPort(), System.getProperty("user.name"));
//			LogManager.getLogger(serverpeer).info("Reply of " + MsgType.description(MsgType.REGISTER.getValue()) + ": " + MsgType.description(head.getMsgType()) + " from " + socket.getInetAddress().getHostAddress() + ":" + socket.getPort());
//
//			/*
//			 * if LACK, then a list of online super peers returned
//			 */
//			if (msgType == MsgType.RACK.getValue())
//			{
//				FeedbackBody feedbackBody = (FeedbackBody)message.getBody();
//				PeerInfo[] onlineSuperPeers = feedbackBody.getOnlineSuperPeers();
//
//				serverpeer.initSession(user, pwd);
//				window.dispose();
//
//				/*
//				 * display entrance points.
//				 * Now I just pass a null value into the constructor,
//				 * in the future, here should pass a list of online
//				 * super peers from message.
//				 */
//				new EntrancePointDialog(serverinstance, onlineSuperPeers);
//			}
//
//			/*
//			 * if only the current peer is the unique super peer in the network,
//			 * and now is online
//			 */
//			else if (msgType == MsgType.UNIQUE_SUPER_PEER.getValue())
//			{
//				serverpeer.initSession(user, pwd);
//				window.dispose();
//
//				/* it is the root of the tree structure */
//				ContentInfo content = new ContentInfo(
//						new BoundaryValue(IndexValue.MIN_KEY.getString(), Long.MIN_VALUE),
//						new BoundaryValue(IndexValue.MAX_KEY.getString(), Long.MAX_VALUE),
//						10, new Vector<IndexValue>());
//				TreeNode treeNode = new TreeNode(new LogicalInfo(0, 1), null, null, null, null, null, new RoutingTableInfo(0), new RoutingTableInfo(0), content, 0, 1);
//
//			    serverpeer.addListItem(treeNode);
//			    serverinstance.updatePane(treeNode);
//
//			    serverinstance.startService();
//				serverinstance.setMenuEnable(true);
//			}
//
//			/*
//			 * login failed
//			 */
//			else if (msgType == MsgType.RNCK.getValue())
//			{
//				JOptionPane.showMessageDialog(serverinstance, "Register failed");
//			}
//
//			/*
//			 * if user does not exist
//			 */
//			else if (msgType == MsgType.USER_EXISTED.getValue())
//			{
//				JOptionPane.showMessageDialog(serverinstance, "User has existed");
//			}
//
//			/*
//			 * if unknown message type
//			 */
//			else
//			{
//				JOptionPane.showMessageDialog(serverinstance, "Unknown response message type: " + msgType);
//				LogManager.getLogger(serverpeer).warn("Unknown response message type: " + msgType);
//			}
//
//			/*
//			 * never close reader before writer
//			 */
//			oos.close();
//			ois.close();
//			socket.close();
//		}
//		catch (Exception e)
//		{
//			// record exception event
//			serverinstance.log(LogEventType.WARNING.getValue(), "Fail to open connection with Bootstrap Server because:\r\n" + Tools.getException(e), Inet.getInetAddress2().getHostName(), Inet.getInetAddress(), System.getProperty("user.name"));
//			LogManager.getLogger(serverpeer).error("Fail to open connection to bootstrap server", e);
//			JOptionPane.showMessageDialog(serverinstance, "Cannot connect to Bootstrap");
//		}
//	}

	/**
	 * Perform a LOGOUT (I_WILL_LEAVE) request to the bootstrap server,
	 * who is responsible for removing the registered information from
	 * the <code>OnlinePeerManager</code> and updating the UI components.
	 * <p>
	 * If the super peer signs out successfully, then another online super peer
	 * may be selected to replace the empty position of the super peer in order
	 * to maintain the correctness of the network structure.
	 * 
	 * @param ip the IP address of the bootstrap server
	 * @param port the port of the bootstrap server
	 * @return if logout successfully, return <code>true</code>;
	 * 			otherwise, return <code>false</code>
	 */
//	public boolean performLogoutRequest(String ip, int port)
//	{
//		try
//		{
//			socket = new Socket(ip, port);
//
//			Head head = new Head();
//			head.setMsgType(MsgType.I_WILL_LEAVE.getValue());
//
//			Session session = Session.getInstance();
//			PhysicalInfo info = serverpeer.getPhysicalInfo();
//			Body body = new UserBody(session.getUserID(), "", "", info.getIP(), info.getPort(), serverpeer.getPeerType());
//
//			Message message = new Message(head, body);
//
//			/* send register message to bootstrap server */
//			ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
//			oos.writeObject(message);
//
//			/* record request */
//			serverinstance.log(LogEventType.INFORMATION.getValue(), MsgType.description(head.getMsgType()) + " request is sent out...", socket.getLocalAddress().getHostName(), socket.getLocalAddress().getHostAddress() + ":" + socket.getLocalPort(), System.getProperty("user.name"));
//			LogManager.getLogger(serverpeer).info(MsgType.description(head.getMsgType()) + " request is sent out from " + socket.getLocalAddress().getHostAddress() + ":" + socket.getLocalPort());
//
//			/*
//			 * never close reader before writer
//			 */
//			oos.close();
//			socket.close();
//
//			/* send a leave request to parent node */
//			this.performLeaveRequest();
//
//			return true;
//		}
//		catch (Exception e)
//		{
//			// record exception event
//			serverinstance.log(LogEventType.WARNING.getValue(), "Fail to open connection with Bootstrap Server because:\r\n" + Tools.getException(e), Inet.getInetAddress2().getHostName(), Inet.getInetAddress(), System.getProperty("user.name"));
//			LogManager.getLogger(serverpeer).error("Fail to open connection to bootstrap server", e);
//			JOptionPane.showMessageDialog(serverinstance, "Cannot connect to Bootstrap");
//			return false;
//		}
//	}
	
	/**
	 * Perform a SP_LOGIN request to an online super peer, who will be 
	 * responsible for routing the SP_JOIN request to the proper super peer,
	 * and initializing the routing table and other necessary information 
	 * of the newcomer.
	 * 
	 * @param ip the IP address of an online super peer
	 * @param port the port of the online super per
	 * @return if join the super peer network successfully, return <code>true</code>;
	 * 			otherwise, return <code>false</code>
	 */
	public boolean performJoinRequest(String ip, int port)
	{
		try
		{
			socket = new Socket(ip, port);

			Head head = new Head();
			head.setMsgType(MsgType.SP_JOIN.getValue());

			PhysicalInfo info = serverpeer.getPhysicalInfo();
			Body body = new SPJoinBody(info, null, info, null);
			Message message = new Message(head, body);

			ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
			oos.writeObject(message);

			/* record request */
//			serverinstance.log(LogEventType.INFORMATION.getValue(), MsgType.description(head.getMsgType()) + " request is sent out...", socket.getLocalAddress().getHostName(),	socket.getLocalAddress().getHostAddress() + ":" + socket.getLocalPort(), System.getProperty("user.name"));
//			LogManager.getLogger(serverpeer).info(MsgType.description(head.getMsgType()) + " request is sent out from " + socket.getLocalAddress().getHostAddress() + ":" + socket.getLocalPort());

			oos.close();
			socket.close();
			return true;
		}
		catch (Exception e)
		{
			// record exception event
			System.out.println("performJoinRequest Failed!");
//			serverinstance.log(LogEventType.WARNING.getValue(), "Fail to open connection with the selected BATON node(" + ip + ":" + port + ") because:\r\n" + Tools.getException(e), Inet.getInetAddress2().getHostName(), Inet.getInetAddress(), System.getProperty("user.name"));
//			LogManager.getLogger(serverpeer).error("Fail to open connection to bootstrap server", e);
//			JOptionPane.showMessageDialog(serverinstance, "Cannot connect to selected super peer");
			return false;
		}
	}

	/**
	 * Perform a JOIN_SUCCESS request to the bootstrap server, who
	 * will add the registered information to <code>OnlinePeerManager</code>/
	 * 
	 * @param ip the IP address of the bootstrap server
	 * @param port the port of the bootstrap server
	 * @return if join operation is success, return <code>true</code>;
	 *          otherwise, return <code>false</code>
	 */
//	public boolean performSuccessJoinRequest(String ip, int port)
//	{
//		try
//		{
//			socket = new Socket(ip, port);
//
//			Head head = new Head();
//			head.setMsgType(MsgType.JOIN_SUCCESS.getValue());
//
//			Session session = Session.getInstance();
//			PhysicalInfo info = serverpeer.getPhysicalInfo();
//			Body body = new UserBody(session.getUserID(), "", "", info.getIP(), info.getPort(), serverpeer.getPeerType());
//
//			Message message = new Message(head, body);
//			ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
//			oos.writeObject(message);
//
//			/* record request */
//			serverinstance.log(LogEventType.INFORMATION.getValue(), MsgType.description(head.getMsgType()) + " request is sent out...",	socket.getLocalAddress().getHostName(),	socket.getLocalAddress().getHostAddress() + ":" + socket.getLocalPort(), System.getProperty("user.name"));
//			LogManager.getLogger(serverpeer).info(MsgType.description(head.getMsgType()) + " request is sent out from " + socket.getLocalAddress().getHostAddress() + ":" + socket.getLocalPort());
//
//			oos.close();
//			return true;
//		}
//		catch (Exception e)
//		{
//			// record exception event
//			serverinstance.log(LogEventType.WARNING.getValue(), "Fail to open connection with Bootstrap Server because:\r\n" + Tools.getException(e), Inet.getInetAddress2().getHostName(), Inet.getInetAddress(), System.getProperty("user.name"));
//			LogManager.getLogger(serverpeer).error("Fail to open connection to bootstrap server", e);
//			JOptionPane.showMessageDialog(serverinstance, "Cannot connect to Bootstrap");
//			return false;
//		}
//	}
	
	/**
	 * Perform a JOIN_FAILURE request to the bootstrap server, who will
	 * remove the registered information from <code>OnlinePeerManager</code>.
	 * 
	 * @param ip the IP address of the bootstrap server
	 * @param port the port of the bootstrap server
	 * @return if join operation is canceled, return <code>true</code>;
	 * 			otherwise, return <code>false</code>
	 */
//	public boolean performCancelJoinRequest(String ip, int port)
//	{
//		try
//		{
//			socket = new Socket(ip, port);
//
//			Head head = new Head();
//			head.setMsgType(MsgType.JOIN_FAILURE.getValue());
//
//			Session session = Session.getInstance();
//			PhysicalInfo info = serverpeer.getPhysicalInfo();
//			Body body = new UserBody(session.getUserID(), "", "", info.getIP(), info.getPort(), serverpeer.getPeerType());
//
//			Message message = new Message(head, body);
//			ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
//			oos.writeObject(message);
//
//			/* record request */
//			serverinstance.log(LogEventType.INFORMATION.getValue(), MsgType.description(head.getMsgType()) + " request is sent out...",	socket.getLocalAddress().getHostName(),	socket.getLocalAddress().getHostAddress() + ":" + socket.getLocalPort(), System.getProperty("user.name"));
//			LogManager.getLogger(serverpeer).info(MsgType.description(head.getMsgType()) + " request is sent out from " + socket.getLocalAddress().getHostAddress() + ":" + socket.getLocalPort());
//
//			oos.close();
//			return true;
//		}
//		catch (Exception e)
//		{
//			// record exception event
//			serverinstance.log(LogEventType.WARNING.getValue(), "Fail to open connection with Bootstrap Server because:\r\n" + Tools.getException(e), Inet.getInetAddress2().getHostName(), Inet.getInetAddress(), System.getProperty("user.name"));
//			LogManager.getLogger(serverpeer).error("Fail to open connection to bootstrap server", e);
//			JOptionPane.showMessageDialog(serverinstance, "Connect to Bootstrap Failed");
//			return false;
//		}
//	}
	
	/**
	 * Perform a LEAVE request when a super peer signs off
	 *
	 */
	public void performLeaveRequest()
	{
		try
		{
			Head head = new Head();
			head.setMsgType(MsgType.SP_LEAVE_URGENT.getValue());
			Body body;
			Message message;
			PhysicalInfo info = serverpeer.getPhysicalInfo();
			PhysicalInfo physicalReplacer = null;
			LogicalInfo logicalReplacer = null;
			PeerInfo[] attachedPeers;
			int i, j, listSize = serverpeer.getListSize();

			for (i = listSize - 1; i >= 0; i--)
			{
				TreeNode treeNode = serverpeer.removeListItem(i);
				if (treeNode.getRole() == 1)
				{
					treeNode.getContent().setData(SPGeneralAction.loadData());

					if ((treeNode.getParentNode() == null) &&
						(treeNode.getLeftChild() == null) &&
						(treeNode.getRightChild() == null))
						return;

					if (treeNode.getParentNode() != null)
					{
						//a leave urgent should be sent to the parent node
						physicalReplacer = treeNode.getParentNode().getPhysicalInfo();
						logicalReplacer = treeNode.getParentNode().getLogicalInfo();
					}
					else
					{
						//a leave urgent should be sent to a child
						if (treeNode.getLeftChild() != null)
						{
							physicalReplacer = treeNode.getLeftChild().getPhysicalInfo();
							logicalReplacer = treeNode.getLeftChild().getLogicalInfo();
						}
						else
						{
							physicalReplacer = treeNode.getRightChild().getPhysicalInfo();
							logicalReplacer = treeNode.getRightChild().getLogicalInfo();
						}
					}

					//1. send a leave urgent request
					body = new SPLeaveUrgentBody(info, treeNode.getLogicalInfo(), treeNode, logicalReplacer);
					message = new Message(head, body);
					serverpeer.sendMessage(physicalReplacer, message);

					/* record request */
//					serverinstance.log(LogEventType.INFORMATION.getValue(), MsgType.description(head.getMsgType()) + " request is sent out...",	socket.getLocalAddress().getHostName(),	socket.getLocalAddress().getHostAddress() + ":" + socket.getLocalPort(), System.getProperty("user.name"));
					LogManager.getLogger(serverpeer).info(MsgType.description(head.getMsgType()) + " request is sent out from " + socket.getLocalAddress().getHostAddress() + ":" + socket.getLocalPort());

					attachedPeers = PeerMaintainer.getInstance().getClients();
					if (attachedPeers.length > 0)
					{
						//2. pass current attached client peers
						head.setMsgType(MsgType.SP_PASS_CLIENT.getValue());
						body = new SPPassClientBody(info, treeNode.getLogicalInfo(),
								attachedPeers, logicalReplacer);
						message = new Message(head, body);
						serverpeer.sendMessage(physicalReplacer, message);

						/* record request */
//						serverinstance.log(LogEventType.INFORMATION.getValue(), MsgType.description(head.getMsgType()) + " request is sent out...",	socket.getLocalAddress().getHostName(), socket.getLocalAddress().getHostAddress() + ":" + socket.getLocalPort(), System.getProperty("user.name"));
						LogManager.getLogger(serverpeer).info(MsgType.description(head.getMsgType()) + " request is sent out from " + socket.getLocalAddress().getHostAddress() + ":" + socket.getLocalPort());

						//3. send a leave notify to attached client peers
						Head headNotifyClient = new Head();
						headNotifyClient.setMsgType(MsgType.SP_LEAVE_NOTIFY_CLIENT.getValue());
						int size = attachedPeers.length;
						for (j = 0; j < size; j++)
						{
							PeerInfo client = attachedPeers[j];
							PhysicalInfo clientpeer = new PhysicalInfo(client.getInetAddress(), client.getPort());
							body = new SPLeaveNotifyClientBody(info, physicalReplacer);
							message = new Message(headNotifyClient, body);
							serverpeer.sendMessage(clientpeer, message);

							/* record request */
//							serverinstance.log(LogEventType.INFORMATION.getValue(), MsgType.description(headNotifyClient.getMsgType()) + " request is sent out...",	socket.getLocalAddress().getHostName(),	socket.getLocalAddress().getHostAddress() + ":" + socket.getLocalPort(), System.getProperty("user.name"));
							LogManager.getLogger(serverpeer).info(MsgType.description(head.getMsgType()) + " request is sent out from " + socket.getLocalAddress().getHostAddress() + ":" + socket.getLocalPort());
						}
					}

					//4. send a leave notify to other linked nodes
					Head headNotify = new Head();
					headNotify.setMsgType(MsgType.SP_LEAVE_NOTIFY.getValue());
					if (treeNode.getLeftChild()!=null)
					{
						body = new SPLeaveNotifyBody(info, treeNode.getLogicalInfo(),
								physicalReplacer, 0, 0, treeNode.getLeftChild().getLogicalInfo());
						message = new Message(headNotify, body);
						serverpeer.sendMessage(treeNode.getLeftChild().getPhysicalInfo(), message);

						/* record request */
//						serverinstance.log(LogEventType.INFORMATION.getValue(), MsgType.description(headNotify.getMsgType()) + " request is sent out...", socket.getLocalAddress().getHostName(), socket.getLocalAddress().getHostAddress() + ":" + socket.getLocalPort(), System.getProperty("user.name"));
						LogManager.getLogger(serverpeer).info(MsgType.description(head.getMsgType()) + " request is sent out from " + socket.getLocalAddress().getHostAddress() + ":" + socket.getLocalPort());
					}
					if (treeNode.getRightChild()!=null)
					{
						body = new SPLeaveNotifyBody(info, treeNode.getLogicalInfo(),
								physicalReplacer, 0, 0, treeNode.getRightChild().getLogicalInfo());
						message = new Message(headNotify, body);
						serverpeer.sendMessage(treeNode.getRightChild().getPhysicalInfo(), message);

						/* record request */
//						serverinstance.log(LogEventType.INFORMATION.getValue(), MsgType.description(headNotify.getMsgType()) + " request is sent out...", socket.getLocalAddress().getHostName(), socket.getLocalAddress().getHostAddress() + ":" + socket.getLocalPort(), System.getProperty("user.name"));
						LogManager.getLogger(serverpeer).info(MsgType.description(head.getMsgType()) + " request is sent out from " + socket.getLocalAddress().getHostAddress() + ":" + socket.getLocalPort());
					}
					if (treeNode.getLeftAdjacentNode()!=null)
					{
						body = new SPLeaveNotifyBody(info, treeNode.getLogicalInfo(),
								physicalReplacer, 2, 0, treeNode.getLeftAdjacentNode().getLogicalInfo());
						message = new Message(headNotify, body);
						serverpeer.sendMessage(treeNode.getLeftAdjacentNode().getPhysicalInfo(), message);

						/* record request */
//						serverinstance.log(LogEventType.INFORMATION.getValue(), MsgType.description(headNotify.getMsgType()) + " request is sent out...", socket.getLocalAddress().getHostName(), socket.getLocalAddress().getHostAddress() + ":" + socket.getLocalPort(), System.getProperty("user.name"));
						LogManager.getLogger(serverpeer).info(MsgType.description(head.getMsgType()) + " request is sent out from " + socket.getLocalAddress().getHostAddress() + ":" + socket.getLocalPort());
					}
					if (treeNode.getRightAdjacentNode()!=null)
					{
						body = new SPLeaveNotifyBody(info, treeNode.getLogicalInfo(),
								physicalReplacer, 1, 0, treeNode.getRightAdjacentNode().getLogicalInfo());
						message = new Message(headNotify, body);
						serverpeer.sendMessage(treeNode.getRightAdjacentNode().getPhysicalInfo(), message);

						/* record request */
//						serverinstance.log(LogEventType.INFORMATION.getValue(), MsgType.description(headNotify.getMsgType()) + " request is sent out...", socket.getLocalAddress().getHostName(), socket.getLocalAddress().getHostAddress() + ":" + socket.getLocalPort(), System.getProperty("user.name"));
						LogManager.getLogger(serverpeer).info(MsgType.description(head.getMsgType()) + " request is sent out from " + socket.getLocalAddress().getHostAddress() + ":" + socket.getLocalPort());
					}
					for (j = 0; j < treeNode.getLeftRoutingTable().getTableSize(); j++)
					{
						RoutingItemInfo nodeInfo = treeNode.getLeftRoutingTable().getRoutingTableNode(j);
						if (nodeInfo!=null)
						{
							body = new SPLeaveNotifyBody(info, treeNode.getLogicalInfo(),
									physicalReplacer, 4, j, nodeInfo.getLogicalInfo());
							message = new Message(headNotify, body);
							serverpeer.sendMessage(nodeInfo.getPhysicalInfo(), message);

							/* record request */
//							serverinstance.log(LogEventType.INFORMATION.getValue(), MsgType.description(headNotify.getMsgType()) + " request is sent out...", socket.getLocalAddress().getHostName(), socket.getLocalAddress().getHostAddress() + ":" + socket.getLocalPort(), System.getProperty("user.name"));
							LogManager.getLogger(serverpeer).info(MsgType.description(head.getMsgType()) + " request is sent out from " + socket.getLocalAddress().getHostAddress() + ":" + socket.getLocalPort());
						}
					}
					for (j = 0; j < treeNode.getRightRoutingTable().getTableSize(); j++)
					{
						RoutingItemInfo nodeInfo = treeNode.getRightRoutingTable().getRoutingTableNode(j);
						if (nodeInfo!=null)
						{
							body = new SPLeaveNotifyBody(info, treeNode.getLogicalInfo(),
									physicalReplacer, 3, j, nodeInfo.getLogicalInfo());
							message = new Message(headNotify, body);
							serverpeer.sendMessage(nodeInfo.getPhysicalInfo(), message);

							/* record request */
//							serverinstance.log(LogEventType.INFORMATION.getValue(), MsgType.description(headNotify.getMsgType()) + " request is sent out...", socket.getLocalAddress().getHostName(), socket.getLocalAddress().getHostAddress() + ":" + socket.getLocalPort(), System.getProperty("user.name"));
							LogManager.getLogger(serverpeer).info(MsgType.description(head.getMsgType()) + " request is sent out from " + socket.getLocalAddress().getHostAddress() + ":" + socket.getLocalPort());
						}
					}
				}
			}
		}
		catch (Exception e)
		{
			// record exception event
//			serverinstance.log(LogEventType.WARNING.getValue(), "Fail to send SP_LEAVEopen requests to link nodes because:\r\n" + Tools.getException(e), Inet.getInetAddress2().getHostName(), Inet.getInetAddress(), System.getProperty("user.name"));
			LogManager.getLogger(serverpeer).error("Fail to open connection to bootstrap server", e);
			return;
		}
	}
	
//	public boolean performStabilizeRequest(String ip, int port, Message msg)
//	{
//		return false;
//	}

//	public boolean performRefreshRequest(String ip, int port, Message msg)
//	{
//		return false;
//	}
	
//	public void performCheckImbalance(int seconds)
//	{
//		// TODO: add log event into CheckImbalance
//		new CheckImbalance(serverpeer, seconds);
//	}
	
	/**
	 * Mihai
	 * 
	 * drops the existing schema and creates a new one as indicated by the bootstrap node
	 * @param newSchema
	 */
//	public void performSchemaUpdate(String newSchema){
//
//		Connection conn = ServerPeer.conn_globalSchema;
//		Statement stmt;
//
//		try{
//
//		stmt = conn.createStatement();
//
//		//drop the existing database
//		String sql= "DROP DATABASE IF EXISTS "+serverpeer.GLOBAL_DB;
//		int rs	= stmt.executeUpdate(sql);
//		//create a new one and give it this schema
//
//		 sql = "CREATE DATABASE "+serverpeer.GLOBAL_DB;
//		 rs = stmt.executeUpdate(sql);
//		 stmt.executeQuery("USE "+serverpeer.GLOBAL_DB);
//
//		 //System.out.println("Begin schema Update at Peers...");
//
//		 String[] tables=newSchema.split(";");
//
//		 //JOptionPane.showMessageDialog(serverinstance, newSchema+"\n"+tables[0]+"\n"+tables[1]);
//
//		 for (int i = 0 ;i < tables.length;i++){
//			 if (tables[i].length()>0){
//				 rs	= stmt.executeUpdate(tables[i]);
//				 //JOptionPane.showMessageDialog(serverinstance, tables[i]);
//				 //System.out.println(tables[i]);
//			 }
//		 }
//
//		}catch(Exception e){
//			serverinstance.log(LogEventType.WARNING.getValue(), "Failed to update schema because:\r\n" + Tools.getException(e), Inet.getInetAddress2().getHostName(), Inet.getInetAddress(), System.getProperty("user.name"));
//			LogManager.getLogger(serverpeer).error("Failed to update schema", e);
//			JOptionPane.showMessageDialog(serverinstance, "Failed to update schema");
//			System.out.println(newSchema);
//			e.printStackTrace();
//			return;
//		}
//		try{
//			createMatchesTable(conn);
//		}catch(Exception e){
//			serverinstance.log(LogEventType.WARNING.getValue(), "Failed to create schema mapping table", Inet.getInetAddress2().getHostName(), Inet.getInetAddress(), System.getProperty("user.name"));
//			LogManager.getLogger(serverpeer).error("Failed to create schema mapping table");
//			JOptionPane.showMessageDialog(serverinstance,"Failed to create schema mapping table");
//			e.printStackTrace();
//		}
//
//	}
	
	/**
	 * mihai, june 24th, 2008
	 * 
	 * creates a table to map the schema mappings.
	 */
//	public void createMatchesTable(Connection conn) throws Exception{
//		Statement stmt = conn.createStatement();
//		stmt.executeUpdate("CREATE DATABASE IF NOT EXISTS "+serverpeer.MATCHES_DB);
//		stmt.executeQuery("USE "+serverpeer.MATCHES_DB);
//		stmt.executeUpdate("CREATE TABLE IF NOT EXISTS Matches (" +
//				"sourceDB VARCHAR(20) NOT NULL DEFAULT 'BestPeerServerLS' COMMENT 'the name of the source database'," +
//				"sourceTable VARCHAR(30) NOT NULL COMMENT 'the name of the source table'," +
//				"sourceVersion INT COMMENT 'the version of the source database, in case versioning is used'," +
//				"sourceColumn VARCHAR(50) NOT NULL COMMENT 'the name of the source column'," +
//				"sourceType VARCHAR(30) NOT NULL COMMENT 'the type of the source column'," +
//				"targetDB VARCHAR(20) NOT NULL DEFAULT 'BestPeerServerGS' COMMENT 'the name of the target database'," +
//				"targetTable VARCHAR(30) NOT NULL COMMENT 'the name of the target table'," +
//				"targetVersion INT COMMENT 'the version of the target database, in case versioning is used'," +
//				"targetColumn VARCHAR(50) NOT NULL COMMENT 'the name of the target column'," +
//				"targetType VARCHAR(30) NOT NULL COMMENT 'the type of the target column'," +
//				"PRIMARY KEY (sourceDB,sourceTable,sourceVersion,sourceColumn)" +
//				") CHARACTER SET utf8 COLLATE utf8_bin");
//	}
	
	/**
	 * Implemented by Quang Hieu, Jun 01, 2008
	 * Share a table
	 * @param tableName
	 */
//	public void shareDatabase(Vector<String> listOfTables, Vector<Vector<String>> listOfIndexedColumns, Vector<Vector<String>> listOfUnindexedColumns)
//	{
//		Message result = null;
//    	Head head = new Head();
//
//		try{
//			PhysicalInfo physicalNodeInfo = this.serverpeer.getPhysicalInfo();
//
//			//index tables
//			Vector<LocalTableIndex> tableIndices = new Vector<LocalTableIndex>();
//			for (int i = 0; i < listOfTables.size(); i++)
//				tableIndices.add(new LocalTableIndex(listOfTables.get(i)));
//			head.setMsgType(MsgType.ERP_INSERT_TABLE_INDEX.getValue());
////			ERPInsertTableIndexBody body = new ERPInsertTableIndexBody(physicalNodeInfo,
////					physicalNodeInfo, tableIndices, null);
////			result = new Message(head, body);
////			this.serverpeer.sendMessage(physicalNodeInfo, result);
//
//			//index columns
//
//			//index data
//		}
//		catch(Exception e)
//		{
//			System.out.println("Error while sharing table");
//			e.printStackTrace();
//		}
//	}
	
	/**
	 * Implemented by Quang Hieu, Jun 01, 2008
	 * Index a column
	 * @param columnName
	 * @param tableName
	 */
//	public void indexData(String columnName, String tableName)
//	{
//		try
//		{
//			DBServer dbServer = new DBServer();
//			Statement stmt = dbServer.createStatement();
//		}
//		catch(Exception e)
//		{
//			System.out.println("Error while indexing data");
//			e.printStackTrace();
//		}
//	}
	
//	/**
//	 * mihai, june 27th, 2008
//	 * @param sourceColumn
//	 * @param targetColumn
//	 * @return
//	 * @see Serverinstance.matchColumns
//	 */
//	public int performColumnMatch(String sourceColumn, String targetColumn){
//		//return schemaMapper.addMatch(sourceColumn, targetColumn);
//		return 1;
//	}
//	/**
//	 * mihai, june 27th, 2008
//	 * @param sourceColumn
//	 * @param targetColumn
//	 * @return
//	 * @see Serverinstance.unmatchColumns
//	 */
//	public int performColumnUnmatch(String sourceColumn, String targetColumn){
//		return schemaMapper.removeMatch(sourceColumn, targetColumn);
//	}
	
//	public static void main(String[] args)
//	{
//		ServerRequestManager srm = new ServerRequestManager();
//
//		Connection conn;
//		Statement stmt;
//		try {
//			Class.forName(ServerPeer.SERVER_DB_DRIVER);
//			conn = DriverManager.getConnection(ServerPeer.SERVER_DB_URL, ServerPeer.SERVER_DB_USER, ServerPeer.SERVER_DB_PASS);
//			stmt = conn.createStatement();
//
//			srm.createMatchesTable(conn);
//		}
//		catch (Exception e) {
//			System.out.println("Error while opening a conneciton to database server: " + e.getMessage());
//			e.printStackTrace();
//			return;
//		}
//	}
}

