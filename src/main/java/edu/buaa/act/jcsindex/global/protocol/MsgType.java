/*
 * @(#) MsgType.java 1.0 2006-1-4
 * 
 * Copyright 2005, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.protocol;

/**
 * Define the message type of the <code>Message</code>.
 * NOTE: The value of 0xFFFFFFFF is preserved as the default value.
 * 
 * @author Xu Linhao
 * @version 1.0 2006-1-4
 */

public enum MsgType
{
	
	/* define COMMON MESSAGE 
	 * The message number begin from 0 */
	common									(0),
	/**
	 * When a user registers himself to the system, if successfully, 
	 * then return <code>RACK</code>; otherwise, return
	 * <code>RNCK</code>.
	 */
	REGISTER								(common.key),
	
	/**
	 * When a user signs in the system, if successfully, 
	 * then return <code>LACK</code>; otherwise, return
	 * <code>LNCK</code>.
	 */
	LOGIN									(common.key + 1),
	
	/**
	 * When check whether exists such a user, if have such an user,
	 * then return <code>USER_EXISTED</code> message; otherwise,
	 * the message <code>USER_NOT_EXIST</code> will be returned.
	 */
	CHECK_USER								(common.key + 2),
	
	/**
	 * When a peer signs in the system successfully, it will be registered
	 * into an <code>OnlinePeerMgr</code>. However, only when the peer
	 * can join the system via an existing super peer successfully, 
	 * then the peer will be displayed into the bootstrap server.
	 * So this message will use for indicating that a peer successfully
	 * joins the system via an existing super peer.
	 */
	JOIN_SUCCESS							(common.key + 3),
	
	/**
	 * If a peer cannot joins the system via an existing super peer,
	 * it should send this message to notify the bootstrap and then
	 * the bootstrap server will remove it from <code>OnlinePeerMgr</code>.
	 */
	JOIN_FAILURE							(common.key + 4),
	
	/**
	 * If a user registers to the system successfully, then 
	 * the bootstrap server returns this message.
	 */
	RACK			 						(common.key + 5),
	
	/**
	 * If a user cannot register to the system, then 
	 * the bootstrap server returns this message.
	 */
	RNCK 									(common.key + 6),
	
	/**
	 * If a user signs in the system successfully, then
	 * the bootstrap server returns this message.
	 */
	LACK								 	(common.key + 7),
	
	/**
	 * If a user signs in the system failed, then
	 * the bootstrap server returns this message.
	 */
	LNCK 									(common.key + 8),
	
	/**
	 * If cannot connect to the bootstrap server.
	 */
	CONN_FAILED	 				 			(common.key + 9),
	
	/**
	 * If the user has already existed.
	 */
	USER_EXISTED				 			(common.key + 10),
	
	/**
	 * If the user does not exist.
	 */
	USER_NOT_EXIST				 			(common.key + 11),
	
	/**
	 * If the password that the user input is invalid.
	 */
	PASSWORD_INVALID 						(common.key + 12),
	
	/**
	 * If the user session should not exist. Notice that this message is 
	 * not used for communicating with other peers.
	 */
	SESSION_SHOULD_NOT_EXIST 				(common.key + 13),
	
	/**
	 * The bootstrap server will send a ping message to each online peer at regular time interval,
	 * for testing whether the peer is still online now.
	 */
	PING									(common.key + 14),
	
	/**
	 * The peer received a ping message will reply a pong message to the bootstrap server,
	 * for indicating its online state.
	 */
	PONG									(common.key + 15),
	
	/**
	 * When a super peer joins the system, if the bootstrap server finds that 
	 * this super peer is the only online super peer, then it will notify
	 * this super peer this message. Then, this super peer can directly join
	 * the system without choosing an existing super peer.
	 */
	UNIQUE_SUPER_PEER						(common.key + 16),

	/**
	 * When a client peer wants to join the system, if there are not any super peers
	 * in the system, then the bootstrap server will reply this client peer for this 
	 * message. This client peer cannot join the system.
	 */
	NO_ONLINE_SUPER_PEER					(common.key + 17),		// if no online super peer
	
	/**
	 * When a peer wants to leave the system without any failure events, it will
	 * notify all peers for this event.
	 */
	I_WILL_LEAVE							(common.key + 18),		// if the peer leaves the network
	
	/**
	 * When bootstrap server leaves the network, it will broadcast a troubleshoot
	 * message to all super peers and each super peer will send a force-out message
	 * to all its client peers.
	 */
	TROUBLESHOOT							(common.key + 19),
	
	/**
	 * add a friend, used in messenger
	 */
	ADD_FRIEND								(common.key + 20),
	
	/**
	 * change a property
	 */
	Change_PROPERTY							(common.key + 21),
	
	/**
	 * send a schema update from bootstrap peer to superpeer
	 * 
	 * added by mihai, june 2nd 2008
	 */
	SCHEMA_UPDATE							(common.key+22),
	
	/* define PEER-PEER MESSAGE BELLOW 
	 * The message number begin from 100 */
	peer_to_peer							(common.key + 100),
	
	/**
	 * When a peer downloads a file from a remote peer, it will notify the peer
	 * for this event.
	 */
	DOWNLOAD								(peer_to_peer.key),
	
	/**
	 * When a peer finishes file downloading, it will notify the remote peer
	 * for this event.
	 */
	DOWNLOAD_ACK							(peer_to_peer.key + 1),
	
	/**
	 * When a peer can not finish file downloading, it will notify the remote peer
	 * for this event
	 */
	DOWNLOAD_NACK							(peer_to_peer.key + 2),
	
	/* define PEER-SUPERPEER MESSAGE BELLOW 
	 * The message number begin from 200 */
	peer_to_superpeer						(common.key + 200),
	
	
	/**
	 * When a peer tries to attach to a super peer, it will notify the super peer
	 * for this event.
	 */
	ATTACH_REQUEST							(peer_to_superpeer.key),
	
	/**
	 * When a peer attaches to a super peer successfully, the super peer will notify
	 * the client peer for this event. Upon receiving this message, the client peer
	 * enable its UI components and is ready for performing operations, such as search,
	 * share, upload, and download etc.
	 */
	ATTACH_SUCCESS							(peer_to_superpeer.key + 1),
	
	/**
	 * When a peer fails to attach to a super peer, the super peer will notify the 
	 * client for this event.
	 */
	ATTACH_FAILURE							(peer_to_superpeer.key + 2),
	
	/**
	 * When a peer shares some files to other peers, the index will be created and then
	 * the indexed meta data will be transferred to its super peer for constructing
	 * distributed index. 
	 */
	UPLOAD_INDEX_INFO						(peer_to_superpeer.key + 3),
	
	/**
	 * When a peer re-index its sharable files, the index will be re-transferred to 
	 * its super peer for updating the distributed index managed by all super peers.
	 */
	UPLOAD_REINDEX_INFO 					(peer_to_superpeer.key + 4),
	
	/**
	 * define type of query sent from knowlelege bank interface
	 * modified by Yu Bei
	 */
	KB_QUERY_TYPE							(peer_to_superpeer.key + 90),
	
	/**
	 * define type of file to be indexed sent from knowledge bank interface
	 * modified by Xu Linhao
	 */
	KB_INDEX_TYPE							(peer_to_superpeer.key + 91),
	
	/**
	 * define type of results for knowledge bank interface
	 * modified by Xu Linhao
	 */
	KB_RESULT								(peer_to_superpeer.key + 92),
	
	/**
	 * define type of results for knowledge bank interface
	 * modified by Xu Linhao
	 */
	KB_QUERY								(peer_to_superpeer.key + 93),
	
	/* define SUPERPEER-PEER MESSAGE BELLOW
	 * The message number begins from 300 */
	Superpeer_to_peer						(common.key + 300),
	
	/**
	 * If a super peer receives a troubleshoot message from bootstrap peer,
	 * then it will send a force-out message to all its client peers.
	 */
	FORCE_OUT								(Superpeer_to_peer.key),
	
	/**
	 * collecting results passed from super peer.
	 */
	RESULT									(Superpeer_to_peer.key + 1),
	
	/* define SUPERPPER-SUPERPEER MESSAGE BELLOW 
	 * The message number begin from 400 */
	superpeer_to_superpeer					(common.key + 400),
	/**
	 * If a super peer received a message
	 */
	SP_RECEIVED_MESSAGE						(superpeer_to_superpeer.key),
	
	/**
	 * When a super peer deletes some data items.
	 */
	SP_DELETE								(superpeer_to_superpeer.key + 1),
	SP_DELETE_BUNDLE						(superpeer_to_superpeer.key + 2),
	
	/**
	 * When a super peer inserts some data items.
	 */
	SP_INSERT								(superpeer_to_superpeer.key + 3),
	SP_INSERT_BUNDLE						(superpeer_to_superpeer.key + 4),
	
	/**
	 * When a super peer joins the network and is accepted
	 * by a certain super peer. 
	 */
	SP_JOIN_ACCEPT							(superpeer_to_superpeer.key + 5),
	
	/**
	 * When a super peer joins the network.
	 */
	SP_JOIN									(superpeer_to_superpeer.key + 6),
	
	/**
	 * Not sure, should be updated by QH. 
	 */
	SP_JOIN_FORCED							(superpeer_to_superpeer.key + 7),
	
	/**
	 * Not sure, should be updated by QH. 
	 */
	SP_JOIN_FORCED_FORWARD					(superpeer_to_superpeer.key + 8),
	
	/**
	 * When a super peer joins the network successfully, it
	 * needs to split the parent's index range.
	 */
	SP_JOIN_SPLIT_DATA						(superpeer_to_superpeer.key + 9),
	
	/**
	 * When a super peer tries to find a underload super peer.
	 */
	SP_LB_FIND_LIGHTLY_NODE					(superpeer_to_superpeer.key + 10),
	
	/**
	 * When a super peer gets the load information 
	 * before executing the load balance.
	 */
	SP_LB_GET_LOAD_INFO						(superpeer_to_superpeer.key + 11),
	
	/**
	 * Not sure, should be updated by QH. 
	 */
	SP_LB_GET_LOAD_INFO_REPLY				(superpeer_to_superpeer.key + 12),
	
	/**
	 * Not sure, should be updated by QH. 
	 */
	SP_LB_GET_LOAD_INFO_RESEND				(superpeer_to_superpeer.key + 13),
	
	/**
	 * When a super peer executes the load balance, if there
	 * does not exist any node to do rotation.
	 */
	SP_LB_NO_ROTATION_NODE					(superpeer_to_superpeer.key + 14),
	
	/**
	 * When a super peer executes the load balance, then 
	 * it updates its adjacent nodes.
	 */
	SP_LB_ROTATE_UPDATE_ADJACENT			(superpeer_to_superpeer.key + 15),
	
	/**
	 * When a super peer executes the load balance, then 
	 * its new adjacent nodes will send back this message to it.
	 */
	SP_LB_ROTATE_UPDATE_ADJACENT_REPLY		(superpeer_to_superpeer.key + 16),
	
	/**
	 * When a super peer executes the load balance, then 
	 * it updates its child nodes.
	 */
	SP_LB_ROTATE_UPDATE_CHILD				(superpeer_to_superpeer.key + 17),
	
	/**
	 * When a super peer executes the load balance, then 
	 * its new child nodes will send back this message to it.
	 */
	SP_LB_ROTATE_UPDATE_CHILD_REPLY			(superpeer_to_superpeer.key + 18),
	
	/**
	 * When a super peer executes the load balance, then 
	 * it updates its parent node.
	 */
	SP_LB_ROTATE_UPDATE_PARENT				(superpeer_to_superpeer.key + 19),
	
	/**
	 * When a super peer executes the load balance, then 
	 * its new parent node will send back this message to it.
	 */
	SP_LB_ROTATE_UPDATE_PARENT_REPLY		(superpeer_to_superpeer.key + 20),
	
	/**
	 * When a super peer executes the load balance, then 
	 * it updates its routing table.
	 */
	SP_LB_ROTATE_UPDATE_ROUTING_TABLE		(superpeer_to_superpeer.key + 21),
	
	/**
	 * When a super peer executes the load balance, then 
	 * all new nodes in the routing table will sends back
	 * this message to it.
	 */
	SP_LB_ROTATE_UPDATE_ROUTING_TABLE_REPLY	(superpeer_to_superpeer.key + 22),
	
	/**
	 * Not sure, should be updated by QH. 
	 */
	SP_LB_ROTATION_PULL						(superpeer_to_superpeer.key + 23),
	
	/**
	 * When a super peer executes the load balance, then 
	 * it updates its index range.
	 */
	SP_LB_SPLIT_DATA						(superpeer_to_superpeer.key + 24),
	
	/**
	 * Not sure, should be updated by QH. 
	 */
	SP_LB_SPLIT_DATA_RESEND					(superpeer_to_superpeer.key + 25),
	
	/**
	 * Not sure, should be updated by QH. 
	 */
	SP_LB_STABLE_POSITION					(superpeer_to_superpeer.key + 26),
	
	/**
	 * When a super peer leaves the network.
	 */
	SP_LEAVE								(superpeer_to_superpeer.key + 27),
	SP_LEAVE_URGENT							(superpeer_to_superpeer.key + 28),
	SP_LEAVE_NOTIFY							(superpeer_to_superpeer.key + 29),
	SP_LEAVE_NOTIFY_CLIENT					(superpeer_to_superpeer.key + 30),
	SP_PASS_CLIENT							(superpeer_to_superpeer.key + 31),
	
	/**
	 * Before a super peer leaves the network, it will send this message to find
	 * a super peer to replace his position after its departure.
	 */
	SP_LEAVE_FIND_REPLACEMENT_NODE			(superpeer_to_superpeer.key + 32),
	
	/**
	 * The super peer that will replace the position of the peer who will leave
	 * the network, will send back a reply message.
	 */
	SP_LEAVE_FIND_REPLACEMENT_NODE_REPLY	(superpeer_to_superpeer.key + 33),
	
	/**
	 * When a super peer leaves the network and notifies another super peer
	 * to replace his position if the tree balance cannot be satisfied.
	 */
	SP_LEAVE_REPLACEMENT					(superpeer_to_superpeer.key + 34),
	
	/**
	 * When a super peer's workload is imbalance or it found other super peers become
	 * overload or underload, then it will notify other peers to begin an load balance
	 * operation.
	 */
	SP_NOTIFY_IMBALANCE						(superpeer_to_superpeer.key + 35),
	
	/**
	 * When a super peer executes an exact search.
	 */
	SP_SEARCH_EXACT							(superpeer_to_superpeer.key + 36),
	SP_SEARCH_PAIR							(superpeer_to_superpeer.key + 37),
	SP_SEARCH_EXACT_BUNDLE					(superpeer_to_superpeer.key + 38),
	
	/**
	 * When a super peer gets the result of an exact search.
	 */
	SP_SEARCH_EXACT_RESULT					(superpeer_to_superpeer.key + 39),
	
	/**
	 * When a super peer executes a range search.
	 */
	SP_SEARCH_RANGE							(superpeer_to_superpeer.key + 40),		
	SP_SEARCH_RANGE_BUNDLE					(superpeer_to_superpeer.key + 41),
	
	/**
	 * When a super peer get the result of a range search.
	 */
	SP_SEARCH_RANGE_RESULT					(superpeer_to_superpeer.key + 42),
	
	/**
	 * When a super peer updates its adjacent links.
	 */
	SP_UPDATE_ADJACENT_LINK					(superpeer_to_superpeer.key + 43),	
	
	/**
	 * When a super peer updates its index range with both 
	 * the maximum and minimum values.
	 */
	SP_UPDATE_MAX_MIN_VALUE					(superpeer_to_superpeer.key + 44),
	
	/**
	 * When a super peer updates its routing table.
	 */
	SP_UPDATE_ROUTING_TABLE					(superpeer_to_superpeer.key + 45),
	
	/**
	 * When a super peer updates its routing table directly (for what reason?)
	 */
	SP_UPDATE_ROUTING_TABLE_DIRECTLY		(superpeer_to_superpeer.key + 46),
	
	/**
	 * When a super peer updates its routing table indirectly (for what reason?)
	 */
	SP_UPDATE_ROUTING_TABLE_INDIRECTLY		(superpeer_to_superpeer.key + 47),
	
	/**
	 * When a super peer gets the reply from remote super peers for 
	 * updating routing table.
	 */
	SP_UPDATE_ROUTING_TABLE_REPLY			(superpeer_to_superpeer.key + 48),

	/**
	 * When a super peer looks up adjacent node of a failed node
	 */
	SP_LI_ADJACENT							(superpeer_to_superpeer.key + 49),
	SP_LI_ADJACENT_ROOT						(superpeer_to_superpeer.key + 50),
	
	/**
	 * When a super peer replies the SP_LI_ADJACENT request
	 */
	SP_LI_ADJACENT_REPLY					(superpeer_to_superpeer.key + 51),
	SP_LI_ADJACENT_ROOT_REPLY				(superpeer_to_superpeer.key + 52),
	
	/**
	 * When a super peer looks up routing table information of a failed node
	 */
	SP_LI_ROUTING_TABLE						(superpeer_to_superpeer.key + 53),
	
	/**
	 * When a super peer replies the SP_LI_ROUTING_TABLE request
	 */
	SP_LI_ROUTING_TABLE_REPLY				(superpeer_to_superpeer.key + 54),
	
	/**
	 * When a super peer replies information of child nodes for the parent
	 * of the failed node
	 */
	SP_LI_CHILD_REPLY						(superpeer_to_superpeer.key + 55),
	
	/**
	 * When a super peer wants to update information of parent node of the
	 * failed node
	 */
	SP_LI_UPDATE_PARENT						(superpeer_to_superpeer.key + 56),
	
	/**
	 * When a super wants to notify parent of a failed node about failure
	 */
	SP_NOTIFY_FAILURE						(superpeer_to_superpeer.key + 57),
	
	/**
	 * When the index of a document is updated at super peer side 
	 */
	SP_UPDATE								(superpeer_to_superpeer.key + 58),
	SP_UPDATE_BUNDLE						(superpeer_to_superpeer.key + 59),

	/**
	 * TODO: 添加新的消息类型
	 * When publish a jcsIndex
	 */
	SP_PUBLISH								(superpeer_to_superpeer.key + 60),
	SP_PUBLISH_PARENT						(superpeer_to_superpeer.key + 61),
	SP_UPDATE_TAG						(superpeer_to_superpeer.key + 62),
	
	/* define CA MESSAGE BELLOW */
	cert_authority							(common.key + 600),
	/**
	 * When a peer requires a certificate from the certificate authority.
	 */
	CA_GET_CERT       		  				(cert_authority.key),
	
	/**
	 * When the certificate authority returns a certificate to the request peer.
	 */
	CA_RE_CERT        		 			 	(cert_authority.key + 1),
	
	/**
	 * When the certificate authority tries to validate a user by his key.
	 */
	CA_VALIDATE_USER  						(cert_authority.key + 2),
	
	/**
	 * If the action that the certificate authority validate a user by his key success.
	 */
	CA_VALIDATE_SUCCESS					 	(cert_authority.key + 3),
	
	/**
	 * If the action that the certificate authority validate a user by his key fails.
	 */
	CA_VALIDATE_FAILURE					   	(cert_authority.key + 4),
	
	/**
	 * If a user tries to create a new user group.
	 */
	CA_CREATE_GROUP   					  	(cert_authority.key + 5),
	
	/**
	 * If the user group has been created successfully.
	 */
	CA_CREATE_GROUP_SUCCESS				  	(cert_authority.key + 6),
	
	/**
	 * If the user group has not been created.
	 */
	CA_CREATE_GROUP_FAILURE				 	(cert_authority.key + 7),
	
	/**
	 * If a user tries to delete a user.
	 */
	CA_DELETE_USER    					 	(cert_authority.key + 8),
	
	/**
	 * If the user has been deleted successfully.
	 */
	CA_DELETE_USER_SUCCESS				 	(cert_authority.key + 9),
	
	/**
	 * If the user has not been deleted.
	 */
	CA_DELETE_USER_FAILURE				 	(cert_authority.key + 10),
	
	/**
	 * If a user tries to delete a user group.
	 */
	CA_DELETE_GROUP    					 	(cert_authority.key + 11),
	
	/**
	 * If the user group has been deleted successfully.
	 */
	CA_DELETE_GROUP_SUCCESS				 	(cert_authority.key + 12),
	
	/**
	 * If the user group has not been deleted.
	 */
	CA_DELETE_GROUP_FAILURE				 	(cert_authority.key + 13),
	
	/**
	 * If the owner of a user group tries to inviate a user to join his user group.
	 */
	CA_ADD_USER_TO_GROUP 					(cert_authority.key + 14),
	
	/**
	 * If a user is invited to join a user group successfully.
	 */
	CA_ADD_USER_TO_GROUP_SUCCESS			(cert_authority.key + 15),
	
	/**
	 * If a user cannot be invited into a user group.
	 */
	CA_ADD_USER_TO_GROUP_FAILURE			(cert_authority.key + 16),
	
	/**
	 * When the certificate authority tries to get the group list from a user.
	 */
	CA_GET_GROUPLIST    					(cert_authority.key + 17),
	
	/**
	 * If get the group list successfully.
	 */
	CA_GET_GROUPLIST_SUCCESS				(cert_authority.key + 18),
	
	/**
	 * If get the group list failure.
	 */
	CA_GET_GROUPLIST_FAILURE				(cert_authority.key + 19),
	
	/**
	 * When the certificate authority tries to get all users in a certain user group.
	 */
	CA_GET_USERLIST     					(cert_authority.key + 20),
	
	/**
	 * If all users in a certain user group are obtained successfully.
	 */
	CA_GET_USERLIST_SUCCESS					(cert_authority.key + 21),
	
	/**
	 * If all users in a certain user group cannot be obtained.
	 */
	CA_GET_USERLIST_FAILURE					(cert_authority.key + 22),
    
	/**
	 * When the certificate authority leaves the system.
	 */
	CA_LEAVE            					(cert_authority.key + 23),
	
	/* define relational search message below */
	database                                (cert_authority.key + 100),
	
	/**
	 * insert range index of a table
	 */
	DB_INSERT_RANGE_INDEX                   (database.key + 1),
	
	/**
	 * insert tuple index of a table
	 */
	DB_INSERT_TUPLE_INDEX                   (database.key + 2),
	
	/**
	 * insert new tuple index
	 */
	DB_INDEX_NOTIFICATION                   (database.key + 3),
	
	/**
	 * relational query
	 */
	DB_QUERY                                (database.key + 4),
	
	/**
	 * query result
	 */
	DB_QUERY_RESULT                         (database.key + 5),
	
	/* define EXTENDED CONFIRM MESSAGE BELOW */
	/**
	 * This message is only for demonstration	 * 
	 */
	PROJECT_MYMESSAGE                        (common.key + 800),
    
	/* protocols used to implement a corporate network */
	erp_message								 (common.key + 1000),
	
	/**
	 * to insert table index
	 */
	ERP_INSERT_TABLE_INDEX					 (erp_message.key + 1),
	
	/**
	 * to insert column index
	 */
	ERP_INSERT_COLUMN_INDEX					 (erp_message.key + 2),
	
	/**
	 * to insert data index
	 */
	ERP_INSERT_DATA_INDEX					(erp_message.key + 3),
	
	/**
	 * to delete table index
	 */
	ERP_DELETE_TABLE_INDEX					(erp_message.key + 4),
	
	/**
	 * to delete column index
	 */
	ERP_DELETE_COLUMN_INDEX					(erp_message.key + 5),
	
	/**
	 * to delete data index
	 */
	ERP_DELETE_DATA_INDEX					(erp_message.key + 6),

	/**
	 * to delete column index
	 */
	ERP_UPDATE_COLUMN_INDEX					(erp_message.key + 7),

	/**
	 * to update data index
	 */
	ERP_UPDATE_DATA_INDEX					(erp_message.key + 8),
	
	
	
	//VHTam: add following code to the next //end VHTam
	// define ACCESS CONTROL MESSAGE BELLOW
	// july 8th 2008
	
	access_control							(common.key + 2000),
	/**
	 * normal peer request for role-setting update
	 */
	ACCESS_CONTROL_ROLE_REQUEST       		  				(access_control.key),
	/**
	 * bootstrap peer respond to role-setting update
	 */
	ACCESS_CONTROL_ROLE_RESULT       		  				(access_control.key+1),
	/**
	 * bootstrap peer's message of user update 
	 */
	ACCESS_CONTROL_BOOTSTRAP_USER_UPDATE       		  		(access_control.key+2),
	/**
	 * normal peer's message of user update 
	 */
	ACCESS_CONTROL_NORMAL_PEER_USER_UPDATE       		  	(access_control.key+3),
	
	//end VHTam
	
	// David adds following
	index_search                  (common.key + 4000),
	TABLE_INDEX_SEARCH            (index_search.key),
	TABLE_INDEX_SEARCH_RESULT     (index_search.key + 1),
	QUERY_PEER                    (index_search.key + 2),
	QUERY_PEER_RESULT					    (index_search.key + 3),
	COLUMN_INDEX_SEARCH           (index_search.key + 10),
	// end David adds
	
	// Wu Sai adds the message type for getting the query result
	query_result				  (common.key + 5000),
	TABLE_RETRIEVAL			      (query_result.key),
	TABLE_DATA					  (query_result.key + 1),
	/**
	 * the default and empty message type and no action should be
	 * responsible for processing this message
	 */
	DEFAULT									(Integer.MAX_VALUE);	// the default message type
	
	/* define message type bellow */
	private final int key;
	
	MsgType(int key)
	{
		this.key = key;
	}
	
	/**
	 * Get the value of the message type.
	 * 
	 * @return the value of the message type
	 */
	public int getValue()
	{
		return this.key;
	}
	
	/**
	 * Check if the message type exists.
	 * 
	 * @param msgType the message type
	 * @return <code>true</code> if exists; otherwise, <code>false</code>
	 */
	public static boolean checkValue(int msgType)
	{
		for (MsgType m: MsgType.values())
		{
			if (m.getValue() == msgType)
			{
				return true;
			}
		}
		return false;
	}

	/**
	 * Get the description of the <code>MsgType</code>.
	 * 
	 * @param msgType the message type
	 * @return the string used for describing the <code>MsgType</code> 
	 */
	public static String description(int msgType)
	{
		String result = "";
		
		// ---------- COMMON EVENT ------------
		if (MsgType.SCHEMA_UPDATE.getValue()==msgType){
			result = "Received a schema update from bootstrap node";
		}
		if (MsgType.REGISTER.getValue() == msgType)
		{
			result = "REGISTER";
		}
		else if (MsgType.LOGIN.getValue() == msgType)
		{
			result = "LOGIN";
		}	
		else if (MsgType.CHECK_USER.getValue() == msgType)
		{
			result = "CHECK USER";
		}
		else if (MsgType.JOIN_SUCCESS.getValue() == msgType)
		{
			result = "JOIN NETWORK SUCCESSFULLY";
		}	
		else if (MsgType.JOIN_FAILURE.getValue() == msgType)
		{
			result = "JOIN NETWORK FAILURE";
		}
		else if (MsgType.RACK.getValue() == msgType)
		{
			result = "REGISTER SUCCESSFULLY";
		}	
		else if (MsgType.RNCK.getValue() == msgType)
		{
			result = "REGISTER FAILURE";
		}
		else if (MsgType.LACK.getValue() == msgType)
		{
			result = "LOGIN SUCCESSFULLY";
		}
		else if (MsgType.LNCK.getValue() == msgType)
		{
			result = "LOGIN FAILURE";
		}
		else if (MsgType.CONN_FAILED.getValue() == msgType)
		{
			result = "CONNECT TO BOOTSTRAP SERVER FAILURE";
		}
		else if (MsgType.USER_EXISTED.getValue() == msgType)
		{
			result = "USER EXIST";
		}	
		else if (MsgType.USER_NOT_EXIST.getValue() == msgType)
		{
			result = "USER DOES NOT EXIST";
		}
		else if (MsgType.PASSWORD_INVALID.getValue() == msgType)
		{
			result = "INVALID PASSWORD";
		}	
		else if (MsgType.SESSION_SHOULD_NOT_EXIST.getValue() == msgType)
		{
			result = "USER SESSION SHOULD NOT EXIST";
		}
		else if (MsgType.PING.getValue() == msgType)
		{
			result = "PING";
		}	
		else if (MsgType.PONG.getValue() == msgType)
		{
			result = "PONG";
		}
		else if (MsgType.UNIQUE_SUPER_PEER.getValue() == msgType)
		{
			result = "ONLY ONE SUPER PEER ONLINE";
		}
		else if (MsgType.NO_ONLINE_SUPER_PEER.getValue() == msgType)
		{
			result = "NO ONLINE SUPER PEER";
		}
		else if (MsgType.I_WILL_LEAVE.getValue() == msgType)
		{
			result = "PEER DEPARTURE";
		}
		else if (MsgType.ATTACH_REQUEST.getValue() == msgType)
		{
			result = "CLIENT PEER TRY TO ATTACH TO SUPER PEER";
		}	
		else if (MsgType.ATTACH_SUCCESS.getValue() == msgType)
		{
			result = "CLIENT PEER ATTACH TO SUPER PEER SUCCESSFULLY";
		}
		else if (MsgType.ATTACH_FAILURE.getValue() == msgType)
		{
			result = "CLIENT PEER ATTACH TO SUPER PEER FAILURE";
		}	
		else if (MsgType.UPLOAD_INDEX_INFO.getValue() == msgType)
		{
			result = "CLIENT PEER UPLOAD INDEX TO SUPER PEER";
		}
		else if (MsgType.UPLOAD_REINDEX_INFO.getValue() == msgType)
		{
			result = "CLIENT PEER UPLOAD RENEWED-INDEX TO SUPER PEER";
		}	
		// ----------- FOR SUPER PEER TO PEER ------------
		else if (MsgType.FORCE_OUT.getValue() == msgType)
		{
			result = "CLIENT PEER ARE FORCED OUT";
		}	
		else if (MsgType.RESULT.getValue() == msgType)
		{
			result = "CLIENT PEER GETS RESULTS FROM SUPER PEER";
		}	
		// ----------- FOR SUPER PEER TO SUPER PEER -------------
		else if (MsgType.SP_RECEIVED_MESSAGE.getValue() == msgType)
		{
			result = "SUPER PEER RECEIVED MESSAGE FROM OTHERS";
		}
		else if (MsgType.SP_DELETE.getValue() == msgType)
		{
			result = "SUPER PEER DELETE A DATA ITEM";
		}
		else if (MsgType.SP_DELETE_BUNDLE.getValue() == msgType)
		{
			result = "SUPER PEER DELETE DATA ITEMS";
		}
		else if (MsgType.SP_INSERT.getValue() == msgType)
		{
			result = "SUPER PEER INSERT A DATA ITEM";
		}
		else if (MsgType.SP_INSERT_BUNDLE.getValue() == msgType)
		{
			result = "SUPER PEER INSERT DATA ITEMS";
		}	
		else if (MsgType.SP_JOIN.getValue() == msgType)
		{
			result = "SUPER PEER JOIN NETWORK REQUEST";
		}
		else if (MsgType.SP_JOIN_ACCEPT.getValue() == msgType)
		{
			result = "SUPER PEER JOIN NETWORK SUCCESSFULLY";
		}	
		else if (MsgType.SP_JOIN_FORCED.getValue() == msgType)
		{
			result = "SUPER FORCED-JOIN NETWORK";
		}
		else if (MsgType.SP_JOIN_FORCED_FORWARD.getValue() == msgType)
		{
			result = "SUPER PEER FORCED-JOIN NETWORK AND FORWARD REQUEST TO OTHERS";
		}	
		else if (MsgType.SP_JOIN_SPLIT_DATA.getValue() == msgType)
		{
			result = "SUPER PEER JOIN NETWORK AND SPLIT DATA RANGE";
		}
		else if (MsgType.SP_LB_FIND_LIGHTLY_NODE.getValue() == msgType)
		{
			result = "SUPER PEER TRY TO FIND UNDERLOAD NODE";
		}
		else if (MsgType.SP_LB_GET_LOAD_INFO.getValue() == msgType)
		{
			result = "SUPER PEER REQUEST LOAD INFO";
		}
		else if (MsgType.SP_LB_GET_LOAD_INFO_REPLY.getValue() == msgType)
		{
			result = "SUPER PEER GET REPLY WITH LOAD INFO";
		}
		else if (MsgType.SP_LB_GET_LOAD_INFO_RESEND.getValue() == msgType)
		{
			result = "SUPER PEER REQUEST LOAD INFO AGAIN";
		}
		else if (MsgType.SP_LB_NO_ROTATION_NODE.getValue() == msgType)
		{
			result = "NO ROTATION NODE FOR LOAD BALANCE";
		}
		else if (MsgType.SP_LB_ROTATE_UPDATE_ADJACENT.getValue() == msgType)
		{
			result = "TRY TO UPDATE ADJACENT LINK AFTER TREE ROTATION";
		}
		else if (MsgType.SP_LB_ROTATE_UPDATE_ADJACENT_REPLY.getValue() == msgType)
		{
			result = "REPLY REQUEST OF ADJACENT LINK UPDATE AFTER TREE ROTATION";
		}
		else if (MsgType.SP_LB_ROTATE_UPDATE_CHILD.getValue() == msgType)
		{
			result = "TRY TO UPDATE CHILD LINK AFTER TREE ROTATION";
		}
		else if (MsgType.SP_LB_ROTATE_UPDATE_CHILD_REPLY.getValue() == msgType)
		{
			result = "REPLY REQUEST OF CHILD LINK UPDATE AFTER TREE ROTATION";
		}
		else if (MsgType.SP_LB_ROTATE_UPDATE_PARENT.getValue() == msgType)
		{
			result = "TRY TO UPDATE PARENT LINK AFTER TREE ROTATION";
		}
		else if (MsgType.SP_LB_ROTATE_UPDATE_PARENT_REPLY.getValue() == msgType)
		{
			result = "REPLY REQUEST OF PARENT LINK UPDATE AFTER TREE ROTATION";
		}
		else if (MsgType.SP_LB_ROTATE_UPDATE_ROUTING_TABLE.getValue() == msgType)
		{
			result = "TRY TO UPDATE ROUTING TABLE AFTER TREE ROTATION";
		}
		else if (MsgType.SP_LB_ROTATE_UPDATE_ROUTING_TABLE_REPLY.getValue() == msgType)
		{
			result = "REPLY REQUEST OF ROUTING TABLE AFTER TREE ROTATION";
		}
		else if (MsgType.SP_LB_ROTATION_PULL.getValue() == msgType)
		{
			result = "PULL AFTER TREE ROTATION";
		}
		else if (MsgType.SP_LB_SPLIT_DATA.getValue() == msgType)
		{
			result = "TRY TO SPLIT DATA RANGE AFTER LOAD BALANCE";
		}
		else if (MsgType.SP_LB_SPLIT_DATA_RESEND.getValue() == msgType)
		{
			result = "RETRY TO SPLIT DATA RANGE AFTER LOAD BALANCE";
		}
		else if (MsgType.SP_LB_STABLE_POSITION.getValue() == msgType)
		{
			result = "FIND STABLE POSITION WHEN LOAD BALANCE";
		}
		else if (MsgType.SP_LEAVE.getValue() == msgType)
		{
			result = "SUPER PEER LEAVE NETWORK";
		}
		else if (MsgType.SP_LEAVE_URGENT.getValue() == msgType)
		{
			result = "SUPER PEER FAILURE";
		}
		else if (MsgType.SP_LEAVE_NOTIFY.getValue() == msgType)
		{
			result = "SUPER PEER LEAVE AND NOTIFY OTHER SUPER PEERS";
		}
		else if (MsgType.SP_LEAVE_NOTIFY_CLIENT.getValue() == msgType)
		{
			result = "SUPER PEER LEAVE AND NOTIFY ITS CLIENT PEERS";
		}
		else if (MsgType.SP_PASS_CLIENT.getValue() == msgType)
		{
			result = "SUPER PEER LEAVE AND PASS CLIENT PEERS TO OTHERS";
		}
		else if (MsgType.SP_LEAVE_FIND_REPLACEMENT_NODE.getValue() == msgType)
		{
			result = "SUPER PEER LEAVE AND FIND ANOTHER ONE TO REPLACE ITS POSITION";
		}
		else if (MsgType.SP_LEAVE_FIND_REPLACEMENT_NODE_REPLY.getValue() == msgType)
		{
			result = "REPLY OF FINDING ANOTHER SUPER PEER TO REPLACE REQUESTER";
		}
		else if (MsgType.SP_LEAVE_REPLACEMENT.getValue() == msgType)
		{
			result = "SUPER PEER LEAVE AND NOTIFY OTHERS TO REPLACE IT";
		}
		else if (MsgType.SP_NOTIFY_IMBALANCE.getValue() == msgType)
		{
			result = "LOAD IMBALANCE NOTIFICATION";
		}
		else if (MsgType.SP_SEARCH_EXACT.getValue() == msgType)
		{
			result = "EXACT SEARCH";
		}
		else if (MsgType.SP_SEARCH_PAIR.getValue() == msgType)
		{
			result = "USER QUERY";
		}
		else if (MsgType.SP_SEARCH_EXACT_BUNDLE.getValue() == msgType)
		{
			result = "SEARCH A SET OF DATA";
		}
		else if (MsgType.SP_SEARCH_EXACT_RESULT.getValue() == msgType)
		{
			result = "GET EXACT SEARCH RESULT";
		}
		else if (MsgType.SP_SEARCH_RANGE.getValue() == msgType)
		{
			result = "SEARCH DATA RANGE";
		}
		else if (MsgType.SP_SEARCH_RANGE_BUNDLE.getValue() == msgType)
		{
			result = "SEARCH A SET OF DATA RANGES";
		}
		else if (MsgType.SP_SEARCH_RANGE_RESULT.getValue() == msgType)
		{
			result = "GET RANGE SEARCH RESULT";
		}
		else if (MsgType.SP_UPDATE_ADJACENT_LINK.getValue() == msgType)
		{
			result = "UPDATE ADJACENT LINK";
		}
		else if (MsgType.SP_UPDATE_MAX_MIN_VALUE.getValue() == msgType)
		{
			result = "UPDATE MAX-MIN VALUE";
		}
		else if (MsgType.SP_UPDATE_ROUTING_TABLE.getValue() == msgType)
		{
			result = "UPDATE ROUTING TABLE";
		}
		else if (MsgType.SP_UPDATE_ROUTING_TABLE_DIRECTLY.getValue() == msgType)
		{
			result = "DIRECTLY UPDATE ROUTING TABLE";
		}
		else if (MsgType.SP_UPDATE_ROUTING_TABLE_INDIRECTLY.getValue() == msgType)
		{
			result = "INDIRECTLY UPDATE ROUTING TABLE";
		}
		else if (MsgType.SP_UPDATE_ROUTING_TABLE_REPLY.getValue() == msgType)
		{
			result = "REPLY OF ROUTING TABLE UPDATE";
		}
		else if (MsgType.SP_LI_ADJACENT.getValue() == msgType)
		{
			result = "LOOK UP ADJACENT INFO OF A FAILED NODE";
		}
		else if (MsgType.SP_LI_ADJACENT_ROOT.getValue() == msgType)
		{
			result = "LOOK UP ADJACENT INFO OF THE FAILED ROOT NODE";
		}
		else if (MsgType.SP_LI_ADJACENT_REPLY.getValue() == msgType)
		{
			result = "REPLY OF LOOK UP ADJACENT INFO OF A FAILED NODE";
		}
		else if (MsgType.SP_LI_ADJACENT_ROOT_REPLY.getValue() == msgType)
		{
			result = "REPLY OF LOOK UP ADJACENT INFO OF THE FAILED ROOT NODE";
		}
		else if (MsgType.SP_LI_ROUTING_TABLE.getValue() == msgType)
		{
			result = "LOOK UP ROUTING TABLE INFO OF A FAILED NODE";
		}
		else if (MsgType.SP_LI_ROUTING_TABLE_REPLY.getValue() == msgType)
		{
			result = "REPLY ROUTING TABLE INFO OF A FAILED NODE";			
		}
		else if (MsgType.SP_LI_CHILD_REPLY.getValue() == msgType)
		{
			result = "REPLY CHILD INFO OF A FAILED NODE";
		}
		else if (MsgType.SP_LI_UPDATE_PARENT.getValue() == msgType)
		{
			result = "UPDATE PARENT INFO OF A CHILD OF A FAILED NODE";
		}
		else if (MsgType.SP_NOTIFY_FAILURE.getValue() == msgType)
		{
			result = "NOTIFY FAILURE OF A NODE";
		}
		else if (MsgType.SP_UPDATE.getValue() == msgType)
		{
			result = "UPDATE INDEX OF A DOCUMENT";
		}
		else if (MsgType.SP_UPDATE_BUNDLE.getValue() == msgType)
		{
			result = "UPDATE INDEX OF A BUNDLE OF TERMS";
		} else if (MsgType.SP_PUBLISH.getValue() == msgType) {
			result = "PUBLISH JCSTUPLE";
		} else if (MsgType.SP_PUBLISH_PARENT.getValue() == msgType) {
			result = "PUBLISH PARENT JCSTUPLE";
		} else if (MsgType.SP_UPDATE_TAG.getValue() == msgType) {
			result = "UPDATE NODE TAG";
		}
		// -------------- FOR CERTICATE AUTHORITY --------------
		else if (MsgType.CA_GET_CERT.getValue() == msgType)
		{
			result = "PEER REQUEST CERTIFICATE FROM CA";
		}
		else if (MsgType.CA_RE_CERT.getValue() == msgType)
		{
			result = "CA RETURN CERTIFICATE TO PEER";
		}
		else if (MsgType.CA_VALIDATE_USER.getValue() == msgType)
		{
			result = "CA TRY TO VALIDATE USER";
		}
		else if (MsgType.CA_VALIDATE_SUCCESS.getValue() == msgType)
		{
			result = "CA SUCCESS VALIDATING USER";
		}
		else if (MsgType.CA_VALIDATE_FAILURE.getValue() == msgType)
		{
			result = "CA FAIL TO VALIDATE USER";
		}
		else if (MsgType.CA_CREATE_GROUP.getValue() == msgType)
		{
			result = "CA TRY TO CREATE GROUP";
		}
		else if (MsgType.CA_CREATE_GROUP_SUCCESS.getValue() == msgType)
		{
			result = "CA SUCCESS CREATING GROUP";
		}	
		else if (MsgType.CA_CREATE_GROUP_FAILURE.getValue() == msgType)
		{
			result = "CA FAIL TO CREATE GROUP";
		}
		else if (MsgType.CA_DELETE_USER.getValue() == msgType)
		{
			result = "CA TRY TO DELETE USER";
		}	
		else if (MsgType.CA_DELETE_USER_SUCCESS.getValue() == msgType)
		{
			result = "CA SUCCESS DELETING USER";
		}
		else if (MsgType.CA_DELETE_USER_FAILURE.getValue() == msgType)
		{
			result = "CA FAIL TO DELETE USER";
		}	
		else if (MsgType.CA_DELETE_GROUP.getValue() == msgType)
		{
			result = "CA TRY TO DELETE GROUP";
		}
		else if (MsgType.CA_DELETE_GROUP_SUCCESS.getValue() == msgType)
		{
			result = "CA SUCCESS DELETING GROUP";
		}
		else if (MsgType.CA_DELETE_GROUP_FAILURE.getValue() == msgType)
		{
			result = "CA FAIL TO DELETE GROUP";
		}
		else if (MsgType.CA_ADD_USER_TO_GROUP.getValue() == msgType)
		{
			result = "CA TRY TO ADD USER INTO GROUP";
		}
		else if (MsgType.CA_ADD_USER_TO_GROUP_SUCCESS.getValue() == msgType)
		{
			result = "CA SUCCESS ADDING USER INTO GROUP";
		}	
		else if (MsgType.CA_ADD_USER_TO_GROUP_FAILURE.getValue() == msgType)
		{
			result = "CA FAIL TO ADD USER INTO GROUP";
		}
		else if (MsgType.CA_GET_GROUPLIST.getValue() == msgType)
		{
			result = "CA TRY TO GET GROUP LIST";
		}	
		else if (MsgType.CA_GET_GROUPLIST_SUCCESS.getValue() == msgType)
		{
			result = "CA SUCCESS GETTING GROUP LIST";
		}
		else if (MsgType.CA_GET_GROUPLIST_FAILURE.getValue() == msgType)
		{
			result = "CA FAIL TO GET GROUP LIST";
		}	
		else if (MsgType.CA_GET_USERLIST.getValue() == msgType)
		{
			result = "CA TRY TO GET USER LIST";
		}
		else if (MsgType.CA_GET_USERLIST_SUCCESS.getValue() == msgType)
		{
			result = "CA SUCCESS GETTING USER LIST";
		}
		else if (MsgType.CA_GET_USERLIST_FAILURE.getValue() == msgType)
		{
			result = "CA FAIL TO GET USER LIST";
		}
		else if (MsgType.CA_LEAVE.getValue() == msgType)
		{
			result = "CA LEAVE NETWORK";
		}
		// ---------- DOWNLOAD EVENT ------------
		else if (MsgType.KB_INDEX_TYPE.getValue() == msgType)
		{
			result = "INDEX MESSAGE FROM KNOWLEDGEBANK";			
		}
		else if (MsgType.KB_QUERY_TYPE.getValue() == msgType)
		{
			result = "QUERY MESSAGE FROM KNOWLEDGEBANK";
		}
		else if (MsgType.KB_RESULT.getValue() == msgType)
		{
			result = "RESULT MESSAGE TO KNOWLEDGEBANK";
		}
		else if (MsgType.KB_QUERY.getValue() == msgType)
		{
			result = "QUERY MESSAGE FROM KNOWLEDGEBANK";
		}
		// ---------- DOWNLOAD EVENT ------------
		else if (MsgType.DOWNLOAD.getValue() == msgType)
		{
			result = "Download Request";			
		}
		else if (MsgType.DOWNLOAD_ACK.getValue() == msgType)
		{
			result = "Download Acknowledge";
		}
		else if (MsgType.DOWNLOAD_NACK.getValue() == msgType)
		{
			result = "Download Negative Acknowledge";
		}
		// ----------DATABASE EVENT--------------
		else if (MsgType.DB_INSERT_RANGE_INDEX.getValue() == msgType)
		{
			result = "Create Database Range Index";
		}
		else if (MsgType.DB_INSERT_TUPLE_INDEX.getValue() == msgType)
		{
			result = "Create Database Tuple Index";
		}
		else if (MsgType.DB_INDEX_NOTIFICATION.getValue() == msgType)
		{
			result = "Database Index Creation Notification";
		}
		else if (MsgType.DB_QUERY.getValue() == msgType)
		{
			result = "Database Query";
		}
		else if (MsgType.DB_QUERY_RESULT.getValue() == msgType)
		{
			result = "Database Query Result";
		}
		
		// ---------- ACCESS CONTROL VALUE -------------
		
		//VHTam: add following code to the next //end VHTam
		//description of access control message type
		
		else if (MsgType.ACCESS_CONTROL_ROLE_REQUEST.getValue() == msgType)
		{
			result = "Request from normal peer to bootrap for updating Role setting of Service provider";
		}			
		else if (MsgType.ACCESS_CONTROL_ROLE_RESULT.getValue() == msgType)
		{
			result = "Result from bootstrap to normal peer for returning Role setting of Service provider";
		}			
		else if (MsgType.ACCESS_CONTROL_BOOTSTRAP_USER_UPDATE.getValue() == msgType)
		{
			result = "Bootstrap peer's message of user update";
		}			
		else if (MsgType.ACCESS_CONTROL_NORMAL_PEER_USER_UPDATE.getValue() == msgType)
		{
			result = "Normal peer's message of user update";
		}			
		
		//end VHTam
		
		// David added this
		else if (MsgType.TABLE_INDEX_SEARCH.getValue() == msgType){
			result = "Search table index";
		}else if (MsgType.COLUMN_INDEX_SEARCH.getValue() == msgType){
			result = "Search column index";;
		}else if (MsgType.TABLE_INDEX_SEARCH_RESULT.getValue() == msgType){
			result = "Table index search result";
		} else if (MsgType.QUERY_PEER.getValue() == msgType) {
			result = "Query peer";
		} else if (MsgType.QUERY_PEER_RESULT.getValue() == msgType) {
			result = "Query peer result";
		}
		// end David added this
		
		else if (MsgType.TABLE_RETRIEVAL.getValue() == msgType){
			result = "Retrieve tuples from remote table";
		}
		else if (MsgType.TABLE_DATA.getValue() == msgType){
			result = "Tuples from remote table";
		}

		/*
		 * add codes to handle your message type
		 */
		else if (MsgType.PROJECT_MYMESSAGE.getValue() == msgType)
		{
			result = "This is my customized message";
		}			
		// ---------- DEFAULT VALUE -------------
		else
		{
			result = "UNKNOWN MESSAGE TYPE: " + msgType;
		}
		
		return result;
	}
	
}
