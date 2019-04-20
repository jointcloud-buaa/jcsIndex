/*
 * @(#) ServerEventDispatcher.java 1.0 2006-2-3
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.peer.management;

//import buaa.act.jointcloudstorage.accesscontrol.normalpeer.CreateUserFromBoostrapListener;

import edu.buaa.act.jcsindex.global.AbstractInstance;
import edu.buaa.act.jcsindex.global.peer.event.*;

/**
 * Implement a concrete event manager used for monitoring
 * all incoming socket connections related to the bootstrap server.
 * 
 * @author Xu Linhao
 * @version 1.0 2006-2-3
 */

public class ServerEventDispatcher extends AbstractEventDispatcher
{

	public ServerEventDispatcher(AbstractInstance instance)
	{
		super(instance);
	}

	@Override
	public void registerActionListeners() 
	{
		/* used for monitoring client peer's events */
		this.addActionListener(new AttachListener(instance));
		this.addActionListener(new LeaveListener(instance));
		this.addActionListener(new ForceOutListener(instance));
		//this.addActionListener(new SchemaUpdateListener(instance));
		
		/* used for monitoring events from knowledge bank */
		//this.addActionListener(new KBIndexPublishListener(instance));
		//this.addActionListener(new KBIndexSearchListener(instance));
		//this.addActionListener(new KBIndexQueryListener(instance));
		//this.addActionListener(new KBResultListener(instance));
		
		/* used for monitoring super peer's events */
		
		/* join and insert data objects */
		/*
		 * SPInsertListner and SPInsertBundleListener are replaced
		 * by SPIndexInsertListener
		 * $Id: ServerEventDispatcher.java,v 1.10 2008/10/09 09:55:18 wusai Exp $
		 *
		 */
		this.addActionListener(new SPInsertListener(instance));
//		this.addActionListener(new SPInsertBundleListener(instance));
		 /*
		//this.addActionListener(new SPIndexInsertBundleListener(instance));
		//this.addActionListener(new SPIndexInsertListener(instance));
		/* end of modification
		 * $Id: ServerEventDispatcher.java,v 1.10 2008/10/09 09:55:18 wusai Exp $
		 */
		
		this.addActionListener(new SPJoinAcceptListener(instance));
		this.addActionListener(new SPJoinListener(instance));
		this.addActionListener(new SPJoinForceListener(instance));
		this.addActionListener(new SPJoinForceForwardListener(instance));
		this.addActionListener(new SPJoinSplitDataListener(instance));
		
		/* load balance */
		this.addActionListener(new SPNotifyImbalanceListener(instance));
		this.addActionListener(new SPLBFindLightlyNodeListener(instance));
		this.addActionListener(new SPLBGetLoadInfoListener(instance));
		this.addActionListener(new SPLBGetLoadInfoReplyListener(instance));
		this.addActionListener(new SPLBGetLoadInfoResendListener(instance));
		this.addActionListener(new SPLBNoRotationNodeListener(instance));
		this.addActionListener(new SPLBRotateUpdateAdjacentListener(instance));
		this.addActionListener(new SPLBRotateUpdateAdjacentReplyListener(instance));
		this.addActionListener(new SPLBRotateUpdateChildListener(instance));
		this.addActionListener(new SPLBRotateUpdateChildReplyListener(instance));
		this.addActionListener(new SPLBRotateUpdateParentListener(instance));
		this.addActionListener(new SPLBRotateUpdateParentReplyListener(instance));
		this.addActionListener(new SPLBRotateUpdateRTListener(instance));
		this.addActionListener(new SPLBRotateUpdateRTReplyListener(instance));
		this.addActionListener(new SPLBSplitDataListener(instance));
		this.addActionListener(new SPLBSplitDataResendListener(instance));
		this.addActionListener(new SPLBStablePositionListener(instance));
		this.addActionListener(new SPLBRotationPullListener(instance));
		
		/* leave the network */
		this.addActionListener(new SPLeaveListener(instance));
		this.addActionListener(new SPLeaveUrgentListener(instance));
		this.addActionListener(new SPLeaveNotifyListener(instance));
		this.addActionListener(new SPLeaveFindReplaceListener(instance));
		this.addActionListener(new SPLeaveFindReplaceReplyListener(instance));
		this.addActionListener(new SPLeaveReplacementListener(instance));
		this.addActionListener(new SPPassClientListener(instance));
		
		/* update links with other super peers */
		this.addActionListener(new SPUpdateAdjacentLinkListener(instance));
		this.addActionListener(new SPUpdateMinMaxValueListener(instance));
		this.addActionListener(new SPUpdateRouteTableListener(instance));
		this.addActionListener(new SPUpdateRouteTableDirectlyListener(instance));
		this.addActionListener(new SPUpdateRouteTableIndirectlyListener(instance));
		this.addActionListener(new SPUpdateRouteTableReplyListener(instance));
		
		/* delete and search data objects*/
		/*
		 * SPDeleteListener and SPDeleteBundleListener
		 * are replaced by SPIndexDeleteListener.
		 * $Id 2007-2-2 14:23 author: xulinhao$
		this.addActionListener(new SPDeleteListener(instance));
		this.addActionListener(new SPDeleteBundleListener(instance));
		 */
		//this.addActionListener(new SPIndexDeleteBundleListener(instance));
		//this.addActionListener(new SPIndexDeleteListener(instance));
		//this.addActionListener(new SPIndexUpdateBundleListener(instance));
		//this.addActionListener(new SPIndexUpdateListener(instance));
		/*
		 * end of modification
		 * $Id 2007-2-2 14:23 author: xulinhao$
		 */

		this.addActionListener(new SPSearchExactListener(instance));
		this.addActionListener(new SPSearchExactResultListener(instance));

		// TODO: 自我添加的Listener
		this.addActionListener(new SPPublishListener(instance));
		this.addActionListener(new SPPublishParentListener(instance));
		this.addActionListener(new SPUpdateTagListener(instance));
		this.addActionListener(new SPUpdateSubtreeRangeListener(instance));

		this.addActionListener(new SPParallelSearchListener(instance));
		this.addActionListener(new SPSearchParentListener(instance));
		this.addActionListener(new SPFindParentListener(instance));
		this.addActionListener(new SPParallelSearchResultListener(instance));

		/*
		 * SPSearchExactListener, SPSearchExactBundleListener,
		 * SPSearchExactResultListener, SPSearchRangeListener,
		 * and SPSearchRangeResultListener are replaced by 
		 * SPIndexSearchListener.
		 * $Id 2007-2-1 11:10 author: xulinhao$
		this.addActionListener(new SPSearchExactListener(instance));
		this.addActionListener(new SPSearchExactBundleListener(instance));
		this.addActionListener(new SPSearchExactResultListener(instance));
		
		this.addActionListener(new SPSearchRangeListener(instance));
		this.addActionListener(new SPSearchRangeResultListener(instance));
		 */
		//this.addActionListener(new SPIndexSearchListener(instance));
		/* end of modification
		 * $Id 2007-2-1 11:10 author: xulinhao$
		 */
		
		/* failure recovery*/
		this.addActionListener(new SPLIAdjacentListener(instance));
		this.addActionListener(new SPLIAdjacentRootListener(instance));
		this.addActionListener(new SPLIAdjacentReplyListener(instance));
		this.addActionListener(new SPLIAdjacentRootReplyListener(instance));
		this.addActionListener(new SPLIRoutingTableListener(instance));
		this.addActionListener(new SPLIRoutingTableReplyListener(instance));
		this.addActionListener(new SPLIChildReplyListener(instance));
		this.addActionListener(new SPNotifyFailureListener(instance));
		this.addActionListener(new SPLIUpdateParentListener(instance));
		
		
	}	
}