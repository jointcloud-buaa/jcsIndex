/*
 * @(#) BootstrapEventDispatcher.java 1.0 2006-2-3
 * 
 * Copyright 2006, National University of Singapore.
 * All rights reserved.
 */

package edu.buaa.act.jcsindex.global.peer.management;

//import buaa.act.jointcloudstorage.accesscontrol.bootstrap.AccessControlRoleUpdateListener;
//import buaa.act.jointcloudstorage.accesscontrol.bootstrap.CreateUserFromServerPeerListener;

import edu.buaa.act.jcsindex.global.AbstractInstance;
import edu.buaa.act.jcsindex.global.peer.event.JoinListener;

/**
 * Implement a concrete event manager used for monitoring
 * all incoming socket connections related to the bootstrap server.
 * 
 * @author Xu Linhao
 * @version 1.0 2006-2-3
 */

public class BootstrapEventDispatcher extends AbstractEventDispatcher
{

	public BootstrapEventDispatcher(AbstractInstance instance)
	{
		super(instance);
	}

	@Override
	public void registerActionListeners() 
	{
		this.addActionListener(new JoinListener(instance));
//		this.addActionListener(new LeaveListener(instance));
		
		//VHTam: add following code to the next //end VHTam
		//add access control listener
		//this.addActionListener(new AccessControlRoleUpdateListener(instance));
		//this.addActionListener(new CreateUserFromServerPeerListener(instance));
		//end VHTam

	}
	
}