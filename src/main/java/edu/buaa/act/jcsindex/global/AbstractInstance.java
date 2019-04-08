package edu.buaa.act.jcsindex.global;

import edu.buaa.act.jcsindex.global.peer.AbstractPeer;

import java.net.ServerSocket;

/**
 * Created by shmin at 2018/3/29 16:50
 *
 * 虚拟实例，不管是Bootstrap peer或者Server peer最终都会是一个实例
 **/
abstract public class AbstractInstance {
    protected static ServerSocket SERVER_SOCKET;

    /** define the high frequency of sending UDP messages to remote peers */
    public final static long HIGH_FREQ = 60000;		// 60 seconds

    /** define the normal frequency of sending UDP messages to remote peers */
    public final static long NORM_FREQ = 90000;		// 90 seconds

    /** define the low frequency of sending UDP messages to remote peers */
    public final static long LOW_FREQ  = 120000;	// 120 seconds

    public abstract AbstractPeer peer();

    public abstract void scheduleUDPSender(long period);

    public abstract void logout(boolean toBoot, boolean toServer, boolean toClient);
}
