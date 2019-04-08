package edu.buaa.act.jcsindex.global;

import edu.buaa.act.jcsindex.global.peer.AbstractPeer;
import edu.buaa.act.jcsindex.global.peer.Bootstrap;
import edu.buaa.act.jcsindex.global.peer.PeerType;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;

/**
 * Created by shmin at 2018/3/29 17:00
 **/
public class BootstrapInstance extends AbstractInstance {
    public static final int RUN_PORT = 60000;
    /** define the high frequency of sending UDP messages to remote peers */
    public final static long HIGH_FREQ = 60000;		// 60 seconds

    /** define the normal frequency of sending UDP messages to remote peers */
    public final static long NORM_FREQ = 90000;		// 90 seconds

    /** define the low frequency of sending UDP messages to remote peers */
    public final static long LOW_FREQ  = 120000;	// 120 seconds

    private Bootstrap bootstrap;

    static {
        Bootstrap.load();
    }

    private static boolean isSingleton()
    {
        try
        {
            SERVER_SOCKET = new ServerSocket(RUN_PORT);
        }
        catch (BindException e)
        {
            return false;
        }
        catch (IOException e)
        {
            return false;
        }
        return true;
    }

    public BootstrapInstance() {
        bootstrap = new Bootstrap(this, PeerType.BOOTSTRAP.getValue());
    }

    @Override
    public  AbstractPeer peer() {
        return bootstrap;
    }

    public boolean startServer()
    {
        if (bootstrap.startEventManager(Bootstrap.LOCAL_SERVER_PORT, Bootstrap.CAPACITY)
                && bootstrap.startUDPServer(Bootstrap.LOCAL_SERVER_PORT, Bootstrap.CAPACITY, BootstrapInstance.NORM_FREQ))
        {

            return true;
        }
        return false;
    }


    public boolean stopServer()
    {
        if (bootstrap.stopEventManager() && bootstrap.stopUDPServer())
        {
            System.out.println("关闭");
            return true;
        }
        return false;
    }

    public void scheduleUDPSender(long period)
    {
        bootstrap.scheduleUDPSender(period);
    }

    @Override
    public void logout(boolean toBoot, boolean toServer, boolean toClient) {
        System.out.println("Hello World");
    }
}
