package edu.buaa.act.jcsindex.global;

import edu.buaa.act.jcsindex.global.peer.PeerType;
import edu.buaa.act.jcsindex.global.peer.ServerPeer;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;

/**
 * Created by shmin at 2018/3/29 17:04
 **/
public class ServerInstance extends AbstractInstance{
    private static final long serialVersionUID = -8997906615159674963L;


    /**
     * Define an obscure port to test whether an instance has existed.
     */
    public static final int RUN_PORT = 60020;

    /**
     * Define a super peer manager, who is responsible for providing
     * all operations related to the super peer. Through this class,
     * the minimal cohensions are expected between GUI and non-GUI services.
     */
    protected ServerPeer serverpeer;

    static {
        ServerPeer.load();
    }

    private static boolean isSingleton()
    {
        if (!ServerPeer.DEBUG)
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
        }
        return true;
    }

    public ServerInstance(int port) {
        serverpeer = new ServerPeer(this, port, PeerType.SUPERPEER.getValue());
    }

    public static boolean tryStartService()
    {
        try
        {
            ServerSocket socket = new ServerSocket(ServerPeer.LOCAL_SERVER_PORT, ServerPeer.CAPACITY);
            socket.close();
            return true;
        }
        catch (BindException e)
        {
            return false;
        }
        catch (IOException e)
        {
            return false;
        }
    }

    public boolean startService()
    {
        if (serverpeer.startEventManager(ServerPeer.LOCAL_SERVER_PORT, ServerPeer.CAPACITY)
                && serverpeer.startUDPServer(ServerPeer.LOCAL_SERVER_PORT, ServerPeer.CAPACITY, NORM_FREQ)
                )
        {
            return true;
        }
        return false;
    }

    public boolean startService(int port)
    {
        if (serverpeer.startEventManager(port, ServerPeer.CAPACITY))
        {
            return true;
        }
        return false;
    }

    public boolean stopService()
    {
        if (serverpeer.stopEventManager() && serverpeer.stopUDPServer())
        {
            return true;
        }
        return false;
    }

    public void scheduleUDPSender(long period)
    {
        serverpeer.scheduleUDPSender(period);
    }

    @Override
    public ServerPeer peer()
    {
        return this.serverpeer;
    }

    @Override
    public void logout(boolean toBoot, boolean toServer, boolean toClient) {
        System.out.println("Hello World");
    }


    public static void main(String[] args) {
        int port = Integer.parseInt(args[0]);
        ServerInstance serverInstance = new ServerInstance(port);
        serverInstance.startService(port);
        new Thread(new CmdServer(serverInstance.peer())).start();
    }
}
