package edu.buaa.act.jcsindex;

import edu.buaa.act.jcsindex.global.CmdServer;
import edu.buaa.act.jcsindex.global.ServerInstance;
import edu.buaa.act.jcsindex.global.proto.BroadcastServer;
import edu.buaa.act.jcsindex.local.jcssti.main.LocalInstance;

/**
 * Created by shimin at 4/19/2019 4:06 PM
 **/
public class MainInstance {
    public static void main(String[] args) {
        // 启动类，通过该类可以启动GlobalIndex或者LocalIndex
        // 写成一个类主要目的是方便打包成jar并启动
        // 如果启动一个GlobalIndex，则需要注意的是，第一个启动的节点不需要加入任何网络，后续的节点则需要加入Baton网络，需要提供目的节点的ip和端口
        // 如果启动一个LocalIndex, 目前至少需要提供的参数是HBase的地址
        if (args.length == 0) {
            System.out.println("Please input your startup args: global | local");
            return;
        }
        String[] startupType = new String[] {"global", "local"};
        if ((args.length >= 1) && args[0].equals(startupType[0])) {
            // 启动GlobalIndex
            System.out.println("start global index");
            // TODO: 默认端口为40000，没有必要修改
            int port = 40000;
            ServerInstance serverInstance = new ServerInstance(port);
            serverInstance.startService(port);
            new Thread(new BroadcastServer(serverInstance.peer())).start();
            new Thread(new CmdServer(serverInstance.peer())).start();
        } else if ((args.length >= 1) && args[0].equals(startupType[1])) {
            // 启动LocalIndex
            System.out.println("start local index");
            int type = Integer.parseInt(args[1]);
            String serverName = args[2];
            try {
                new LocalInstance().start(type, serverName);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            System.out.println("wrong param");
        }
    }
}
