package edu.buaa.act.jcsindex.global;

import edu.buaa.act.jcsindex.global.peer.ServerPeer;
import edu.buaa.act.jcsindex.global.peer.info.ContentInfo;
import edu.buaa.act.jcsindex.global.peer.info.IndexInfo;
import edu.buaa.act.jcsindex.global.peer.info.IndexValue;
import edu.buaa.act.jcsindex.global.peer.info.TreeNode;
import edu.buaa.act.jcsindex.global.protocol.Head;
import edu.buaa.act.jcsindex.global.protocol.Message;
import edu.buaa.act.jcsindex.global.protocol.MsgType;
import edu.buaa.act.jcsindex.global.protocol.body.Body;
import edu.buaa.act.jcsindex.global.protocol.body.SPInsertBody;
import edu.buaa.act.jcsindex.global.protocol.body.SPSearchExactBody;

import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Scanner;

/**
 * Created by shmin at 2018/3/30 0:01
 **/
public class CmdServer implements Runnable {
    private ServerPeer serverPeer;

    public CmdServer(ServerPeer serverPeer) {
        this.serverPeer = serverPeer;
    }

    @Override
    public void run() {
        // TODO: 目前只需要完成最基礎的功能
        final String HELP_MSG = new StringBuilder().append("COMMAND SERVER\n")
                .append("Usage: \n")
                .append("(1) To join into a Baton Network, using command like `join ip port`\n")
                .append("(2) To view peer information, using command like `info`\n")
                .append("(3) To view local storage data, using command like `storage`\n")
                .append("(4) To insert an item into Baton Network, using command like `insert timeIndex key value`\n")
                .append("(5) To query an item in the Baton Network, using command like `query timeIndex key`\n")
                .append("(6) To test insert and storage, using command like `test key value`")
                .toString();
        final String JOIN_CMD = "join";
        final String HELP_CMD = "help";
        final String INFO_CMD = "info";
        final String INSERT_CMD = "insert";
        final String STORAGE_CMD = "storage";
        final String QUERY_CMD = "query";
        final String TEST_CMD = "test";

        Scanner sc = new Scanner(System.in);
        String[] args = null;
        String cmdStr = null;

        System.out.println(HELP_MSG);
        while (true) {
            try {
                cmdStr = sc.nextLine().trim();
                if (cmdStr != null && cmdStr.length() > 0) {
                    args = cmdStr.split("\\s+");
                    if (args[0].equals(JOIN_CMD)) {
                        // 加入Baton Network
                        if (args.length == 3) {
                            String ip = args[1];
                            int port = Integer.parseInt(args[2]);
                            if (serverPeer.performJoinRequest(ip, port)) {
                                System.out.println("连接成功");
                            } else {
                                System.out.println("连接失败");
                            }
                        } else {
                            System.out.println("FORMAT: join ip port");
                        }
                    } else if (args[0].equals(INFO_CMD)) {
                        // 查询该结点的相关信息
                        TreeNode treeNode = serverPeer.getListItem(0);
                        System.out.println(treeNode.formatedInfo());
                    } else if (args[0].equals(STORAGE_CMD)) {
                        // 查询该结点存储的键值对
                        TreeNode treeNode = serverPeer.getListItem(0);
                        System.out.println(treeNode.getContent().formatedString());
                    } else if (args[0].equals(TEST_CMD)){
                        // 用作测试
                        if (args.length == 3) {
                            String key = args[1], value = args[2];
                            TreeNode treeNode = serverPeer.getListItem(0);
                            ContentInfo contentInfo = treeNode.getContent();
                            contentInfo.insertData(new IndexValue(IndexValue.STRING_TYPE, key, new IndexInfo(value)), ContentInfo.INSERT_NORMALLY);
                            System.out.println("插入数据成功");
                        } else {
                            System.out.println("FORMAT: test key value");
                        }
                    } else if (args[0].equals(INSERT_CMD)) {
                        // 插入数据
                        if (args.length == 4) {
                            int timeIndex = Integer.parseInt(args[1]);
                            String key = args[2], value = args[3];
                            try {
                                Socket socket = new Socket(serverPeer.getPhysicalInfo().getIP(), serverPeer.getPhysicalInfo().getPort());

                                Head head = new Head();
                                head.setMsgType(MsgType.SP_INSERT.getValue());

                                Body body = new SPInsertBody(
                                        serverPeer.getPhysicalInfo(),
                                        serverPeer.getListItem(0).getLogicalInfo(),
                                        new IndexValue(IndexValue.STRING_TYPE, timeIndex, key, new IndexInfo(value)),
                                        null
                                );
                                Message message = new Message(head, body);

                                ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                                oos.writeObject(message);

                                socket.close();
                                System.out.println("插入数据完成");
                            } catch (Exception e) {
                                e.printStackTrace();
                                System.err.println("Unknown Host Exception");
                            }
                        } else {
                            System.out.println("FORMAT: insert key value");
                        }
                    } else if (args[0].equals(QUERY_CMD)) {
                        // 查询数据
                        if (args.length == 3) {
                            int timeIndex = Integer.parseInt(args[1]);
                            String key = args[2];

                            try {
                                Socket socket = new Socket(serverPeer.getPhysicalInfo().getIP(), serverPeer.getPhysicalInfo().getPort());

                                Head head = new Head();
                                head.setMsgType(MsgType.SP_SEARCH_EXACT.getValue());

                                Body body = new SPSearchExactBody(
                                        serverPeer.getPhysicalInfo(),
                                        serverPeer.getListItem(0).getLogicalInfo(),
                                        serverPeer.getPhysicalInfo(),
                                        serverPeer.getListItem(0).getLogicalInfo(),
                                        new IndexValue(IndexValue.STRING_TYPE, timeIndex, key, new IndexInfo("NULL")),
                                        null
                                );

                                Message message = new Message(head, body);

                                ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                                oos.writeObject(message);

                                socket.close();
                                System.out.println("查询输入完成");
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        } else {
                            System.out.println("FORMAT: query key");
                        }
                    } else if (args[0].equals(HELP_CMD)) {
                        System.out.println(HELP_MSG);
                    } else {
                        System.out.println("UNKNOWN COMMANd");
                        System.out.println(HELP_MSG);
                    }
                } else {
                    System.out.println(HELP_MSG);
                }
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // TODO: Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
}
