package edu.buaa.act.jcsindex.global;

import edu.buaa.act.jcsindex.global.peer.ServerPeer;
import edu.buaa.act.jcsindex.global.peer.info.*;
import edu.buaa.act.jcsindex.global.protocol.Head;
import edu.buaa.act.jcsindex.global.protocol.Message;
import edu.buaa.act.jcsindex.global.protocol.MsgType;
import edu.buaa.act.jcsindex.global.protocol.body.*;

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
                .append("(2) To leave a Baton Network, using command like `leave`\n")
                .append("(3) To view peer information, using command like `info`\n")
                .append("(4) To view local storage data, using command like `storage [n]`\n")
                .append("(5) To insert an item into Baton Network, using command like `insert key value`\n")
                .append("(6) To insert an jcstuple into Baton Network, using command like `inserttuple timeIndex key value`\n")
                .append("(7) To query an item in the Baton Network, using command like `query key`\n")
                .append("(8) To query an jcstuple in the Baton Network, using command like `pquery timeIndex leftbound rightbound`\n")
                .append("(9) To test insert and storage, using command like `test key value`")
                .toString();
        final String JOIN_CMD = "join";
        final String LEAVE_CMD = "leave";
        final String HELP_CMD = "help";
        final String INFO_CMD = "info";
        final String INSERT_CMD = "insert";
        final String INSERTTUPLE_CMD = "inserttuple";
        final String STORAGE_CMD = "storage";
        final String QUERY_CMD = "query";
        final String PQUERY_CMD = "pquery";
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
                    } else if (args[0].equals(LEAVE_CMD)) {
                        try {
                            Socket socket = new Socket(serverPeer.getPhysicalInfo().getIP(), serverPeer.getPhysicalInfo().getPort());

                            Head head = new Head();
                            head.setMsgType(MsgType.SP_LEAVE_URGENT.getValue());

                            Body body = new SPLeaveUrgentBody(
                                    serverPeer.getPhysicalInfo(),
                                    serverPeer.getListItem(0).getLogicalInfo(),
                                    serverPeer.getListItem(0),
                                    null
                            );
                            Message message = new Message(head, body);

                            ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                            oos.writeObject(message);

                            socket.close();
                            System.out.println("退出完成");
                        } catch (Exception e) {
                            e.printStackTrace();
                            System.err.println("Unknown Host Exception");
                        }
                    } else if (args[0].equals(INFO_CMD)) {
                        // 查询该结点的相关信息
                        TreeNode treeNode = serverPeer.getListItem(0);
                        System.out.println(treeNode.formatedInfo());
                    } else if (args[0].equals(STORAGE_CMD)) {
                        // 查询该结点存储的键值对
                        TreeNode treeNode = serverPeer.getListItem(0);
                        if (args.length == 2) {
                            int ind = Integer.parseInt(args[1]);
                            if (ind == 1) {
                                System.out.println(treeNode.getContent().formatedString());
                            } else {
                                System.out.println(treeNode.getContent().formatTupleString());
                            }
                        } else {
                            System.out.println("Wrong usage: storage 1 or storage 2");
                        }
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
                        if (args.length == 3) {
                            String key = args[1], value = args[2];
                            try {
                                Socket socket = new Socket(serverPeer.getPhysicalInfo().getIP(), serverPeer.getPhysicalInfo().getPort());

                                Head head = new Head();
                                head.setMsgType(MsgType.SP_INSERT.getValue());

                                Body body = new SPInsertBody(
                                        serverPeer.getPhysicalInfo(),
                                        serverPeer.getListItem(0).getLogicalInfo(),
                                        new IndexValue(IndexValue.STRING_TYPE, key, new IndexInfo(value)),
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
                    } else if (args[0].equals(INSERTTUPLE_CMD)){
                        if (args.length == 5) {
                            int timeIndex = Integer.parseInt(args[1]);
                            long left = Long.parseLong(args[2]), right = Long.parseLong(args[3]);
                            String desc = args[4];
                            try {
                                Socket socket = new Socket(serverPeer.getPhysicalInfo().getIP(), serverPeer.getPhysicalInfo().getPort());

                                Head head = new Head();
                                head.setMsgType(MsgType.SP_PUBLISH.getValue());

                                Body body = new SPPublishBody(
                                        serverPeer.getPhysicalInfo(),
                                        serverPeer.getListItem(0).getLogicalInfo(),
                                        new JcsTuple(timeIndex, left, right, desc),
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
                        if (args.length == 2) {
                            String key = args[1];

                            try {
                                Socket socket = new Socket(serverPeer.getPhysicalInfo().getIP(), serverPeer.getPhysicalInfo().getPort());

                                Head head = new Head();
                                head.setMsgType(MsgType.SP_SEARCH_EXACT.getValue());

                                Body body = new SPSearchExactBody(
                                        serverPeer.getPhysicalInfo(),
                                        serverPeer.getListItem(0).getLogicalInfo(),
                                        serverPeer.getPhysicalInfo(),
                                        serverPeer.getListItem(0).getLogicalInfo(),
                                        new IndexValue(IndexValue.STRING_TYPE, key, new IndexInfo("NULL")),
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
                    } else if (args[0].equals(PQUERY_CMD)) {
                        // 查询数据
                        if (args.length == 4) {
                            int timeIndex = Integer.parseInt(args[1]);
                            long left = Long.parseLong(args[2]), right = Long.parseLong(args[3]);

                            try {
                                Socket socket = new Socket(serverPeer.getPhysicalInfo().getIP(), serverPeer.getPhysicalInfo().getPort());

                                Head head = new Head();
                                head.setMsgType(MsgType.SP_PARALLEL_SEARCH.getValue());

                                Body body = new SPParallelSearchBody(
                                        serverPeer.getPhysicalInfo(),
                                        serverPeer.getListItem(0).getLogicalInfo(),
                                        serverPeer.getPhysicalInfo(),
                                        serverPeer.getListItem(0).getLogicalInfo(),
                                        new JcsTuple(timeIndex, left, right, ""),
                                        null
                                );

                                Message message = new Message(head, body);

                                ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                                oos.writeObject(message);

                                socket.close();
                                System.out.println("jcstuple查询输入完成");
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        } else {
                            System.out.println("FORMAT: pquery timeIndex leftbound rightbound");
                        }
                    }
                    else if (args[0].equals(HELP_CMD)) {
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
