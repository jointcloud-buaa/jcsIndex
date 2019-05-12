package edu.buaa.act.jcsindex.local.jcssti.index.node;

import bplus.bptree.BPlusTree;
import bplus.bptree.RangePosition;
import edu.buaa.act.jcsindex.global.peer.info.JcsTuple;
import edu.buaa.act.jcsindex.global.peer.info.PhysicalInfo;
import edu.buaa.act.jcsindex.global.protocol.Head;
import edu.buaa.act.jcsindex.global.protocol.Message;
import edu.buaa.act.jcsindex.global.protocol.MsgType;
import edu.buaa.act.jcsindex.global.protocol.body.Body;
import edu.buaa.act.jcsindex.global.protocol.body.SPPublishBody;

import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by shimin at 2018/10/24 23:40
 **/
public class IndexSummary implements Runnable{
    private BPlusTree[] indexs;
    private String ip;
    private Map<Integer, TreeSet<RangePosition>> summary;
    private LinkedBlockingQueue<RangePosition> queue;
    private volatile boolean isRunning = true;
    private boolean isInitialize = false;
    private boolean isExpand = false;

    public IndexSummary(BPlusTree[] indexs, String ip) {
        this.indexs = indexs;
        this.ip = ip;
        this.queue = new LinkedBlockingQueue<>();
        this.summary = new HashMap<>();
    }

    public void initialize() {
        // 初始化
        isInitialize = true;
        int count = 0;
        int maxsize = 0;
        // 只拓展第1个月，其它月份不拓展
        // 主要是考虑到拓展一个月的JcsTuple数目为23万，拓展时间为16分钟，综合考虑一次发布索引需要20分钟
        // 因此15台机器则需要5个小时才能完成一次发布
        for (int i = 0; i < indexs.length; i++) {
            // 更改为拓展到第三层，检查有多少tuple，如果tuple很多，那么我可以选择只拓展1个月的
            // 因此使得type = 1
            List<RangePosition> rps = indexs[i].expandRange(new RangePosition(), 0);
            TreeSet<RangePosition> treeSet = new TreeSet<>();
            for (RangePosition rp : rps) {
                if (isExpand && i < 744) {
                    List<RangePosition> rps1 = indexs[i].expandRange(rp, 0);
                    for (RangePosition rp2 : rps1) {
                        rp2.min = indexs[i].getMinRange(rp2);
                        rp2.max = indexs[i].getMaxRange(rp2);
                        treeSet.add(rp2);
                        count++;
                    }
                } else {
                    rp.min = indexs[i].getMinRange(rp);
                    rp.max = indexs[i].getMaxRange(rp);
                    treeSet.add(rp);
                    count++;
                }
            }
            summary.put(i, treeSet);
            maxsize = Math.max(indexs[i].getRootNodeSize(), maxsize);

//            List<RangePosition> rps = indexs[i].expandRange(new RangePosition(), 0);
//            TreeSet<RangePosition> treeSet = new TreeSet<>();
//            for (RangePosition rp : rps) {
//                rp.min = indexs[i].getMinRange(rp);
//                rp.max = indexs[i].getMaxRange(rp);
//                treeSet.add(rp);
//                count++;
//            }
//            summary.put(i, treeSet);
//            maxsize = Math.max(indexs[i].getRootNodeSize(), maxsize);
//            System.out.println("Root node keyArray size: " + indexs[i].getRootNodeSize() + " Every timeIndex: " + rps.size());
        }
        System.out.println("all count: " + count + " Max root node size: " + maxsize);
    }

    public void initializeAndpublish() {
        isInitialize = true;
        int count = 0;
        for (int i = 0; i < indexs.length; i++) {
            List<RangePosition> rps = indexs[i].expandRange(new RangePosition(), 0);
            TreeSet<RangePosition> treeSet = new TreeSet<>();
            for (RangePosition rp : rps) {
                if (isExpand && i < 744) {
                    List<RangePosition> rps1 = indexs[i].expandRange(rp, 0);
                    for (RangePosition rp2 : rps1) {
                        rp2.min = indexs[i].getMinRange(rp2);
                        rp2.max = indexs[i].getMaxRange(rp2);
                        treeSet.add(rp2);
                        realPublish(i, rp2);
                        count++;
                    }
                } else {
                    rp.min = indexs[i].getMinRange(rp);
                    rp.max = indexs[i].getMaxRange(rp);
                    treeSet.add(rp);
                    realPublish(i, rp);
                    count++;
                }

//                rp.min = indexs[i].getMinRange(rp);
//                rp.max = indexs[i].getMaxRange(rp);
//                treeSet.add(rp);
//                realPublish(i, rp);
//                count++;
            }
            summary.put(i, treeSet);
        }
        System.out.println("all count: " + count);
    }

    public void publishRange() {
        for (Map.Entry<Integer, TreeSet<RangePosition>> entry : summary.entrySet()) {
            int timeIndex = entry.getKey();
            TreeSet<RangePosition> treeSet = entry.getValue();
            for (RangePosition rangePosition : treeSet) {
                realPublish(timeIndex, rangePosition);
            }
        }
    }

    public void realPublish(int timeIndex, RangePosition rp) {
        try {
            Socket socket = new Socket("127.0.0.1", 40000);

            Head head = new Head();
            head.setMsgType(MsgType.SP_PUBLISH.getValue());

            Body body = new SPPublishBody(
                    new PhysicalInfo(ip, 40000),
                    null,
                    new JcsTuple(timeIndex, rp.min, rp.max, ip),
                    null
            );
            Message message = new Message(head, body);

            ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
            oos.writeObject(message);

            socket.close();
        } catch (Exception e) {
            System.out.println("Publish Range failed");
        }
    }

    public boolean contains(int index, int value) {
        if (!isInitialize) {
            return true;
        }
        if (summary.containsKey(index)) {
            TreeSet<RangePosition> treeSet = summary.get(index);
            RangePosition searchRp = new RangePosition();
            searchRp.min = value;
            RangePosition item = treeSet.floor(searchRp);
            if (item == null) return false;
            else if (item.max >= value) return true;
            else return false;
        } else {
            return false;
        }
    }

    public void append(RangePosition rp) {
        this.queue.offer(rp);
    }

    public boolean publish() {
        return true;
    }

    @Override
    public void run() {
        while (isRunning) {
            try {
                Thread.sleep(50);
                RangePosition rp = this.queue.poll();
                // 拓展时先发布新的范围，再取消旧的范围。
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static boolean contains(TreeSet<RangePosition> treeSet, int value) {
        RangePosition key = new RangePosition();
        key.min = value;
        RangePosition item = treeSet.floor(key);
        if (item == null) {
            System.out.println();
            return false;
        }
        else if (item.max >= value) {
            return true;
        }
        else {
            return false;
        }
    }

    public static void main(String[] args) {
        TreeSet<RangePosition> treeSet = new TreeSet<>();
        RangePosition rp1 = new RangePosition();
        rp1.min = 13; rp1.max = 20;
        treeSet.add(rp1);
        RangePosition rp2 = new RangePosition();
        rp2.min = 23; rp2.max = 50;
        treeSet.add(rp2);
        for (RangePosition rp : treeSet) {
            System.out.println(rp.min + " " + rp.max);
        }
        System.out.println(contains(treeSet, 15));
        System.out.println(contains(treeSet, 20));
        System.out.println(contains(treeSet, 21));
        System.out.println(contains(treeSet, 23));
    }
}
