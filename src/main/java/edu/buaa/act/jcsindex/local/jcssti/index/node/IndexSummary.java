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

    public IndexSummary(BPlusTree[] indexs, String ip) {
        this.indexs = indexs;
        this.ip = ip;
        this.queue = new LinkedBlockingQueue<>();
    }

    public void initialize() {
        // 初始化
        int count = 0;
        for (int i = 0; i < indexs.length; i++) {
            List<RangePosition> rps = indexs[i].expandRange(new RangePosition());
            TreeSet<RangePosition> treeSet = new TreeSet<>();
            for (RangePosition rp : rps) {
                rp.min = indexs[i].getMinRange(rp);
                rp.max = indexs[i].getMaxRange(rp);
                treeSet.add(rp);
                realPublish(i, rp);
                // System.out.println("(" + rp.left + ", " + rp.right + ") " + "(" + rp.min + ", " + rp.max + ")");
                count++;
            }
            summary.put(i, treeSet);
            System.out.println("Every timeIndex: " + rps.size());
        }
        System.out.println("all count: " + count);
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
        if (summary.containsKey(index)) {
            TreeSet<RangePosition> treeSet = summary.get(index);
            RangePosition item = treeSet.floor(new RangePosition(value));
            if (item == null) return false;
            else if (item.right > value) return true;
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
}
