package edu.buaa.act.jcsindex.local.jcssti.index.node;

import bplus.bptree.BPlusTree;
import bplus.bptree.RangePosition;

import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by shmin at 2018/10/24 23:40
 **/
public class IndexSummary implements Runnable{
    private BPlusTree[] indexs;
    private Map<Integer, TreeSet<RangePosition>> summary;
    private LinkedBlockingQueue<RangePosition> queue;
    private volatile boolean isRunning = true;
    public IndexSummary(BPlusTree[] indexs) {
        this.indexs = indexs;
        this.queue = new LinkedBlockingQueue<>();
    }

    public void initialize() {
        // 初始化
        for (int i = 0; i < indexs.length; i++) {
            List<RangePosition> rps = indexs[i].expandRange(new RangePosition());
            TreeSet<RangePosition> treeSet = new TreeSet<>();
            for (RangePosition rp : rps) {
                treeSet.add(rp);
            }
        }
        // 发布到全局索引中
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
