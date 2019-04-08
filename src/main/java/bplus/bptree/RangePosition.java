package bplus.bptree;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by shmin at 2018/6/22 0:50
 **/
public class RangePosition implements Comparable<RangePosition> {
    public int level;
    public List<Integer> indexs;
    public long left, right;
    private int all, hit;

    public RangePosition() {
        this.level = 0;
        this.left = 0L;
        this.right = Long.MAX_VALUE;
        this.indexs = new ArrayList<>();
        this.all = this.hit = 0;
    }

    public RangePosition(int level) {
        this.level = level;
        this.indexs = new ArrayList<>();
    }

    public RangePosition(int level, List<Integer> indexs, long left, long right) {
        this.level = level;
        this.indexs = indexs;
        this.left = left;
        this.right = right;
    }

    public RangePosition(long left) {
        this.left = left;
    }

    @Override
    public int compareTo(RangePosition that) {
        return (int )(this.left - that.left);
    }

    public void increaseAll() {
        this.all++;
    }

    public void increaseHit() {
        this.hit++;
    }
}
