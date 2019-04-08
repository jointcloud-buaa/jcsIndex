package bplus.fudger;

import bplus.bptree.BPlusConfiguration;
import bplus.bptree.BPlusTree;
import bplus.bptree.BPlusTreePerformanceCounter;
import bplus.util.InvalidBTreeStateException;
import bplus.util.TestRunner;

import java.io.IOException;

public class Main {

    public static void main(String[] args)
            throws IOException, InvalidBTreeStateException {
        boolean fastTrials = true;
        boolean recreateTree = true;
        BPlusConfiguration btconf = new BPlusConfiguration();
        BPlusTreePerformanceCounter bPerf = new BPlusTreePerformanceCounter(true);
        BPlusTree bt = new BPlusTree(btconf, recreateTree ? "rw+" : "rw", bPerf);

        //int tlen = 20000;
        //long skey = 0;
        //long eKey = tlen;
        //String val = "1234567890";
        //boolean unique = true;
        bt.printCurrentConfiguration();
//        if(recreateTree) {
//            Utilities.sequentialAddToTree(skey, eKey,
//                    val, unique, bt);
//            bPerf.printTotalStatistics();
//        }
// TODO：暂时注释
//        if(fastTrials)
//            {TestRunner.runDefaultTrialsFast(bPerf);}
//        else
//            {TestRunner.runBench(bPerf);}

        System.out.println("\n -- Total pages in the end: " + bt.getTotalTreePages());
        // finally close it.
        bt.commitTree();

    }

}
