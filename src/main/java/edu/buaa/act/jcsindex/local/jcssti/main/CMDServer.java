package edu.buaa.act.jcsindex.local.jcssti.main;

import edu.buaa.act.jcsindex.local.jcssti.index.node.DataNodeImpl;
import edu.buaa.act.jcsindex.local.bean.ParaGPSRecord;

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Scanner;

/**
 * Created by shmin at 2018/7/19 16:02
 **/
public class CMDServer implements Runnable{
    private DataNodeImpl dataNode;
    private boolean isRunning;

    public CMDServer(DataNodeImpl dataNode) {
        this.dataNode = dataNode;
        this.isRunning = true;
    }

    @Override
    public void run() {
        final String HELP_MSG = new StringBuilder().append("===== COMMAND SERVER =====\n")
                .append("Usage: \n")
                .append("1) query: query time lx ly rx ry\n")
                .append("2) insert: insert data into B-plus-tree\n")
                .append("3) quit: quit the CMD Server\n")
                .append("4) test: test time\n")
                .append("5) range: range time\n")
                .append("6) memory: check memory usage\n")
                .append("7) help: print `HELP` message")
                .toString();

        final String INSERT_CMD = "insert";
        final String HELP_CMD = "help";
        final String QUERY_CMD = "query";
        final String QUIT_CMD = "quit";
        final String TEST_CMD = "test";
        final String RANGE_CMD = "range";
        final String MEMORY_CMD = "memory";

        Scanner sc = new Scanner(System.in);
        String[] args = null;
        String cmdStr = null;

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        System.out.println(HELP_MSG);
        while (isRunning) {
            try {
                cmdStr = sc.nextLine().trim();
                if (cmdStr != null && cmdStr.length() > 0) {
                    args = cmdStr.split("\\s+");
                    if (args[0].equals(INSERT_CMD)) {
                        dataNode.insert();
                    } else if (args[0].equals(QUERY_CMD)) {
                        // TODO: 添加真正的查询逻辑
                        System.out.println("Query done.");
                    } else if (args[0].equals(TEST_CMD)) {
                        long start = System.currentTimeMillis();
                        int ind = 0;
                        if (args.length == 2) {
                            ind = Integer.parseInt(args[1]);
                        }
                        List<ParaGPSRecord> res = dataNode.rangeQuery(ind);
                        long end = System.currentTimeMillis();
                        System.out.println("查询时间为：" + (end - start) + ", 返回数据大小为: " + res.size());
                    } else if (args[0].equals(MEMORY_CMD)) {
                        Runtime runtime = Runtime.getRuntime();
                        long memoryUsed = runtime.totalMemory() - runtime.freeMemory();
                        System.out.printf("in CmdServer,curTime:[%s], memory used:[%dB ≈ %fKB ≈ %fMB]\n",
                                sdf.format(System.currentTimeMillis()), memoryUsed, (memoryUsed / 1024.0),
                                memoryUsed / (1024 * 1024.0));
                    } else if (args[0].equals(RANGE_CMD)) {
                        // TODO: 待添加真正的逻辑
                    } else if (args[0].equals(QUIT_CMD)) {
                        dataNode.exit();
                        isRunning = false;
                    } else if (args[0].equals(HELP_CMD)) {
                        System.out.println(HELP_MSG);
                    } else {
                        System.out.println("Unsupported Command!");
                        System.out.println(HELP_MSG);
                    }
                } else {
                    System.out.println("Wrong Command. Input again!");
                    System.out.println(HELP_MSG);
                }
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
