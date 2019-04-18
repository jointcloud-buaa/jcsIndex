package edu.buaa.act.jcsindex.global.proto;


import edu.buaa.act.jcsindex.global.peer.ServerPeer;
import edu.buaa.act.jcsindex.global.peer.info.TreeNode;
import io.grpc.ClientCall;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by shimin at 4/18/2019 8:16 PM
 **/
public class BroadcastServer implements Runnable{

    private static Logger log = Logger.getLogger(BroadcastServer.class);

    // TODO: 固定端口号，可以考虑修改为从配置中获取
    private int port = 50051;

    private Server server;

    private ServerPeer serverPeer;

    public BroadcastServer(ServerPeer serverPeer) {
        this.serverPeer = serverPeer;
    }

    private void start() throws IOException {
        server = ServerBuilder.forPort(port).addService(new BroadcastImpl()).build().start();
        log.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.err.println("*** shutting down gRPC server since JVM is shutdown");
                BroadcastServer.this.stop();
                System.err.println("*** server shutdown");
            }
        });
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    public void startServer() throws IOException, InterruptedException {
        start();
        blockUntilShutdown();
    }

    @Override
    public void run() {
        try {
            startServer();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private class BroadcastImpl extends BroadcastSearchGrpc.BroadcastSearchImplBase {
        @Override
        public void search(BroadcastProto.BroadRequest request, StreamObserver<BroadcastProto.BroadResponse> responseObserver) {
            TreeNode node = serverPeer.getListItem(0);
            Set<String> ans = new HashSet<>();
            int timeIndex = request.getTimeIndex();
            long leftBound = request.getLeftBound();
            long rightBound = request.getRightBound();
            ans.addAll(node.getContent().localSearch(timeIndex, leftBound, rightBound));
            // 如果不是叶子节点
            if (!(node.getLeftChild() == null && node.getRightChild() == null)) {
                BroadcastClient client = null;
                // 左边孩子的子树区间与之相交
                if (node.getLeftChild() != null && node.getContent().isLeftChildOverlap(leftBound, rightBound)) {
                    // 发送请求给下游
                    client = new BroadcastClient(node.getLeftChild().getPhysicalInfo().getIP());
                    ans.addAll(client.broadcastSearch(timeIndex, leftBound, rightBound));
                }
                // 右边孩子的子树区间与之相交
                if (node.getRightChild() != null && node.getContent().isRightChildOverlap(leftBound, rightBound)) {
                    // 发送请求给下游
                    client = new BroadcastClient(node.getRightChild().getPhysicalInfo().getIP());
                    ans.addAll(client.broadcastSearch(timeIndex, leftBound, rightBound));
                }
            }
            BroadcastProto.BroadResponse response = BroadcastProto.BroadResponse.newBuilder().addAllDests(ans).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }
}
