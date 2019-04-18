package edu.buaa.act.jcsindex.global.proto;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Created by shimin at 4/18/2019 8:58 PM
 **/
public class BroadcastClient {
    private static Logger log = Logger.getLogger(BroadcastServer.class);

    private final ManagedChannel channel;
    private final BroadcastSearchGrpc.BroadcastSearchBlockingStub blockingStub;

    public BroadcastClient(String host) {
        this(ManagedChannelBuilder.forAddress(host, 50051)
                .usePlaintext(true)
                .build());
    }

    public BroadcastClient(ManagedChannel channel) {
        this.channel = channel;
        blockingStub = BroadcastSearchGrpc.newBlockingStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public List<String> broadcastSearch(int timeIndex, long leftBound, long rightBound) {
        log.info("Start broadcast search");
        BroadcastProto.BroadRequest request = BroadcastProto.BroadRequest.newBuilder().setTimeIndex(timeIndex)
                                                                                        .setLeftBound(leftBound)
                                                                                        .setRightBound(rightBound)
                                                                                        .build();
        BroadcastProto.BroadResponse response;
        List<String> ans = new ArrayList<>();
        try {
            response = blockingStub.search(request);
            for (int i = 0; i < response.getDestsCount(); i++) {
                ans.add(response.getDests(i));
            }
        } catch (StatusRuntimeException e) {
            log.info("RPC failed: " + e.getStatus());
        }
        try {
            shutdown();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return ans;
    }
}
