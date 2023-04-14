package edu.sjsu.cs249.chainreplication;

import edu.sjsu.cs249.chain.*;
import io.grpc.stub.StreamObserver;

public class ChainDebugInstance extends ChainDebugGrpc.ChainDebugImplBase{

    final ChainReplicationInstance chainReplicationInstance;

    ChainDebugInstance(ChainReplicationInstance chainReplicationInstance) {
        this.chainReplicationInstance = chainReplicationInstance;
    }
    @Override
    public void debug(ChainDebugRequest request, StreamObserver<ChainDebugResponse> responseObserver) {
//        synchronized (chainReplicationInstance) {
            ChainDebugResponse.Builder builder = ChainDebugResponse.newBuilder();
            chainReplicationInstance.addLog("debug grpc called");
            builder
                .setXid(chainReplicationInstance.lastUpdateRequestXid)
                .putAllState(chainReplicationInstance.replicaState)
                .addAllLogs(chainReplicationInstance.logs);

            for(int key: chainReplicationInstance.pendingUpdateRequests.keySet()) {
                builder.addSent(UpdateRequest.newBuilder()
                    .setXid(key)
                    .setKey(chainReplicationInstance.pendingUpdateRequests.get(key).key)
                    .setNewValue(chainReplicationInstance.pendingUpdateRequests.get(key).value)
                    .build());
            }
            chainReplicationInstance.addLog("xid: " + builder.getXid() +
                    ", state: " + builder.getStateMap() +
                    ", sent: " + builder.getSentList());
            chainReplicationInstance.addLog("exiting debug synchronized block");
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
//        }
    }

    @Override
    public void exit(ExitRequest request, StreamObserver<ExitResponse> responseObserver) {
        synchronized (chainReplicationInstance) {
            System.out.println("Exiting Program!");
            responseObserver.onNext(ExitResponse.newBuilder().build());
            responseObserver.onCompleted();
            System.exit(0);
        }
    }
}
