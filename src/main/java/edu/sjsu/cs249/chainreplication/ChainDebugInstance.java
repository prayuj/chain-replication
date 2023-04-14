package edu.sjsu.cs249.chainreplication;

import edu.sjsu.cs249.chain.*;
import io.grpc.stub.StreamObserver;

public class ChainDebugInstance extends ChainDebugGrpc.ChainDebugImplBase{

    ChainReplicationInstance chainReplicationInstance;

    ChainDebugInstance(ChainReplicationInstance chainReplicationInstance) {
        this.chainReplicationInstance = chainReplicationInstance;
    }
    @Override
    public synchronized void debug(ChainDebugRequest request, StreamObserver<ChainDebugResponse> responseObserver) {
        try {
            chainReplicationInstance.addLog("trying to acquire semaphore in debug");
            chainReplicationInstance.semaphore.acquire();
            chainReplicationInstance.addLog("debug rpc called");
            ChainDebugResponse.Builder builder = ChainDebugResponse.newBuilder();
            builder
                    .setXid(chainReplicationInstance.lastAckXid)
                    .putAllState(chainReplicationInstance.replicaState)
                    .addAllLogs(chainReplicationInstance.logs);

            for(int key: chainReplicationInstance.pendingUpdateRequests.keySet()) {
                builder.addSent(UpdateRequest.newBuilder()
                        .setXid(key)
                        .setKey(chainReplicationInstance.pendingUpdateRequests.get(key).key)
                        .setNewValue(chainReplicationInstance.pendingUpdateRequests.get(key).value)
                        .build());
            }
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } catch (InterruptedException e) {
            chainReplicationInstance.addLog("Problem acquiring semaphore");
            chainReplicationInstance.addLog(e.getMessage());
        } finally {
            chainReplicationInstance.addLog("releasing semaphore for debug");
            chainReplicationInstance.semaphore.release();
        }
    }

    @Override
    public void exit(ExitRequest request, StreamObserver<ExitResponse> responseObserver) {
        try {
            chainReplicationInstance.addLog("trying to acquire semaphore in exit");
            chainReplicationInstance.semaphore.acquire();
            System.out.println("Exiting Program!");
            responseObserver.onNext(ExitResponse.newBuilder().build());
            responseObserver.onCompleted();
            System.exit(0);
        } catch (InterruptedException e) {
            chainReplicationInstance.addLog("Problem acquiring semaphore");
            chainReplicationInstance.addLog(e.getMessage());
        } finally {
            chainReplicationInstance.addLog("releasing semaphore for exit");
            chainReplicationInstance.semaphore.release();
        }
    }
}
