package edu.sjsu.cs249.chainreplication;

import edu.sjsu.cs249.chain.*;
import edu.sjsu.cs249.chainreplication.*;
import io.grpc.stub.StreamObserver;

public class TailChainReplicaGRPCServer extends TailChainReplicaGrpc.TailChainReplicaImplBase {
    ChainReplicationInstance chainReplicationInstance;
    TailChainReplicaGRPCServer(ChainReplicationInstance chainReplicationInstance){
        this.chainReplicationInstance = chainReplicationInstance;
    }
    @Override
    public synchronized void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        try {
            chainReplicationInstance.semaphore.acquire();
            chainReplicationInstance.addLog("get grpc called");
            if (!chainReplicationInstance.isTail) {
                responseObserver.onNext(GetResponse.newBuilder().setRc(1).build());
                responseObserver.onCompleted();
                return;
            }
            String key = request.getKey();
            int value = chainReplicationInstance.replicaState.getOrDefault(key, 0);
            responseObserver.onNext(GetResponse.newBuilder().setValue(value).setRc(0).build());
            responseObserver.onCompleted();
        } catch (InterruptedException e) {
            chainReplicationInstance.addLog("Problem acquiring semaphore");
            chainReplicationInstance.addLog(e.getMessage());
        } finally {
            chainReplicationInstance.semaphore.release();
        }
        String key = request.getKey();
        int value = chainReplicationInstance.replicaState.getOrDefault(key, 0);
        responseObserver.onNext(GetResponse.newBuilder().setValue(value).setRc(0).build());
        responseObserver.onCompleted();
    }
}
