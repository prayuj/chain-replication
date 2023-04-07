package edu.sjsu.cs249.chainreplication;

import edu.sjsu.cs249.chain.*;
import io.grpc.stub.StreamObserver;

import java.util.HashMap;

public class HeadChainReplicaGRPCServer extends HeadChainReplicaGrpc.HeadChainReplicaImplBase   {
    ChainReplicationInstance chainReplicationInstance;
    HeadChainReplicaGRPCServer(ChainReplicationInstance chainReplicationInstance){
        this.chainReplicationInstance = chainReplicationInstance;
    }
    @Override
    public void increment(IncRequest request, StreamObserver<HeadResponse> responseObserver) {
        if (!chainReplicationInstance.isHead) {
            responseObserver.onNext(HeadResponse.newBuilder().setRc(1).build());
            responseObserver.onCompleted();
            return;
        }
        String key = request.getKey();
        int incrementer = request.getIncValue();
        int newValue;
        if (chainReplicationInstance.replicaState.containsKey(key)) {
            newValue = chainReplicationInstance.replicaState.get(key) + incrementer;
        } else {
            newValue = incrementer;
        }
        chainReplicationInstance.replicaState.put(key, newValue);
        chainReplicationInstance.pendingUpdateRequests.put(++chainReplicationInstance.lastXid, new HashTableEntry(key, newValue));
        chainReplicationInstance.pendingHeadStreamObserver.put(chainReplicationInstance.lastZxidSeen, responseObserver);
    }
}
