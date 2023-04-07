package edu.sjsu.cs249.chainreplication;

import edu.sjsu.cs249.chain.*;
import io.grpc.stub.StreamObserver;

public class HeadChainReplicaGRPCServer extends HeadChainReplicaGrpc.HeadChainReplicaImplBase   {
    ChainReplicationInstance chainReplicationInstance;
    HeadChainReplicaGRPCServer(ChainReplicationInstance chainReplicationInstance){
        this.chainReplicationInstance = chainReplicationInstance;
    }
    @Override
    public void increment(IncRequest request, StreamObserver<HeadResponse> responseObserver) {
        chainReplicationInstance.logs.add("increment grpc called");
        if (!chainReplicationInstance.isHead) {
            chainReplicationInstance.logs.add("not head, cannot update");
            responseObserver.onNext(HeadResponse.newBuilder().setRc(1).build());
            responseObserver.onCompleted();
            return;
        }
        String key = request.getKey();
        int incrementer = request.getIncValue();
        int newValue;
        if (chainReplicationInstance.replicaState.containsKey(key)) {
            chainReplicationInstance.logs.add("key: " + key + ", " + "oldValue: " + chainReplicationInstance.replicaState.get(key));
            newValue = chainReplicationInstance.replicaState.get(key) + incrementer;
        } else {
            chainReplicationInstance.logs.add("key: " + key + ", " + "oldValue: " + 0);
            newValue = incrementer;
        }

        chainReplicationInstance.logs.add("key: " + key + ", " + "newValue: " + newValue);
        chainReplicationInstance.replicaState.put(key, newValue);
        ++chainReplicationInstance.lastUpdateRequestXid;
        chainReplicationInstance.logs.add("xid generated: " + chainReplicationInstance.lastUpdateRequestXid);
        chainReplicationInstance.pendingUpdateRequests.put(chainReplicationInstance.lastUpdateRequestXid, new HashTableEntry(key, newValue));
        chainReplicationInstance.pendingHeadStreamObserver.put(chainReplicationInstance.lastUpdateRequestXid, responseObserver);
    }
}
