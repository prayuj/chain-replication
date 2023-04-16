package edu.sjsu.cs249.chainreplication;

import edu.sjsu.cs249.chain.*;
import io.grpc.stub.StreamObserver;

public class HeadChainReplicaGRPCServer extends HeadChainReplicaGrpc.HeadChainReplicaImplBase   {
    final ChainReplicationInstance chainReplicationInstance;
    HeadChainReplicaGRPCServer(ChainReplicationInstance chainReplicationInstance){
        this.chainReplicationInstance = chainReplicationInstance;
    }
    @Override
    public void increment(IncRequest request, StreamObserver<HeadResponse> responseObserver) {
        synchronized (chainReplicationInstance) {
            chainReplicationInstance.addLog("increment grpc called");
            if (!chainReplicationInstance.isHead) {
                chainReplicationInstance.addLog("not head, cannot update");
                responseObserver.onNext(HeadResponse.newBuilder().setRc(1).build());
                responseObserver.onCompleted();
                return;
            }
            String key = request.getKey();
            int incrementer = request.getIncValue();
            int newValue;
            if (chainReplicationInstance.replicaState.containsKey(key)) {
                newValue = chainReplicationInstance.replicaState.get(key) + incrementer;
                chainReplicationInstance.addLog("key: " + key + ", " + "oldValue: " + chainReplicationInstance.replicaState.get(key) + ", " + "newValue: " + newValue);
            } else {
                newValue = incrementer;
                chainReplicationInstance.addLog("key: " + key + ", " + "oldValue: " + 0+ ", " + "newValue: " + newValue);
            }
            chainReplicationInstance.replicaState.put(key, newValue);
            int xid = ++chainReplicationInstance.lastUpdateRequestXid;
            chainReplicationInstance.addLog("xid generated: " + xid);

            if (chainReplicationInstance.isTail) {
                chainReplicationInstance.lastAckXid = xid;
                responseObserver.onNext(HeadResponse.newBuilder().setRc(0).build());
                responseObserver.onCompleted();
            } else {
                chainReplicationInstance.pendingUpdateRequests.put(xid, new HashTableEntry(key, newValue));
                chainReplicationInstance.pendingHeadStreamObserver.put(xid, responseObserver);

                chainReplicationInstance.addLog("hasSuccessorContacted: " + chainReplicationInstance.hasSuccessorContacted);
                if (!chainReplicationInstance.hasSuccessorContacted) return;
                chainReplicationInstance.updateSuccessor(key, newValue, xid);
            }
            chainReplicationInstance.addLog("exiting increment synchronized block");
        }
    }
}
