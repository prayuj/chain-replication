package edu.sjsu.cs249.chainreplication;

import edu.sjsu.cs249.chain.*;
import io.grpc.stub.StreamObserver;

public class HeadChainReplicaGRPCServer extends HeadChainReplicaGrpc.HeadChainReplicaImplBase   {
    ChainReplicationInstance chainReplicationInstance;
    HeadChainReplicaGRPCServer(ChainReplicationInstance chainReplicationInstance){
        this.chainReplicationInstance = chainReplicationInstance;
    }
    @Override
    public synchronized void increment(IncRequest request, StreamObserver<HeadResponse> responseObserver) {
        try {
            chainReplicationInstance.semaphore.acquire();
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

                if (!chainReplicationInstance.hasSuccessorContacted) return;
                chainReplicationInstance.addLog("making update call to successor: " + chainReplicationInstance.successorAddress);
                chainReplicationInstance.addLog("params:" +
                        ", xid: " + xid +
                        ", key: " + key +
                        ", newValue: " + newValue);
                var channel = chainReplicationInstance.createChannel(chainReplicationInstance.successorAddress);
                var stub = ReplicaGrpc.newBlockingStub(channel);
                var updateRequest = UpdateRequest.newBuilder()
                        .setXid(xid)
                        .setKey(key)
                        .setNewValue(newValue)
                        .build();
                stub.update(updateRequest);
                channel.shutdownNow();
            }

        } catch (InterruptedException e) {
            chainReplicationInstance.addLog("Problem acquiring semaphore");
            chainReplicationInstance.addLog(e.getMessage());
        } finally {
            chainReplicationInstance.semaphore.release();
        }
    }
}
