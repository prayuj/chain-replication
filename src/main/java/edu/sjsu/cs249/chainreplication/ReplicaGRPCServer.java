package edu.sjsu.cs249.chainreplication;

import edu.sjsu.cs249.chain.ReplicaGrpc;
import edu.sjsu.cs249.chain.*;
import io.grpc.stub.StreamObserver;

public class ReplicaGRPCServer extends ReplicaGrpc.ReplicaImplBase {
    ChainReplicationInstance chainReplicationInstance;
    ReplicaGRPCServer(ChainReplicationInstance chainReplicationInstance){
        this.chainReplicationInstance = chainReplicationInstance;
    }
    @Override
    public void update(UpdateRequest request, StreamObserver<UpdateResponse> responseObserver) {
        chainReplicationInstance.logs.add("update grpc called");

        String key = request.getKey();
        int newValue = request.getNewValue();
        int xid = request.getXid();

        chainReplicationInstance.logs.add("key: " + key + ", newValue: " + newValue);
        chainReplicationInstance.replicaState.put(key, newValue);
        chainReplicationInstance.pendingUpdateRequests.put(xid, new HashTableEntry(key, newValue));

        responseObserver.onNext(UpdateResponse.newBuilder().build());
        responseObserver.onCompleted();

//        TODO:
//        1. If not tail, tell successor of this update
//        2. If tail, ack to your predecessor

        if (chainReplicationInstance.isTail) {

        } else {

        }

    }

    @Override
    public void newSuccessor(NewSuccessorRequest request, StreamObserver<NewSuccessorResponse> responseObserver) {
        chainReplicationInstance.logs.add("newSuccessor grpc called");

        long lastZxidSeen = request.getLastZxidSeen();
        int lastXid = request.getLastXid();
        int lastAck = request.getLastAck();
        String znodeName = request.getZnodeName();

        //TODO: add logic for state and update request.

        NewSuccessorResponse.Builder builder = NewSuccessorResponse.newBuilder();
        builder.setRc(0).setLastXid(chainReplicationInstance.lastUpdateRequestXid).putAllState(chainReplicationInstance.replicaState);

        chainReplicationInstance.logs.add("response values:");
        chainReplicationInstance.logs.add(
                "rc: " + 0 +
                ", lastXid: " + chainReplicationInstance.lastUpdateRequestXid +
                ", state: " + chainReplicationInstance.replicaState.toString());
        chainReplicationInstance.logs.add("pending request values:");
        for(int xid: chainReplicationInstance.pendingUpdateRequests.keySet()) {
            builder.addSent(UpdateRequest.newBuilder()
                    .setXid(xid)
                    .setKey(chainReplicationInstance.pendingUpdateRequests.get(xid).key)
                    .setNewValue(chainReplicationInstance.pendingUpdateRequests.get(xid).value)
                    .build());
            chainReplicationInstance.logs.add(
                    "xid: " + xid +
                    ", key: " + chainReplicationInstance.pendingUpdateRequests.get(xid).key +
                    ", value: "+ chainReplicationInstance.pendingUpdateRequests.get(xid).value);
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void ack(AckRequest request, StreamObserver<AckResponse> responseObserver) {
        chainReplicationInstance.logs.add("ack grpc called");
        int xid = request.getXid();

        chainReplicationInstance.lastProcessedXid = xid;
        chainReplicationInstance.pendingUpdateRequests.remove(xid);

        responseObserver.onNext(AckResponse.newBuilder().build());
        responseObserver.onCompleted();

        if (chainReplicationInstance.isHead) {
            chainReplicationInstance.logs.add("sending response back to client");
            StreamObserver<HeadResponse> headResponseStreamObserver = chainReplicationInstance.pendingHeadStreamObserver.remove(xid);
            headResponseStreamObserver.onNext(HeadResponse.newBuilder().setRc(0).build());
            headResponseStreamObserver.onCompleted();
        } else {
            chainReplicationInstance.logs.add("calling ack method of predecessor: " + chainReplicationInstance.predecessorAddress);
            var channel = chainReplicationInstance.createChannel(chainReplicationInstance.predecessorAddress);
            var stub = ReplicaGrpc.newBlockingStub(channel);
            stub.ack(AckRequest.newBuilder().setXid(xid).build());
        }
    }
}


