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
        String key = request.getKey();
        int newValue = request.getNewValue();
        int xid = request.getXid();

        chainReplicationInstance.replicaState.put(key, newValue);
        chainReplicationInstance.pendingUpdateRequests.put(xid, new HashTableEntry(key, newValue));

        responseObserver.onNext(UpdateResponse.newBuilder().build());
        responseObserver.onCompleted();

//        TODO:
//        1. If not tail, tell successor of this update
//        2. If tail, ack to your predecessor
    }

    @Override
    public void newSuccessor(NewSuccessorRequest request, StreamObserver<NewSuccessorResponse> responseObserver) {
        long lastZxidSeen = request.getLastZxidSeen();
        int lastXid = request.getLastXid();
        int lastAck = request.getLastAck();
        String znodeName = request.getZnodeName();
    }

    @Override
    public void ack(AckRequest request, StreamObserver<AckResponse> responseObserver) {
        int xid = request.getXid();

        chainReplicationInstance.pendingUpdateRequests.remove(xid);

        responseObserver.onNext(AckResponse.newBuilder().build());
        responseObserver.onCompleted();

        if (chainReplicationInstance.isHead) {
            StreamObserver<HeadResponse> headResponseStreamObserver = chainReplicationInstance.pendingHeadStreamObserver.get(xid);
            headResponseStreamObserver.onNext(HeadResponse.newBuilder().setRc(0).build());
            headResponseStreamObserver.onCompleted();
            chainReplicationInstance.pendingHeadStreamObserver.remove(xid);
        }
    }
}


