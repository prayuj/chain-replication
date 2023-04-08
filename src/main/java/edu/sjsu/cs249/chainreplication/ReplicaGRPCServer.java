package edu.sjsu.cs249.chainreplication;

import edu.sjsu.cs249.chain.ReplicaGrpc;
import edu.sjsu.cs249.chain.*;
import io.grpc.stub.StreamObserver;
import org.apache.zookeeper.KeeperException;

public class ReplicaGRPCServer extends ReplicaGrpc.ReplicaImplBase {
    ChainReplicationInstance chainReplicationInstance;
    ReplicaGRPCServer(ChainReplicationInstance chainReplicationInstance){
        this.chainReplicationInstance = chainReplicationInstance;
    }
    @Override
    public void update(UpdateRequest request, StreamObserver<UpdateResponse> responseObserver) {
        chainReplicationInstance.addLog("update grpc called");

        String key = request.getKey();
        int newValue = request.getNewValue();
        int xid = request.getXid();

        chainReplicationInstance.addLog("xid: " + xid + ", key: " + key + ", newValue: " + newValue);
        chainReplicationInstance.replicaState.put(key, newValue);

        chainReplicationInstance.lastUpdateRequestXid = xid;
        chainReplicationInstance.pendingUpdateRequests.put(xid, new HashTableEntry(key, newValue));

        responseObserver.onNext(UpdateResponse.newBuilder().build());
        responseObserver.onCompleted();

        /*
         * TODO:
         *  1. If tail, ack to your predecessor
         *  2. else, tell successor of this update */

        chainReplicationInstance.addLog("isTail: " + chainReplicationInstance.isTail);
        if (chainReplicationInstance.isTail) {
            chainReplicationInstance.addLog("I am tail, ack back!");
            chainReplicationInstance.addLog("calling ack method of predecessor: " + chainReplicationInstance.predecessorAddress);
            chainReplicationInstance.lastProcessedXid = xid;
            chainReplicationInstance.pendingUpdateRequests.remove(xid);
            var channel = chainReplicationInstance.createChannel(chainReplicationInstance.predecessorAddress);
            var stub = ReplicaGrpc.newBlockingStub(channel);
            var ackRequest = AckRequest.newBuilder()
                    .setXid(xid).build();
            stub.ack(ackRequest);
        } else if (chainReplicationInstance.hasSuccessorContacted) {
            var channel = chainReplicationInstance.createChannel(chainReplicationInstance.successorAddress);
            var stub = ReplicaGrpc.newBlockingStub(channel);
            var updateRequest = UpdateRequest.newBuilder()
                    .setXid(xid)
                    .setKey(key)
                    .setNewValue(newValue)
                    .build();
            stub.update(updateRequest);
        }
    }

    @Override
    public void newSuccessor(NewSuccessorRequest request, StreamObserver<NewSuccessorResponse> responseObserver) {
        chainReplicationInstance.addLog("newSuccessor grpc called");

        long lastZxidSeen = request.getLastZxidSeen();
        int lastXid = request.getLastXid();
        int lastAck = request.getLastAck();
        String znodeName = request.getZnodeName();

        //TODO: add logic for state and update request.

        NewSuccessorResponse.Builder builder = NewSuccessorResponse.newBuilder();
        builder.setRc(0).setLastXid(chainReplicationInstance.lastUpdateRequestXid).putAllState(chainReplicationInstance.replicaState);

        chainReplicationInstance.addLog("response values:");
        chainReplicationInstance.addLog(
                "rc: " + 0 +
                ", lastXid: " + chainReplicationInstance.lastUpdateRequestXid +
                ", state: " + chainReplicationInstance.replicaState.toString());
        chainReplicationInstance.addLog("pending request values:");
        for(int xid: chainReplicationInstance.pendingUpdateRequests.keySet()) {
            builder.addSent(UpdateRequest.newBuilder()
                    .setXid(xid)
                    .setKey(chainReplicationInstance.pendingUpdateRequests.get(xid).key)
                    .setNewValue(chainReplicationInstance.pendingUpdateRequests.get(xid).value)
                    .build());
            chainReplicationInstance.addLog(
                    "xid: " + xid +
                    ", key: " + chainReplicationInstance.pendingUpdateRequests.get(xid).key +
                    ", value: "+ chainReplicationInstance.pendingUpdateRequests.get(xid).value);
        }
        try {
            chainReplicationInstance.successorReplicaName = znodeName;
            String data = new String(chainReplicationInstance.zk.getData(chainReplicationInstance.control_path + "/" + znodeName, false, null));
            chainReplicationInstance.successorAddress = data.split("\n")[0];
            chainReplicationInstance.addLog("new successor");
            chainReplicationInstance.addLog("successorAddress: " + chainReplicationInstance.successorAddress);
            chainReplicationInstance.addLog("successor name: " + data.split("\n")[1]);
        } catch (InterruptedException | KeeperException e) {
            chainReplicationInstance.addLog("error in getting successor address from zookeeper");
        }
        chainReplicationInstance.hasSuccessorContacted = true;
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void ack(AckRequest request, StreamObserver<AckResponse> responseObserver) {
        chainReplicationInstance.addLog("ack grpc called");
        int xid = request.getXid();

        chainReplicationInstance.lastProcessedXid = xid;
        chainReplicationInstance.lastAck = xid;
        chainReplicationInstance.pendingUpdateRequests.remove(xid);

        responseObserver.onNext(AckResponse.newBuilder().build());
        responseObserver.onCompleted();

        if (chainReplicationInstance.isHead) {
            chainReplicationInstance.addLog("sending response back to client");
            StreamObserver<HeadResponse> headResponseStreamObserver = chainReplicationInstance.pendingHeadStreamObserver.remove(xid);
            headResponseStreamObserver.onNext(HeadResponse.newBuilder().setRc(0).build());
            headResponseStreamObserver.onCompleted();
        } else {
            chainReplicationInstance.addLog("calling ack method of predecessor: " + chainReplicationInstance.predecessorAddress);
            var channel = chainReplicationInstance.createChannel(chainReplicationInstance.predecessorAddress);
            var stub = ReplicaGrpc.newBlockingStub(channel);
            stub.ack(AckRequest.newBuilder().setXid(xid).build());
        }
    }
}


