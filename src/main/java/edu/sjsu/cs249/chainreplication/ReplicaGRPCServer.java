package edu.sjsu.cs249.chainreplication;

import com.google.rpc.Code;
import edu.sjsu.cs249.chain.ReplicaGrpc;
import edu.sjsu.cs249.chain.*;
import io.grpc.stub.StreamObserver;
import org.apache.zookeeper.KeeperException;

import java.util.Objects;

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

        chainReplicationInstance.addLog("isTail: " + chainReplicationInstance.isTail);
        if (chainReplicationInstance.isTail) {
            chainReplicationInstance.addLog("I am tail, ack back!");
            chainReplicationInstance.ackXid(xid);
        } else if (chainReplicationInstance.hasSuccessorContacted) {
            var channel = chainReplicationInstance.createChannel(chainReplicationInstance.successorAddress);
            var stub = ReplicaGrpc.newBlockingStub(channel);
            var updateRequest = UpdateRequest.newBuilder()
                    .setXid(xid)
                    .setKey(key)
                    .setNewValue(newValue)
                    .build();
            stub.update(updateRequest);
            channel.shutdown();

        }
    }

    @Override
    public void newSuccessor(NewSuccessorRequest request, StreamObserver<NewSuccessorResponse> responseObserver) {
        chainReplicationInstance.addLog("newSuccessor grpc called");

        long lastZxidSeen = request.getLastZxidSeen();
        int lastXid = request.getLastXid();
        int lastAck = request.getLastAck();
        String znodeName = request.getZnodeName();

        chainReplicationInstance.addLog("request params");
        chainReplicationInstance.addLog("lastZxidSeen: " + lastZxidSeen +
                ", lastXid: " + lastXid +
                ", lastAck: " + lastAck +
                ", znodeName: " + znodeName);
        chainReplicationInstance.addLog("my lastZxidSeen: " + chainReplicationInstance.lastZxidSeen);

        if (lastZxidSeen < chainReplicationInstance.lastZxidSeen) {
            chainReplicationInstance.addLog("replica has older view of zookeeper than me, ignoring request");
            responseObserver.onNext(NewSuccessorResponse.newBuilder().setRc(-1).build());
            responseObserver.onCompleted();
        }
        else if (lastZxidSeen == chainReplicationInstance.lastZxidSeen) {
            chainReplicationInstance.addLog("my successorReplicaName: " + chainReplicationInstance.successorZNode);
            if (Objects.equals(chainReplicationInstance.successorZNode, znodeName)) {
                successorProcedure(lastAck, lastXid, znodeName, responseObserver);
            } else {
                chainReplicationInstance.addLog("replica is not the replica i saw in my view of zookeeper");
                responseObserver.onNext(NewSuccessorResponse.newBuilder().setRc(-1).build());
                responseObserver.onCompleted();
            }
        }
        else if (lastZxidSeen > chainReplicationInstance.lastZxidSeen){
            chainReplicationInstance.addLog("replica has newer view of zookeeper than me, syncing request");
            chainReplicationInstance.zk.sync(chainReplicationInstance.control_path, (i, s, o) -> {
                if (i == Code.OK_VALUE && Objects.equals(chainReplicationInstance.successorZNode, znodeName)) {
                    successorProcedure(lastAck, lastXid, znodeName, responseObserver);
                }
            }, null);
        }
    }

    public void successorProcedure(int lastAck, int lastXid, String znodeName, StreamObserver<NewSuccessorResponse> responseObserver) {
        NewSuccessorResponse.Builder builder = NewSuccessorResponse.newBuilder();
        builder.setRc(1);

        //If acks mismatch, then some state might be missing
        if (lastAck != -1) {
            builder.setRc(0)
                    .putAllState(chainReplicationInstance.replicaState);
        }

        //TODO: just send everything
        for (int xid = lastXid + 1; xid <= chainReplicationInstance.lastUpdateRequestXid; xid += 1) {
            if (chainReplicationInstance.pendingUpdateRequests.containsKey(xid)) {
                builder.addSent(UpdateRequest.newBuilder()
                        .setXid(xid)
                        .setKey(chainReplicationInstance.pendingUpdateRequests.get(xid).key)
                        .setNewValue(chainReplicationInstance.pendingUpdateRequests.get(xid).value)
                        .build());
            }
        }
        builder.setLastXid(chainReplicationInstance.lastAckXid);

        chainReplicationInstance.addLog("response values:");
        chainReplicationInstance.addLog(
                "rc: " + builder.getRc() +
                        ", lastXid: " + builder.getLastXid() +
                        ", state: " + builder.getStateMap() +
                        ", sent: " + builder.getSentList());

        try {
            String data = new String(chainReplicationInstance.zk.getData(znodeName, false, null));
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
        chainReplicationInstance.addLog("xid: " + xid);

        chainReplicationInstance.lastAckXid = xid;
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
            channel.shutdown();
        }
    }
}


