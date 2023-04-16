package edu.sjsu.cs249.chainreplication;

import com.google.rpc.Code;
import edu.sjsu.cs249.chain.ReplicaGrpc;
import edu.sjsu.cs249.chain.*;
import io.grpc.stub.StreamObserver;
import org.apache.zookeeper.KeeperException;

import java.util.Objects;

public class ReplicaGRPCServer extends ReplicaGrpc.ReplicaImplBase {
    final ChainReplicationInstance chainReplicationInstance;
    ReplicaGRPCServer(ChainReplicationInstance chainReplicationInstance){
        this.chainReplicationInstance = chainReplicationInstance;
    }
    @Override
    public void update(UpdateRequest request, StreamObserver<UpdateResponse> responseObserver) {
        synchronized (chainReplicationInstance) {
            chainReplicationInstance.addLog("update grpc called");

            String key = request.getKey();
            int newValue = request.getNewValue();
            int xid = request.getXid();

            chainReplicationInstance.addLog("xid: " + xid + ", key: " + key + ", newValue: " + newValue);

            chainReplicationInstance.replicaState.put(key, newValue);
            chainReplicationInstance.lastUpdateRequestXid = xid;
            chainReplicationInstance.pendingUpdateRequests.put(xid, new HashTableEntry(key, newValue));

            chainReplicationInstance.addLog("isTail: " + chainReplicationInstance.isTail);
            chainReplicationInstance.addLog("hasSuccessorContacted: " + chainReplicationInstance.hasSuccessorContacted);
            if (chainReplicationInstance.isTail) {
                chainReplicationInstance.addLog("I am tail, ack back!");
                chainReplicationInstance.ackPredecessor(xid);
            } else if (chainReplicationInstance.hasSuccessorContacted) {
                chainReplicationInstance.updateSuccessor(key, newValue, xid);
            }
            responseObserver.onNext(UpdateResponse.newBuilder().build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void newSuccessor(NewSuccessorRequest request, StreamObserver<NewSuccessorResponse> responseObserver) {
        synchronized (chainReplicationInstance) {
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
            else {
                chainReplicationInstance.addLog("replica has newer view of zookeeper than me, syncing request");
                chainReplicationInstance.zk.sync(chainReplicationInstance.control_path, (i, s, o) -> {
                    if (i == Code.OK_VALUE && Objects.equals(chainReplicationInstance.successorZNode, znodeName)) {
                        successorProcedure(lastAck, lastXid, znodeName, responseObserver);
                    }
                }, null);
            }
            chainReplicationInstance.addLog("exiting newSuccessor synchronized block");
        }
    }

    public void successorProcedure(int lastAck, int lastXid, String znodeName, StreamObserver<NewSuccessorResponse> responseObserver) {
        NewSuccessorResponse.Builder builder = NewSuccessorResponse.newBuilder();
        builder.setRc(1);
//        chainReplicationInstance.predecessorQueue.pause();
        //If lastXid is -1, send all state
        if (lastXid == -1) {
            builder.setRc(0)
                .putAllState(chainReplicationInstance.replicaState);
        }

        // send update request starting from their lastXid + 1 till your lastXid
        for (int xid = lastXid + 1; xid <= chainReplicationInstance.lastUpdateRequestXid; xid += 1) {
            if (chainReplicationInstance.pendingUpdateRequests.containsKey(xid)) {
                builder.addSent(UpdateRequest.newBuilder()
                        .setXid(xid)
                        .setKey(chainReplicationInstance.pendingUpdateRequests.get(xid).key)
                        .setNewValue(chainReplicationInstance.pendingUpdateRequests.get(xid).value)
                        .build());
            }
        }

        // ack back request start from your lastAck till their last ack
        for (int myAckXid = chainReplicationInstance.lastAckXid + 1; myAckXid <= lastAck; myAckXid += 1) {
            chainReplicationInstance.ackPredecessor(myAckXid);
        }

        builder.setLastXid(chainReplicationInstance.lastUpdateRequestXid);

        chainReplicationInstance.addLog("response values:");
        chainReplicationInstance.addLog(
                "rc: " + builder.getRc() +
                        ", lastXid: " + builder.getLastXid() +
                        ", state: " + builder.getStateMap() +
                        ", sent: " + builder.getSentList());

        try {
            String data = new String(chainReplicationInstance.zk.getData(chainReplicationInstance.control_path + "/" + znodeName, false, null));
            chainReplicationInstance.successorAddress = data.split("\n")[0];
            chainReplicationInstance.successorChannel = chainReplicationInstance.createChannel(chainReplicationInstance.successorAddress);
            chainReplicationInstance.addLog("new successor");
            chainReplicationInstance.addLog("successorAddress: " + chainReplicationInstance.successorAddress);
            chainReplicationInstance.addLog("successor name: " + data.split("\n")[1]);
        } catch (InterruptedException | KeeperException e) {
            chainReplicationInstance.addLog("error in getting successor address from zookeeper");
        }
        chainReplicationInstance.addLog("successfully connected to successor, resuming successorQueue");
//        chainReplicationInstance.predecessorQueue.play();
        chainReplicationInstance.hasSuccessorContacted = true;
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void ack(AckRequest request, StreamObserver<AckResponse> responseObserver) {
        synchronized (chainReplicationInstance) {
            chainReplicationInstance.addLog("ack grpc called");
            int xid = request.getXid();

            chainReplicationInstance.addLog("xid: " + xid);

            if (chainReplicationInstance.isHead) {
                chainReplicationInstance.lastAckXid = xid;
                chainReplicationInstance.pendingUpdateRequests.remove(xid);
                chainReplicationInstance.addLog("sending response back to client");
                StreamObserver<HeadResponse> headResponseStreamObserver = chainReplicationInstance.pendingHeadStreamObserver.remove(xid);
                headResponseStreamObserver.onNext(HeadResponse.newBuilder().setRc(0).build());
                headResponseStreamObserver.onCompleted();
            } else {
                chainReplicationInstance.ackPredecessor(xid);
            }
            responseObserver.onNext(AckResponse.newBuilder().build());
            responseObserver.onCompleted();
        }
    }
}


