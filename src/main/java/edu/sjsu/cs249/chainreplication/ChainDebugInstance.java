package edu.sjsu.cs249.chainreplication;

import edu.sjsu.cs249.chain.*;
import io.grpc.stub.StreamObserver;

public class ChainDebugInstance extends ChainDebugGrpc.ChainDebugImplBase{

    final ChainReplicationInstance chainReplicationInstance;

    ChainDebugInstance(ChainReplicationInstance chainReplicationInstance) {
        this.chainReplicationInstance = chainReplicationInstance;
    }

    /*
    * Method for Debug RPC calls
    * Without thread safety (especially for logs array), I faced numerous errors where I was getting race conditions
    * If I make it thread safe, then the client's request deadline completes before my program becomes thread safe
    * */

    @Override
    public void debug(ChainDebugRequest request, StreamObserver<ChainDebugResponse> responseObserver) {
//        try {
//            chainReplicationInstance.logLock.acquire();
            ChainDebugResponse.Builder builder = ChainDebugResponse.newBuilder();
            System.out.println("debug grpc called");
            builder
                .setXid(chainReplicationInstance.lastUpdateRequestXid)
                .putAllState(chainReplicationInstance.replicaState)
                .addAllLogs(chainReplicationInstance.logs);

            for (int key : chainReplicationInstance.pendingUpdateRequests.keySet()) {
                builder.addSent(UpdateRequest.newBuilder()
                        .setXid(key)
                        .setKey(chainReplicationInstance.pendingUpdateRequests.get(key).key)
                        .setNewValue(chainReplicationInstance.pendingUpdateRequests.get(key).value)
                        .build());
            }
            System.out.println("xid: " + builder.getXid() +
                    ", state: " + builder.getStateMap() +
                    ", sent: " + builder.getSentList());
            System.out.println("exiting debug synchronized block");
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
//        } catch (InterruptedException e) {
//            System.out.println("Problem acquiring semaphore");
//            System.out.println(e.getMessage());
//        } finally {
//            chainReplicationInstance.logLock.release();
//        }
    }

    @Override
    public void exit(ExitRequest request, StreamObserver<ExitResponse> responseObserver) {

        //synchronize and acquire ack semaphore to allow all pending requests to complete before you exit
        synchronized (chainReplicationInstance) {
            try {
                chainReplicationInstance.ackSemaphore.acquire();
                System.out.println("Exiting Program!");
                responseObserver.onNext(ExitResponse.newBuilder().build());
                responseObserver.onCompleted();
                chainReplicationInstance.addLog("releasing semaphore for exit");
                chainReplicationInstance.ackSemaphore.release();
                System.exit(0);
            } catch (InterruptedException e) {
                chainReplicationInstance.addLog("Problem acquiring semaphore");
                chainReplicationInstance.addLog(e.getMessage());
                System.exit(0);
            }
        }
    }
}
