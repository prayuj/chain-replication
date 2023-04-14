package edu.sjsu.cs249.chainreplication;

import edu.sjsu.cs249.chain.*;
import io.grpc.stub.StreamObserver;

public class TailChainReplicaGRPCServer extends TailChainReplicaGrpc.TailChainReplicaImplBase {
    final ChainReplicationInstance chainReplicationInstance;
    TailChainReplicaGRPCServer(ChainReplicationInstance chainReplicationInstance){
        this.chainReplicationInstance = chainReplicationInstance;
    }
    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        synchronized (chainReplicationInstance) {
            chainReplicationInstance.addLog("get grpc called");
            if (!chainReplicationInstance.isTail) {
                responseObserver.onNext(GetResponse.newBuilder().setRc(1).build());
                responseObserver.onCompleted();
                return;
            }
            String key = request.getKey();
            int value = chainReplicationInstance.replicaState.getOrDefault(key, 0);
            responseObserver.onNext(GetResponse.newBuilder().setValue(value).setRc(0).build());
            responseObserver.onCompleted();
            chainReplicationInstance.addLog("exiting get synchronized block");
        }
    }
}
