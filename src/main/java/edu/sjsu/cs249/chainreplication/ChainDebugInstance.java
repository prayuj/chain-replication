package edu.sjsu.cs249.chainreplication;

import edu.sjsu.cs249.chain.*;
import io.grpc.stub.StreamObserver;

public class ChainDebugInstance extends ChainDebugGrpc.ChainDebugImplBase{

    ChainReplicationInstance chainReplicationInstance;

    ChainDebugInstance(ChainReplicationInstance chainReplicationInstance) {
        this.chainReplicationInstance = chainReplicationInstance;
    }
    @Override
    public void debug(ChainDebugRequest request, StreamObserver<ChainDebugResponse> responseObserver) {

    }

    @Override
    public void exit(ExitRequest request, StreamObserver<ExitResponse> responseObserver) {
        responseObserver.onNext(ExitResponse.newBuilder().build());
        responseObserver.onCompleted();
        System.exit(0);
    }
}
