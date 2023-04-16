package edu.sjsu.cs249.chainreplication;

import edu.sjsu.cs249.chain.AckRequest;
import edu.sjsu.cs249.chain.ReplicaGrpc;
import edu.sjsu.cs249.chain.UpdateRequest;
import io.grpc.StatusRuntimeException;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class QueueableRequest<T> extends Thread {
    private final BlockingQueue<T> requestQueue;
    ChainReplicationInstance chainReplicationInstance;

    private boolean isExecuting;

    public QueueableRequest(ChainReplicationInstance chainReplicationInstance) {
        requestQueue = new LinkedBlockingQueue<>();
        this.chainReplicationInstance = chainReplicationInstance;
        isExecuting = false;
    }

    public void submitRequest(T request) {
        requestQueue.offer(request);
    }

    public boolean isEmpty() {
        return requestQueue.isEmpty();
    }

    public boolean isProcessing() {
        return isExecuting;
    }

    @Override
    public void run() {
        while (true) {
            try {
                T request = requestQueue.take();
                executeRequest(request);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void executeRequest(T request) throws InterruptedException {
        int retryCount = 0;
        boolean success = false;
        int delay = chainReplicationInstance.RETRY_INTERVAL; // initial delay time in milliseconds
        isExecuting = true;
        while (!success && retryCount < chainReplicationInstance.MAX_RETRIES) {
            try {
                if (request instanceof UpdateRequest) {
                    ReplicaGrpc.ReplicaBlockingStub stub = ReplicaGrpc.newBlockingStub(chainReplicationInstance.successorChannel).withDeadlineAfter(3L, TimeUnit.SECONDS);
                    stub.update((UpdateRequest) request);
                    System.out.println("sent update request with xid: " + ((UpdateRequest) request).getXid());
                } else if (request instanceof AckRequest) {
                    ReplicaGrpc.ReplicaBlockingStub stub = ReplicaGrpc.newBlockingStub(chainReplicationInstance.predecessorChannel).withDeadlineAfter(3L, TimeUnit.SECONDS);
                    stub.ack((AckRequest) request);
                    System.out.println("sent ack request with xid: " + ((AckRequest) request).getXid());
                }

                success = true; // request was successful, so exit the retry loop
            } catch (StatusRuntimeException e) {
                // handle the error
                System.err.println("Error occurred while executing the request: " + e.getMessage());
                retryCount++;
                Thread.sleep(delay);
                delay *= 2; // exponential backoff

            }
        }

        if (!success) {
            // retries failed, so log an error message
            System.err.println("Failed to execute the request after " + chainReplicationInstance.MAX_RETRIES + " retries: " + request);
        }
        isExecuting = false;
    }

}
