package edu.sjsu.cs249.chainreplication;

import edu.sjsu.cs249.chain.HeadResponse;
import edu.sjsu.cs249.chain.NewSuccessorRequest;
import edu.sjsu.cs249.chain.ReplicaGrpc;
import edu.sjsu.cs249.chain.UpdateRequest;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

public class ChainReplicationInstance {
    String name;
    String grpcHostPort;
    String zookeeper_server_list;
    String control_path;
    ZooKeeper zk;
    boolean isHead;
    boolean isTail;
    int lastZxidSeen;
    int lastXid;
    int lastAck;
    String myReplicaName;
    String predecessorAddress;
    String predecessorName;
    HashMap <Integer, HashTableEntry> pendingUpdateRequests;
    HashMap<Integer, StreamObserver<HeadResponse>> pendingHeadStreamObserver;
    HashMap <String, Integer> replicaState;
    List<String> replicas;

    ChainReplicationInstance(String name, String grpcHostPort, String zookeeper_server_list, String control_path) {
        this.name = name;
        this.grpcHostPort = grpcHostPort;
        this.zookeeper_server_list = zookeeper_server_list;
        this.control_path = control_path;
        isHead = false;
        isTail = false;
        lastZxidSeen = -1;
        lastXid = -1;
        lastAck = -1;
        pendingUpdateRequests = new HashMap<>();
        pendingHeadStreamObserver = new HashMap<>();
        replicaState = new HashMap<>();
    }
    void start () throws IOException, InterruptedException, KeeperException {
        zk = new ZooKeeper(zookeeper_server_list, 10000, System.out::println);
        myReplicaName = zk.create(control_path + "/replica-", (grpcHostPort + "\n" + name).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);


        this.getChildrenInPath();
        System.out.println(replicas.toString());

        this.callPredecessor();

        HeadChainReplicaGRPCServer headChainReplicaGRPCServer = new HeadChainReplicaGRPCServer(this);
        TailChainReplicaGRPCServer tailChainReplicaGRPCServer = new TailChainReplicaGRPCServer(this);
        ReplicaGRPCServer replicaGRPCServer = new ReplicaGRPCServer(this);

        Server server = ServerBuilder.forPort(Integer.parseInt(grpcHostPort.split(":")[1]))
                .addService(headChainReplicaGRPCServer)
                .addService(tailChainReplicaGRPCServer)
                .addService(replicaGRPCServer)
                .build();
        server.start();
        System.out.printf("will listen on port %s\n", server.getPort());
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                zk.close();
            } catch (InterruptedException e) {
                System.out.println("Error in closing zookeeper instance");
                e.printStackTrace();
            }
            server.shutdown();
            System.out.println("Successfully stopped the server");
        }));
        server.awaitTermination();
    }

    private Watcher childrenWatcher() {
        return watchedEvent -> {
            System.out.println("In childrenWatcher");
            System.out.println("WatchedEvent: " + watchedEvent.getType() + " on " + watchedEvent.getPath());
            try {
                ChainReplicationInstance.this.getChildrenInPath();
                System.out.println(replicas.toString());
            } catch (InterruptedException | KeeperException e) {
                System.out.println("Error getting children with getChildrenInPath()");
            }
        };
    }

    private void getChildrenInPath() throws InterruptedException, KeeperException {
        List<String> children = zk.getChildren(control_path, childrenWatcher());
        replicas = children.stream().filter(
                child -> child.contains("replica-")).toList();
    }

    private Watcher predecessorWatcher() {
        return watchedEvent -> {
            System.out.println("In predecessorWatcher watcher");
            System.out.println("WatchedEvent: " + watchedEvent.getType() + " on " + watchedEvent.getPath());
            try {
                ChainReplicationInstance.this.getChildrenInPath();
                System.out.println(replicas.toString());
                ChainReplicationInstance.this.callPredecessor();
            } catch (InterruptedException | KeeperException e) {
                System.out.println("Error in getChildrenInPath() or callPredecessor()");
            }
        };
    }

    void callPredecessor() throws InterruptedException, KeeperException {
        List<String> sortedReplicas = replicas.stream().sorted(Comparator.naturalOrder()).toList();

        isHead = sortedReplicas.get(0).equals(myReplicaName);
        isTail = sortedReplicas.get(sortedReplicas.size() - 1).equals(myReplicaName);

        // Call predecessor here
        if (!isHead) {
            int index = sortedReplicas.indexOf(myReplicaName);
            String predecessorReplicaName = sortedReplicas.get(index - 1);
//            TODO: figure out whether you should be adding a watch to the predecessor my understanding is that you should be, since when it leaves, you have to send a request to a new predecessor
            String data = new String(zk.getData(control_path + predecessorReplicaName, predecessorWatcher(), null));
            predecessorAddress = data.split("\n")[0];
            predecessorName = data.split("\n")[1];
        }

        var channel = this.createChannel(predecessorAddress);
        var stub = ReplicaGrpc.newBlockingStub(channel);
        var newSuccessorRequest = NewSuccessorRequest.newBuilder()
                .setLastZxidSeen(lastZxidSeen)
                .setLastXid(lastXid)
                .setLastAck(lastAck).build();
        var result = stub.newSuccessor(newSuccessorRequest);
        int rc = result.getRc();

        if (rc == -1) {
            //TODO: what should be the behaviour if this happens
        } else {
            lastXid = result.getLastXid();
            replicaState = (HashMap<String, Integer>) result.getStateMap();

            List<UpdateRequest> sent = result.getSentList();
            for (UpdateRequest request : sent) {
                String key = request.getKey();
                int newValue = request.getNewValue();
                int xid = request.getXid();
                pendingUpdateRequests.put(xid, new HashTableEntry(key, newValue));
            }
        }
    }

    public io.grpc.ManagedChannel createChannel(String serverAddress){
        var lastColon = serverAddress.lastIndexOf(':');
        var host = serverAddress.substring(0, lastColon);
        var port = Integer.parseInt(serverAddress.substring(lastColon+1));
        return ManagedChannelBuilder
                .forAddress(host, port)
                .usePlaintext()
                .build();
    }
}

class HashTableEntry {
    String key;
    int value;
    HashTableEntry(String key, int value) {
        this.key = key;
        this.value = value;
    }
}
