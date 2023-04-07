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
import java.util.ArrayList;
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
    ArrayList <String> logs;

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
        logs = new ArrayList<>();
    }
    void start () throws IOException, InterruptedException, KeeperException {
        zk = new ZooKeeper(zookeeper_server_list, 10000, System.out::println);
        String pathName = zk.create(control_path + "/replica-", (grpcHostPort + "\n" + name).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        myReplicaName = pathName.replace(control_path + "/", "");
        logs.add("Created znode name: " + myReplicaName);

        this.getChildrenInPath();
        this.callPredecessor();

        HeadChainReplicaGRPCServer headChainReplicaGRPCServer = new HeadChainReplicaGRPCServer(this);
        TailChainReplicaGRPCServer tailChainReplicaGRPCServer = new TailChainReplicaGRPCServer(this);
        ReplicaGRPCServer replicaGRPCServer = new ReplicaGRPCServer(this);
        ChainDebugInstance chainDebugInstance = new ChainDebugInstance(this);

        Server server = ServerBuilder.forPort(Integer.parseInt(grpcHostPort.split(":")[1]))
                .addService(headChainReplicaGRPCServer)
                .addService(tailChainReplicaGRPCServer)
                .addService(replicaGRPCServer)
                .addService(chainDebugInstance)
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
                ChainReplicationInstance.this.callPredecessor();
            } catch (InterruptedException | KeeperException e) {
                System.out.println("Error getting children with getChildrenInPath()");
            }
        };
    }

    /**
     Triggers first time on initialize.
     Triggers when there is a change in children path.
     Set up a watcher on the path of there are any changes in children.
     */
    private void getChildrenInPath() throws InterruptedException, KeeperException {
        List<String> children = zk.getChildren(control_path, childrenWatcher());
        replicas = children.stream().filter(
                child -> child.contains("replica-")).toList();
        System.out.println(replicas);
        logs.add("Current replicas: " + replicas);
    }

    /**
     Triggers first time on initialize.
     Triggers when there is a change in children path.
     */
    void callPredecessor() throws InterruptedException, KeeperException {
        List<String> sortedReplicas = replicas.stream().sorted(Comparator.naturalOrder()).toList();

        isHead = sortedReplicas.get(0).equals(myReplicaName);
        isTail = sortedReplicas.get(sortedReplicas.size() - 1).equals(myReplicaName);

        //Don't need to call predecessor if you're head! Reset predecessor values
        if (isHead) {
            predecessorName = "";
            predecessorAddress = "";
            return;
        }

        int index = sortedReplicas.indexOf(myReplicaName);
        String predecessorReplicaName = sortedReplicas.get(index - 1);

//      TODO: figure out whether you should be adding a watch to the predecessor. Don't need, since you're watching the children path and updating predecessor when necessary
        String data = new String(zk.getData(control_path + "/" + predecessorReplicaName, false, null));

        String newPredecessorAddress = data.split("\n")[0];
        String newPredecessorName = data.split("\n")[1];

        // If last predecessor is same as the current one, then don't call!
        if (newPredecessorAddress.equals(predecessorAddress)) return;

        predecessorAddress = newPredecessorAddress;
        predecessorName = newPredecessorName;
        var channel = this.createChannel(predecessorAddress);
        var stub = ReplicaGrpc.newBlockingStub(channel);
        var newSuccessorRequest = NewSuccessorRequest.newBuilder()
                .setLastZxidSeen(lastZxidSeen)
                .setLastXid(lastXid)
                .setLastAck(lastAck)
                .setZnodeName(myReplicaName).build();
        var result = stub.newSuccessor(newSuccessorRequest);
        int rc = result.getRc();

        if (rc == -1) {
            //TODO: what should be the behaviour if this happens
        } else {
            lastXid = result.getLastXid();
            for (String key: result.getStateMap().keySet()){
                replicaState.put(key, result.getStateMap().get(key));
            }
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
