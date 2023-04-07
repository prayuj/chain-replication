package edu.sjsu.cs249.chainreplication;

import edu.sjsu.cs249.chain.HeadResponse;
import edu.sjsu.cs249.chain.NewSuccessorRequest;
import edu.sjsu.cs249.chain.ReplicaGrpc;
import edu.sjsu.cs249.chain.UpdateRequest;
import io.grpc.ManagedChannelBuilder;
import io.grpc.ManagedChannel;
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
    int lastUpdateRequestXid;
    int lastProcessedXid;
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
        lastUpdateRequestXid = -1;
        lastProcessedXid = -1;
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
            ChainReplicationInstance.this.logs.add("childrenWatcher triggered");
            System.out.println("In childrenWatcher");
            System.out.println("WatchedEvent: " + watchedEvent.getType() + " on " + watchedEvent.getPath());
            ChainReplicationInstance.this.logs.add("WatchedEvent: " + watchedEvent.getType() + " on " + watchedEvent.getPath());
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
        logs.add("isHead: " + isHead + ", isTail: " + isTail);

        //Don't need to call predecessor if you're head! Reset predecessor values
        if (isHead) {
            predecessorName = "";
            predecessorAddress = "";
            return;
        }

        int index = sortedReplicas.indexOf(myReplicaName);
        String predecessorReplicaName = sortedReplicas.get(index - 1);

//      Don't need to watch, since you're watching the children path, and it will trigger when predecessor goes
        String data = new String(zk.getData(control_path + "/" + predecessorReplicaName, false, null));

        String newPredecessorAddress = data.split("\n")[0];
        String newPredecessorName = data.split("\n")[1];

        // If last predecessor is same as the current one, then don't call!
        if (newPredecessorAddress.equals(predecessorAddress)) return;

        logs.add("new predecessor");
        logs.add("newPredecessorAddress: " + newPredecessorAddress);
        logs.add("newPredecessorName: " + newPredecessorName);

        logs.add("calling newSuccessor of new predecessor.");
        logs.add("params:" +
                ", lastZxidSeen: " + lastZxidSeen +
                ", lastXid: " + lastUpdateRequestXid +
                ", lastAck: " + lastAck +
                ", myReplicaName: " + myReplicaName);

        predecessorAddress = newPredecessorAddress;
        predecessorName = newPredecessorName;
        var channel = this.createChannel(predecessorAddress);
        var stub = ReplicaGrpc.newBlockingStub(channel);
        var newSuccessorRequest = NewSuccessorRequest.newBuilder()
                .setLastZxidSeen(lastZxidSeen)
                .setLastXid(lastUpdateRequestXid)
                .setLastAck(lastAck)
                .setZnodeName(myReplicaName).build();
        var result = stub.newSuccessor(newSuccessorRequest);
        int rc = result.getRc();

        logs.add("Response received");
        logs.add("rc: " + rc);
        if (rc == -1) {
            //TODO: what should be the behaviour if this happens
        } else {
            lastUpdateRequestXid = result.getLastXid();
            logs.add("lastXid: " + lastUpdateRequestXid);
            logs.add("state value:");
            for (String key: result.getStateMap().keySet()){
                replicaState.put(key, result.getStateMap().get(key));
                logs.add(key + ": " + result.getStateMap().get(key));
            }
            List<UpdateRequest> sent = result.getSentList();
            logs.add("sent requests: ");
            for (UpdateRequest request : sent) {
                String key = request.getKey();
                int newValue = request.getNewValue();
                int xid = request.getXid();
                pendingUpdateRequests.put(xid, new HashTableEntry(key, newValue));
                logs.add("xid: " + xid + ", key: " + key + ", value: " + newValue);
            }
        }
    }

    public ManagedChannel createChannel(String serverAddress){
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
