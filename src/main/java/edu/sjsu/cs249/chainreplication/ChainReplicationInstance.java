package edu.sjsu.cs249.chainreplication;

import edu.sjsu.cs249.chain.*;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.sql.Timestamp;
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
    long lastZxidSeen;
    int lastUpdateRequestXid;
    int lastAckXid;
    String myZNodeName;
    String predecessorAddress;
    String successorAddress;
    String successorZNode;
    boolean hasSuccessorContacted;
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
        lastAckXid = -1;
        successorAddress = "";
        successorZNode = "";
        pendingUpdateRequests = new HashMap<>();
        pendingHeadStreamObserver = new HashMap<>();
        replicaState = new HashMap<>();
        logs = new ArrayList<>();
        hasSuccessorContacted = false;
    }
    void start () throws IOException, InterruptedException, KeeperException {
        zk = new ZooKeeper(zookeeper_server_list, 10000, System.out::println);
        String pathName = zk.create(control_path + "/replica-", (grpcHostPort + "\n" + name).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        myZNodeName = pathName.replace(control_path + "/", "");
        addLog("Created znode name: " + myZNodeName);

        this.getChildrenInPath();
        this.callPredecessorAndSetSuccessorData();

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
            ChainReplicationInstance.this.addLog("childrenWatcher triggered");
            ChainReplicationInstance.this.addLog("WatchedEvent: " + watchedEvent.getType() + " on " + watchedEvent.getPath());
            try {
                ChainReplicationInstance.this.getChildrenInPath();
                ChainReplicationInstance.this.callPredecessorAndSetSuccessorData();
            } catch (InterruptedException | KeeperException e) {
                ChainReplicationInstance.this.addLog("Error getting children with getChildrenInPath()");
            }
        };
    }

    /**
     Triggers first time on initialize.
     Triggers when there is a change in children path.
     Set up a watcher on the path of there are any changes in children.
     */
    private void getChildrenInPath() throws InterruptedException, KeeperException {
        Stat replicaPath = new Stat();
        List<String> children = zk.getChildren(control_path, childrenWatcher(), replicaPath);
        lastZxidSeen = replicaPath.getPzxid();
        replicas = children.stream().filter(
                child -> child.contains("replica-")).toList();
        addLog("Current replicas: " + replicas +
                ", lastZxidSeen: " + lastZxidSeen);
    }

    /**
     Triggers first time on initialize.
     Triggers when there is a change in children path.
     */
    void callPredecessorAndSetSuccessorData() throws InterruptedException, KeeperException {
        List<String> sortedReplicas = replicas.stream().sorted(Comparator.naturalOrder()).toList();

        isHead = sortedReplicas.get(0).equals(myZNodeName);
        isTail = sortedReplicas.get(sortedReplicas.size() - 1).equals(myZNodeName);
        addLog("isHead: " + isHead + ", isTail: " + isTail);

        callPredecessor(sortedReplicas);
        setSuccessorData(sortedReplicas);
    }

    void callPredecessor(List<String> sortedReplicas) throws InterruptedException, KeeperException {
        //Don't need to call predecessor if you're head! Reset predecessor values
        if (isHead) {
            predecessorAddress = "";
            return;
        }

        int index = sortedReplicas.indexOf(myZNodeName);
        String predecessorReplicaName = sortedReplicas.get(index - 1);

//      Don't need to watch, since you're watching the children path, and it will trigger when predecessor goes
        String data = new String(zk.getData(control_path + "/" + predecessorReplicaName, false, null));

        String newPredecessorAddress = data.split("\n")[0];
        String newPredecessorName = data.split("\n")[1];

        // If last predecessor is  not the same as the last one, then call the new one!
        if (!newPredecessorAddress.equals(predecessorAddress)) {
            addLog("new predecessor");
            addLog("newPredecessorAddress: " + newPredecessorAddress);
            addLog("newPredecessorName: " + newPredecessorName);

            addLog("calling newSuccessor of new predecessor.");
            addLog("params:" +
                    ", lastZxidSeen: " + lastZxidSeen +
                    ", lastXid: " + lastUpdateRequestXid +
                    ", lastAck: " + lastAckXid +
                    ", myReplicaName: " + myZNodeName);

            predecessorAddress = newPredecessorAddress;
            var channel = this.createChannel(predecessorAddress);
            var stub = ReplicaGrpc.newBlockingStub(channel);
            var newSuccessorRequest = NewSuccessorRequest.newBuilder()
                    .setLastZxidSeen(lastZxidSeen)
                    .setLastXid(lastUpdateRequestXid)
                    .setLastAck(lastAckXid)
                    .setZnodeName(myZNodeName).build();
            var result = stub.newSuccessor(newSuccessorRequest);
            channel.shutdown();

            long rc = result.getRc();

            addLog("Response received");
            addLog("rc: " + rc);
            if (rc == -1) {
                this.getChildrenInPath();
                this.callPredecessorAndSetSuccessorData();
            } else if (rc == 0) {
                lastAckXid = result.getLastXid();
                addLog("lastAckXid: " + lastAckXid);
                addLog("state value:");
                for (String key : result.getStateMap().keySet()) {
                    replicaState.put(key, result.getStateMap().get(key));
                    addLog(key + ": " + result.getStateMap().get(key));
                }
                addPendingUpdateRequests(result);
            } else {
                lastAckXid = result.getLastXid();
                addLog("lastAckXid: " + lastAckXid);
                addPendingUpdateRequests(result);
            }
        }
    }

    void setSuccessorData(List<String> sortedReplicas) {
        if (isTail) {
            successorZNode = "";
            successorAddress = "";
            hasSuccessorContacted = false;
            return;
        }

        int index = sortedReplicas.indexOf(myZNodeName);
        String newSuccessorZNode = sortedReplicas.get(index + 1);

        // If the curr successor replica name matches the new one,
        // then hasSuccessorContacted should be the old value of hasSuccessorContacted
        // else it should be false
        if (!newSuccessorZNode.equals(successorZNode)) {
            successorZNode = newSuccessorZNode;
            hasSuccessorContacted = false;
            addLog("new successor");
            addLog("successorZNode: " + successorZNode);
        }
    }
    private void addPendingUpdateRequests(NewSuccessorResponse result) {
        List<UpdateRequest> sent = result.getSentList();
        addLog("sent requests: ");
        for (UpdateRequest request : sent) {
            String key = request.getKey();
            int newValue = request.getNewValue();
            int xid = request.getXid();
            pendingUpdateRequests.put(xid, new HashTableEntry(key, newValue));
            addLog("xid: " + xid + ", key: " + key + ", value: " + newValue);
        }

        if (isTail && pendingUpdateRequests.size() > 0) {
            addLog("I am tail, have to ack back all pending requests!");
            for (int xid: pendingUpdateRequests.keySet()) {
                ackXid(xid);
            }
        }
    }

    public void ackXid (int xid) {
        addLog("calling ack method of predecessor: " + predecessorAddress);
        lastAckXid = xid;
        pendingUpdateRequests.remove(xid);
        addLog("lastAckXid: " + lastAckXid);
        var channel = this.createChannel(this.predecessorAddress);
        var stub = ReplicaGrpc.newBlockingStub(channel);
        var ackRequest = AckRequest.newBuilder()
                .setXid(xid).build();
        stub.ack(ackRequest);
        channel.shutdown();

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

    public void addLog(String message) {
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        logs.add(timestamp + " " + message);
        System.out.println(timestamp + " " + message);
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
