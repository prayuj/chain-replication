package edu.sjsu.cs249.chainreplication;

import picocli.CommandLine;
import java.util.concurrent.Callable;

public class Main {
    public static void main(String[] args) {
        System.exit(new CommandLine(new ServerCli()).execute(args));
    }

    static class ServerCli implements Callable<Integer> {


        @CommandLine.Parameters(index = "0", description = "your name.")
        String name;

        @CommandLine.Parameters(index = "1", description = "grpc host:port to hit.")
        String grpcHostPort;

        @CommandLine.Parameters(index = "2", description = "zookeeper server to interact with.")
        String zookeeper_server_list;

        @CommandLine.Parameters(index = "3", description = "specify a node in zookeeper where sequential nodes will be created")
        String control_path;

        @Override
        public Integer call() throws Exception {
            new ChainReplicationInstance(name, grpcHostPort, zookeeper_server_list, control_path).start();
            return 0;
        }
    }

}