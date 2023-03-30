import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

public class Node {
    public static void main(String[] args) throws IOException, InterruptedException {
        //Uses the hostname to get the information about its neighbor nodes
        String hostName = InetAddress.getLocalHost().getHostName();
        String synchronizerHost = "localhost";//Change to dc01
        int syncport = 1993;
        String env = "";
        //args have to be passed, used for both localhost and prod based testing
        if (args.length == 2) {
            //environment is test
            env = args[0];
            if (env.equals("test")) {
                hostName = args[1];
            } else {
                printHelpMessage();
                return;
            }
        } else if (args.length == 1) {
            //environment is prod
            env = args[0];
            if (!env.equals("prod")) {
                printHelpMessage();
                return;
            }
        } else {
            printHelpMessage();
            return;
        }

        ConfigurationReader configurationReader = new ConfigurationReader();
        String configFileName = "configuration";
        NetworkInformation networkInformation;


        networkInformation = configurationReader.readConfiguration(hostName, configFileName, env);
        NodeMetaData nodeMetaData = networkInformation.nodeMetaData;
        Queue<String> inputMessages = new LinkedList<>();

        nodeMetaData.neighbors.forEach(neighbor -> {
            neighbor.msgQueue = new LinkedList<>();
        });


        Socket synchronizerSocket = null;
        synchronizerSocket = new Socket(synchronizerHost, syncport);
        DataOutputStream synchronizerOutputStream = new DataOutputStream(synchronizerSocket.getOutputStream());
        DataInputStream synchronizerInputStream = new DataInputStream(synchronizerSocket.getInputStream());
        int phase = -1;
        synchronizerOutputStream.writeInt(Messages.ENQUIRY.name().length());
        synchronizerOutputStream.writeBytes(Messages.ENQUIRY.name());
        int respLength = synchronizerInputStream.readInt();
        if (respLength > 0) {
            byte[] line = new byte[respLength];
            synchronizerInputStream.readFully(line);
            String msgResp = new String(line);
            phase = Integer.parseInt(msgResp);
        }
        System.out.println("Phase is " + phase);
        String format = "%s,%s";

        System.out.println(String.format("Node host: %s, Node UID: %d, Node url:%s, Node port: %d, Neighbor UIDs: %s",
                networkInformation.nodeMetaData.host, networkInformation.nodeMetaData.uid,
                networkInformation.nodeMetaData.hostUrl, networkInformation.nodeMetaData.port,
                getNeighborUIDs(networkInformation.nodeMetaData.neighborUIDsAndWeights.keySet().stream().toList())));

        ListenerThread _lth = new ListenerThread(networkInformation, inputMessages);

        Thread listenerThread = new Thread(_lth);
        listenerThread.start();

        networkInformation.nodeMetaData.neighbors.forEach(neighbor -> {
            SenderThread senderThread = new SenderThread(neighbor);
            new Thread(senderThread).start();
        });

        boolean allNodesNotConnected = true;
        while (allNodesNotConnected) {
            boolean allCon = true;
            for (NodeMetaData neigh : nodeMetaData.neighbors) {
                if (!neigh.isConnected.get()) {
                    allCon = false;
                }
            }
            allNodesNotConnected = !allCon;
            Thread.sleep(100);
        }
        System.out.println("All nodes are connected, waiting to stabilize");
        Thread.sleep(5000);

        /*
         *  if you recieve search message and the component id is different than urs, and if you recieve search message
         *  from the same component to which you sent serach, and if your UID is bigger send reject message to that SEARCH
         *  but if your UID is smaller than send accept if that is your MWOE
         * */

        while (nodeMetaData.parentUID == -1) {
            /*
             * Just send TEST message to shortest distance neighbor
             * */

            if (phase == 0) {
                AtomicInteger smallestDistNeighbor = new AtomicInteger(Integer.MAX_VALUE);
                AtomicInteger smallestDistNeighborUID = new AtomicInteger(Integer.MAX_VALUE); // this should not be UID, its okay here
                nodeMetaData.neighborUIDsAndWeights.keySet().forEach(key -> {
                    if (smallestDistNeighbor.get() < nodeMetaData.neighborUIDsAndWeights.get(key)
                            || (smallestDistNeighbor.get() == nodeMetaData.neighborUIDsAndWeights.get(key) && key < smallestDistNeighborUID.get())) {
                        smallestDistNeighbor.set(nodeMetaData.neighborUIDsAndWeights.get(key));
                        smallestDistNeighborUID.set(key);
                    }
                });
                //Sends TEST message to shortest neighbor
                NodeMetaData shortestDistNeighbor = getNodeFromID(nodeMetaData, smallestDistNeighborUID.get());
                if (shortestDistNeighbor == null) {
                    System.out.println("Fatal Error, node doesnt match neighbor UID");
                    System.exit(-1);
                }
                shortestDistNeighbor.msgQueue.add(String.format(format, Messages.TEST, nodeMetaData.uid));
                System.out.println(String.format("Shortest neighbor node is with UID: %d", smallestDistNeighborUID.get()));

                while (inputMessages.isEmpty()) {
                }
                //Since I am sending one message, I will expect one message back.
                String message = inputMessages.poll();
                boolean acceptSent = false;
                System.out.println(String.format("Msg recieved is %s", message));
                if (message.startsWith(Messages.TEST.value)) {
                    //If my UID is smaller and its my shortest edge, send accept.
                    //Check my edges, if the test is my smallest neighbor and my UID is smaller, send ACCEPT
                    //Check my edges, if the test is my smallest neight and my UID is bigger, send REJECT
                    String[] messageSplit = message.split(",");
                    int messageUID = Integer.parseInt(messageSplit[1]);
                    NodeMetaData recievedFrom = getNodeFromID(nodeMetaData, messageUID);
                    if (messageUID == shortestDistNeighbor.uid) {
                        String msgToSend = "";
                        if (nodeMetaData.uid < messageUID) {
                            //send accept
                            msgToSend = String.format(Messages.ACCEPT.value, nodeMetaData.uid);
                        } else {
                            //send reject
                            msgToSend = String.format(Messages.REJECT.value, nodeMetaData.uid);
                        }
                        recievedFrom.msgQueue.add(msgToSend);
                    }
                }
                while (inputMessages.isEmpty()) {
                }
                message = inputMessages.poll();
                System.out.println(String.format("Msg recieved is %s", message));
                //It will be either REJECT or ACCEPT, if reject, you are a non contender
                if (message.startsWith(Messages.ACCEPT.value)) {
                    //Send ADD node message
                    // Add a neighbor as part of tree
                    String[] messageSplit = message.split(",");
                    int msgUID = Integer.parseInt(messageSplit[1]);
                    NodeMetaData msgRcvdFromNode = getNodeFromID(nodeMetaData, msgUID);
                    msgRcvdFromNode.status = 1;
                    msgRcvdFromNode.msgQueue.add(String.format(format, Messages.ADD.value));
                    String msgToSend = Messages.COMPLETE_CONTENDER.value;
                    synchronizerOutputStream.writeInt(msgToSend.length());
                    synchronizerOutputStream.writeBytes(msgToSend);
                } else if (message.startsWith(Messages.REJECT.value)) {
                    //I am a child node and now expect an ADD message or nothing at all.
                    //if I have sent an accept already, I expect a NULL or ADD else inform that you are not a contender and
                    //go to next phase
                    if (acceptSent) {
                        while (inputMessages.isEmpty()) {
                        }
                        message = inputMessages.poll();
                        System.out.println(String.format("Msg recieved is %s", message));
                        if (message.startsWith(Messages.ADD.value)) {
                            String[] messageSplit = message.split(",");
                            int messageUID = Integer.parseInt(messageSplit[1]);
                            nodeMetaData.parentUID = messageUID;
                            NodeMetaData parentNode = getNodeFromID(nodeMetaData, nodeMetaData.parentUID);
                            parentNode.status = 0;
                            System.out.println("Added node " + parentNode.uid + " as parent");
                        }

                    }
                    //Send the synchronizer and remove your contendership
                }

            }
            //
        }

        // Once parent is set, I just compute MWOE


    }

    /***
     * Get the UID of all the neighbor nodes
     * @param neighborUIDs
     * @return
     */
    private static String getNeighborUIDs(List<Integer> neighborUIDs) {
        StringBuilder builder = new StringBuilder();
        neighborUIDs.forEach(uid -> {
            builder.append(uid + " ");
        });
        return builder.toString();
    }

    static NodeMetaData getNodeFromID(NodeMetaData nodeMetaData, int uid) {
        NodeMetaData shortestDistNeighbor = nodeMetaData.neighbors.stream()
                .filter(nodeMetaData1 -> nodeMetaData1.uid == uid)
                .findFirst().orElse(null);
        return shortestDistNeighbor;
    }

    /**
     * Used to print help message
     */
    static void printHelpMessage() {
        System.out.println("The system needs some parameters to function");
        System.out.println();
        System.out.println("If the environment is test, pass 3 args in the format \"java Node test dc01.utdallas.edu\"");
        System.out.println("the first arg will be the environment, second will be the id of the node to be assumed from config file as all systems run on localhost while development");
        System.out.println("The third argument indicates if the node is initiator or not opts true or false");
        System.out.println();
        System.out.println("If the environment is production, pass 2 args in the format \"java Node prod\"");
    }
}
