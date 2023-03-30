import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

public class Node {
    public static void main(String[] args) throws IOException {
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


        // If my parent is not set, generate SEARCH message


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

                while (inputMessages.isEmpty()) {

                }
                //Since I am sending one message, I will expect one message back.
                String message = inputMessages.poll();
                boolean acceptSent = false;
                if (message.startsWith("TEST")) {
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
                            msgToSend = String.format(Messages.ACCEPT.name(), nodeMetaData.uid);
                        } else {
                            //send reject
                            msgToSend = String.format(Messages.REJECT.name(), nodeMetaData.uid);
                        }
                        recievedFrom.msgQueue.add(msgToSend);
                    }
                }
                while (inputMessages.isEmpty()) {

                }
                message = inputMessages.poll();
                //It will be either REJECT or ACCEPT, if reject, you are a non contender// what if 2 accepts? not possible?
                if (message.startsWith("ACCEPT")) {
                    //Send ADD node message
                    // Add a neighbor as part of tree
                    String[] messageSplit = message.split(",");
                    int msgUID = Integer.parseInt(messageSplit[1]);
                    NodeMetaData msgRcvdFromNode = getNodeFromID(nodeMetaData, msgUID);
                    msgRcvdFromNode.status = 1;
                    msgRcvdFromNode.msgQueue.add(String.format(format, ))

                } else if (message.startsWith("REJECT")) {
                    //I am a child node and now expect an ADD message or nothing at all.
                    //if I have sent an accept already, I expect a NULL or ADD else inform that you are not a contender and
                    //go to next phase
                    if (acceptSent) {
                        //expect a NULL or ADD else ignore and move on to next phase
                    }
                    //Send the synchronizer and remove your contendership
                }

            }

            //process messages you recieve
        }

        // Once parent is set, I just compute MWOE


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
