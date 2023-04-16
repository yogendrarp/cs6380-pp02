import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class Node {
    static String SYNCHRONIZER_HOST = "localhost"; //Change to dc01
    static int SYNCPORT = 1993;

    public static void main(String[] args) throws IOException, InterruptedException {
        //Uses the hostname to get the information about its neighbor nodes
        String hostName = InetAddress.getLocalHost().getHostName();
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
        String configFileName = "configuration.txt";
        NetworkInformation networkInformation;

        System.out.println(hostName + "," + configFileName + "," + env);
        networkInformation = configurationReader.readConfiguration(hostName, configFileName, env);
        NodeMetaData nodeMetaData = networkInformation.nodeMetaData;
        Queue<String> inputMessages = new LinkedList<>();

        nodeMetaData.neighbors.forEach(neighbor -> {
            neighbor.msgQueue = new LinkedList<>();
        });


        int phase = synchronizerMessenger(Messages.ENQUIRY.value);

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
            Thread.sleep(2000);
        }
        System.out.println("All nodes are connected, waiting to stabilize");
        Thread.sleep(10000);



        /*
         *  if you recieve search message and the component id is different than urs, and if you recieve search message
         *  from the same component to which you sent serach, and if your UID is bigger send reject message to that SEARCH
         *  but if your UID is smaller than send accept if that is your MWOE
         * */

        if (nodeMetaData.parentUID == -1) {
            /*
             * Just send TEST message to shortest distance neighbor
             * */
            boolean phaseZeroCompleted = false;
            int smallestDistNeighborUID = getSmallestDistNodeNeighborUID(nodeMetaData, nodeMetaData.parentUID);

            //Sends TEST message to shortest neighbor
            NodeMetaData shortestDistNeighbor = getNodeFromID(nodeMetaData, smallestDistNeighborUID);
            if (shortestDistNeighbor == null) {
                System.out.println("Fatal Error, node doesn't match neighbor UID");
                System.exit(-1);
            }
            shortestDistNeighbor.msgQueue.add(String.format(format, Messages.TEST, nodeMetaData.uid));
            System.out.println(String.format("Shortest neighbor node is with UID: %d", smallestDistNeighborUID));


            while (!phaseZeroCompleted) {
                Thread.sleep(getRand() * 1000L);
                int tempPhase = synchronizerMessenger(Messages.ENQUIRY.value);

                if (tempPhase > phase) {
                    phase = tempPhase;
                    phaseZeroCompleted = true;
                    continue;
                }

                if (inputMessages.size() <= 0) {
                    Thread.sleep(getRand() * 100L);
                    continue;
                }


                String message = inputMessages.poll();
                Thread.sleep(getRand() * 500L);
                System.out.printf("Msg recieved is %s%n", message);

                if (message.startsWith(Messages.TEST.value)) {
                    //If my UID is smaller and its my shortest edge, send accept.
                    //Check my edges, if the test is my smallest neighbor and my UID is smaller, send ACCEPT
                    //Check my edges, if the test is my smallest weight and my UID is bigger, send REJECT
                    String[] messageSplit = message.split(",");
                    int messageUID = Integer.parseInt(messageSplit[1]);
                    NodeMetaData recievedFrom = getNodeFromID(nodeMetaData, messageUID);
                    String msgToSend = "";

                    if (messageUID == shortestDistNeighbor.uid) {
                        if (nodeMetaData.uid < messageUID) {
                            //send accept
                            msgToSend = String.format(format, Messages.ACCEPT.value, nodeMetaData.uid);
                            //Should I send remove contendership from here?
                        } else {
                            //send reject
                            msgToSend = String.format(format, Messages.REJECT.value, nodeMetaData.uid);
                        }
                    } else {
                        msgToSend = String.format(format, Messages.REJECTNOTMWOE.value, nodeMetaData.uid);
                    }
                    System.out.println("Message to send is " + msgToSend + " to node with UID " + recievedFrom.uid);
                    recievedFrom.msgQueue.add(msgToSend);

                } else if (message.startsWith(Messages.ACCEPT.value)) {
                    //Send ADD node message
                    // Add a neighbor as part of tree
                    String[] messageSplit = message.split(",");
                    int msgUID = Integer.parseInt(messageSplit[1]);
                    NodeMetaData msgRcvdFromNode = getNodeFromID(nodeMetaData, msgUID);
                    msgRcvdFromNode.status = 1;
                    nodeMetaData.level += 1;
                    System.out.println("Sending ADD message");
                    msgRcvdFromNode.msgQueue.add(String.format(format, Messages.ADD.value, nodeMetaData.uid));
                    System.out.println("Keeping my contendership");
                    synchronizerMessenger(Messages.COMPLETE_CONTENDER.value);
                } else if (message.startsWith(Messages.REJECTNOTMWOE.value)) {
                    //Send the synchronizer and remove your contendership
                    System.out.println("Keeping my contendership");
                    synchronizerMessenger(Messages.COMPLETE_CONTENDER.value);
                } else if (message.startsWith(Messages.ADD.value)) {

                    String[] messageSplit = message.split(",");
                    int messageUID = Integer.parseInt(messageSplit[1]);
                    System.out.println("Adding " + messageUID + " parent node");
                    nodeMetaData.parentUID = messageUID;
                    nodeMetaData.leaderUID = messageUID;
                    nodeMetaData.level += 1;
                    NodeMetaData parentNode = getNodeFromID(nodeMetaData, nodeMetaData.parentUID);
                    parentNode.status = 0;
                    System.out.println("Added node " + parentNode.uid + " as parent");
                    System.out.println("Removing my contendership");
                    synchronizerMessenger(Messages.COMPLETE_NONCONTENDER.value);
                } else if (message.startsWith(Messages.REJECT.value)) {
                    //Do nothing
                    System.out.printf("Reject recieved %s%n", message);
                } else {
                    System.out.printf("Junk message:%s", message);
                }
            }

            System.out.println("Phase 0 completed");
            System.out.printf("Node %d with parent node as %d and component leader as %d%n", nodeMetaData.uid, nodeMetaData.parentUID, nodeMetaData.leaderUID);
        }


        Thread.sleep(getRand() * 1000L);
        System.out.println("*********************************************************************************");
        Thread.sleep(getRand() * 800L);
        boolean mfsTreeConstructed = false;
        while (!mfsTreeConstructed) {


            //find a way to mark tree construction
            //if a node recieves NULL from all its neighbors as a response of search,
            //A node whose parent is set, wont generate search message, it will forward search or will send a test message
            // Secondly, a node recieves only one search message but multiple test messages, if component UID on test
            //message is greater than the component it belongs to it sends a CHECK message to its parent which
            // can be directly the leader or can be a parent in the tree
            if (nodeMetaData.uid == nodeMetaData.leaderUID) {
                System.out.println("MFS not constructed but I am currently a leader " + nodeMetaData.uid);
            } else {
                System.out.printf("%d uid is part of some component %d%n", nodeMetaData.uid, nodeMetaData.parentUID);
            }
            System.out.printf("Current phase is %d%n", phase);
            boolean phaseCompleted = false;
            HashMap<Integer, Integer> responseHashMap = new HashMap<>();
            nodeMetaData.neighbors.forEach(nw -> responseHashMap.put(nw.uid, -1));
            int currentknownMinWeightEdge = Integer.MAX_VALUE;
            String currentKnownMinWeighString = "";
            //1->expecting a reply, 0->not expecting a reply or all replies recieved, if all are 0. Done with the phase, -1 not yet sent
            //contender node generate search to neighbors part of tree and TEST to non
            if (nodeMetaData.parentUID == -1) {
                System.out.println("Evaluating to TEST message");
                boolean areAllNodesUnmarked =
                        nodeMetaData.neighbors.stream().filter(nm -> nm.status == -1).count()
                                == nodeMetaData.neighbors.size();
                if (areAllNodesUnmarked) {
                    //Send test message only by computing MWOE like above
                    //non MWOE edges are marked 0, MWOE edge marked 1
                    System.out.println("All nodes are unmarked, sending TEST message to smallest UID");
                    int smallestdistNeighborUID = getSmallestDistNodeNeighborUID(nodeMetaData, nodeMetaData.parentUID);
                    NodeMetaData neighborNode = getNodeFromID(nodeMetaData, smallestdistNeighborUID);
                    neighborNode.msgQueue.add(String.format("%s,%s,%s,%s", Messages.TEST, nodeMetaData.uid,
                            nodeMetaData.neighborUIDsAndWeights.get(smallestdistNeighborUID), neighborNode.uid));
                    responseHashMap.keySet().forEach(key -> {
                        responseHashMap.put(key, 0);
                    });
                    responseHashMap.put(smallestdistNeighborUID, 1);
                } else {
                    //Send search across neighbor node and set status to 1
                    nodeMetaData.neighbors.stream().filter(neigh -> neigh.status == 1).forEach(nmd -> {
                        System.out.println("Sending SEARCH message to " + nmd.uid);
                        nmd.msgQueue.add(String.format(format, Messages.SEARCH.value, nodeMetaData.uid));
                        responseHashMap.put(nmd.uid, 1);
                    });
                }
            }

            boolean myPhaseDone = false;
            while (!phaseCompleted) {
                Thread.sleep(500);
                int tempPhase = synchronizerMessenger(Messages.ENQUIRY.value);
                if (tempPhase > phase) {
                    phase = tempPhase;
                    Thread.sleep(10000);
                    break;
                }
                Thread.sleep(5000);

                if (inputMessages.isEmpty()) {
                    Thread.sleep(1000);
                    System.out.println("wait....");
                    continue;
                }
                //how will a child node deal with all responses like the above hashmap
                // a component leader can recieve a test message, when will this happen, only if the other incoming neighbor distance is greater
                String message = inputMessages.poll();
                System.out.println("Message received is " + message);
                int myknownminedge = Integer.MAX_VALUE;
                if (message.startsWith(Messages.TEST.value)) {
                    //******This might change entirely
                    ////TEST, WEIGHT,COMPONENTUID path....
                    //Change everything down
                    // from test message, last is my UID, remove it veryify, if that UID is my uid and i am parent
                    // accept the milana
                    // fatal crash if its not my uid
                    // peek last uid, send to that node
                    String[] msgSplist = message.split(",");
                    if (nodeMetaData.leaderUID == nodeMetaData.uid) {
                        System.out.println("Reached 4");
                        int smallestWeightEdge = Integer.parseInt(msgSplist[2]);
                        int otherComponentUID = Integer.parseInt(msgSplist[1]);
                        if (otherComponentUID == nodeMetaData.leaderUID) {
                            NodeMetaData otherComponent = getNodeFromID(nodeMetaData, otherComponentUID);
                            String msgToSend = String.format("%s,%s", Messages.SAMECOMPONENT.value, nodeMetaData.uid);
                            System.out.printf("Msg to send %s%n", msgToSend);
                            otherComponent.msgQueue.add(msgToSend);
                        } else if (smallestWeightEdge <= myknownminedge && nodeMetaData.uid < otherComponentUID && !myPhaseDone) {
                            //Deal with currentknownminstring
                            System.out.println("Reached 5");
                            //if my UID is smaller, I accept
                            String acceptMsg = rejectionOrAcceptMsgBuilder(Messages.ACCEPT.value, msgSplist);
                            String lastest = acceptMsg.substring(0, acceptMsg.lastIndexOf(","));
                            System.out.printf("Deduced string %s%n", lastest);
                            int lastNode = Integer.parseInt(lastest.substring(1 + lastest.lastIndexOf(",")));
                            //acceptMsg = acceptMsg + "," + nodeMetaData.uid;
                            //Change my child as my parent, update my leader UID, remove my contendership done in reject
                            System.out.printf("1.Message to send %s%n", acceptMsg); // TODO: 4/4/2023 compare 326 and 330 are they same?
                            System.out.printf("othercomponent UID is %d and lastnode is %d%n", otherComponentUID, lastNode);
                            nodeMetaData.level++;
                            nodeMetaData.leaderUID = otherComponentUID;
                            nodeMetaData.parentUID = lastNode;
                            NodeMetaData parentNode = getNodeFromID(nodeMetaData, lastNode);
                            parentNode.status = 0;
                            parentNode.msgQueue.add(acceptMsg);
                            System.out.printf("Added node %d as my parent and %d as my component leader %n",
                                    nodeMetaData.parentUID, nodeMetaData.leaderUID);
                            myPhaseDone = true;
                            System.out.println("**** Removing my contendership");
                            synchronizerMessenger(Messages.COMPLETE_NONCONTENDER.value);
                        } else {
                            //reject
                            String rejectMsg = rejectionOrAcceptMsgBuilder(Messages.REJECT.value, msgSplist);
                            String[] rejSplit = rejectMsg.split(",");
                            System.out.println(rejectMsg); // TODO: 4/4/2023

                            int lastNode = Integer.parseInt(rejectMsg.substring(rejectMsg.lastIndexOf(",") + 1));
                            NodeMetaData nodeTosend = null;
                            if (lastNode == nodeMetaData.uid) {
                                System.out.println("Direct message, send to first node");
                                nodeTosend = getNodeFromID(nodeMetaData, Integer.parseInt(rejSplit[1]));
                            } else{
                                nodeTosend = getNodeFromID(nodeMetaData, lastNode);
                            }
                            System.out.printf("2.Msg to send %s%n", rejectMsg);
                            nodeTosend.msgQueue.add(rejectMsg);
                        }


                    } else {
                        System.out.println("Reached 6");
                        //What if I am the last node, send to my parent append its UID
                        //TEST,COMPONENTUID, WEIGHT, path....
                        int smallestWeightEdge = Integer.parseInt(msgSplist[2]);
                        int otherComponentUID = Integer.parseInt(msgSplist[1]);
                        if (otherComponentUID == nodeMetaData.leaderUID) {
                            NodeMetaData otherComponent = getNodeFromID(nodeMetaData, otherComponentUID);
                            String msgToSend = String.format("%s,%s", Messages.SAMECOMPONENT.value, nodeMetaData.uid);
                            System.out.printf("3.Message to send %s%n", message);
                            otherComponent.msgQueue.add(msgToSend);
                            continue;
                        }
                        message = message + "," + nodeMetaData.parentUID;
                        System.out.printf("4.Message to send %s%n", message);
                        NodeMetaData parentNode = getNodeFromID(nodeMetaData, nodeMetaData.parentUID);
                        parentNode.msgQueue.add(message);
                    }
                } else if (message.startsWith(Messages.SEARCH.value)) {
                    //If all my edges are not marked, send my parent the minimum of 3
                    // else forward search to marked nodes
                    // if no nodes are -1, send COMPLETION
                    System.out.println("Received Search message from " + message);
                    long unmarkedEdges = nodeMetaData.neighbors.stream().filter(nm -> nm.status == -1).count();
                    if (unmarkedEdges == nodeMetaData.neighbors.size() - 1) {
                        int shortestdistNeighborUID = getSmallestDistNodeNeighborUID(nodeMetaData, nodeMetaData.parentUID);
                        int weightOfIt = nodeMetaData.neighborUIDsAndWeights.get(shortestdistNeighborUID);
                        //MIN_EDGE,WEIGHT,MYUID,UIDOFOTHERCOMPONENT, PARENT WILL APPEND ITS
                        String msgToSendParent = String.format("%s,%s,%s,%s", Messages.MIN_EDGE.value, weightOfIt,
                                nodeMetaData.uid, shortestdistNeighborUID);
                        NodeMetaData parentNode = getNodeFromID(nodeMetaData, nodeMetaData.parentUID);
                        System.out.printf("5.Message to send %s%n", msgToSendParent);
                        parentNode.msgQueue.add(msgToSendParent);
                    } else if (unmarkedEdges == 0) {
                        String msgToSendParent = String.format("%s,%s", Messages.COMPLETED, nodeMetaData.uid);
                        NodeMetaData parentNode = getNodeFromID(nodeMetaData, nodeMetaData.parentUID);
                        parentNode.msgQueue.add(msgToSendParent);
                    } else {
                        final String _msg = message;
                        System.out.printf("6.Message to send %s%n", _msg);
                        nodeMetaData.neighbors.stream().filter(nm -> nm.status == 1).forEach(nm -> {
                            nm.msgQueue.add(_msg);
                        });
                    }
                } else if (message.startsWith(Messages.MIN_EDGE.value)) {
                    //What if I am the parent?
                    //Must change this logic?
                    if(nodeMetaData.leaderUID==nodeMetaData.uid && myPhaseDone) {
                        continue;
                    }
                    String[] msgSplits = message.split(",");
                    int minWeight = Integer.parseInt(msgSplits[1]);
                    int responseFrom = Integer.parseInt(msgSplits[2]);
                    if (minWeight < currentknownMinWeightEdge) {
                        currentknownMinWeightEdge = minWeight;
                        currentKnownMinWeighString = message;
                    }
                    responseHashMap.put(responseFrom, 0);
                    AtomicBoolean allresponse = new AtomicBoolean(true);
                    responseHashMap.keySet().forEach(key -> {
                        if (responseHashMap.get(key) == 1) {
                            allresponse.set(false);
                        }
                    });
                    //All nodes have responded, but check if there was one or more immediate neighbor
                    if (allresponse.get()) {
                        //check if an direct neighbor 1 or more is present and check their weights
                        List<Integer> immediate = responseHashMap.keySet().stream().filter(key -> responseHashMap.get(key) == -1).collect(Collectors.toList());
                        for (int i = 0; i < immediate.size(); i++) {
                            int key = immediate.get(i);
                            if (currentknownMinWeightEdge > nodeMetaData.neighborUIDsAndWeights.get(key)) {
                                currentknownMinWeightEdge = nodeMetaData.neighborUIDsAndWeights.get(key);
                                //here I might have to recreate the entire message, if so
                                //MIN_EDGE, WEIGHT,MYUID, CONNECTINGUID .. not adding my UID here, will add it at bottom
                                currentKnownMinWeighString = Messages.MIN_EDGE.value + "," + currentknownMinWeightEdge + "," + key;     //String.format(format, Messages.MIN_EDGE, key);
                            }
                        }
                        if (nodeMetaData.leaderUID == nodeMetaData.uid) {
                            //Take decision here
                            //Each node sends 1 TEST message, but might recieve multiple test messages.
                            //Min edge message has a trace of the path
                            //MIN_EDGE,WEIGHT,SHORTEDGE,ITSPARENT,ITSPARENTSPARENT
                            //TEST,COMPONENTUID, WEIGHT, path....
                            String testMessage = Messages.TEST.value + "," + nodeMetaData.uid +
                                    "," + currentKnownMinWeighString.substring(1 + currentKnownMinWeighString.indexOf(","));
                            System.out.println("currentknowMinweightstr " + currentKnownMinWeighString);
                            System.out.println("Some sunstring : " + currentKnownMinWeighString.substring(1 + currentKnownMinWeighString.indexOf(",")));

                            System.out.println("Test message generated is " + testMessage);
                            int nextNodeId = Integer.parseInt(testMessage.split(",")[3]);
                            NodeMetaData nextNode = getNodeFromID(nodeMetaData, nextNodeId);
                            nextNode.msgQueue.add(testMessage);
                            myknownminedge = currentknownMinWeightEdge;
                        } else {
                            //Send min edge to parent but add my info last but wight
                            String[] splitsOFCurrentMINED = currentKnownMinWeighString.split(",");
                            StringBuilder builder = new StringBuilder();
                            for (int i = 0; i < splitsOFCurrentMINED.length; i++) {
                                if (i == 2) {
                                    builder.append(nodeMetaData.uid).append(","); //be aware of this ','
                                } else if (i == splitsOFCurrentMINED.length - 1) {
                                    builder.append(splitsOFCurrentMINED[i]);
                                } else {
                                    builder.append(splitsOFCurrentMINED[i]).append(",");
                                }
                            }
                            String updatedMinEdgemsg = builder.toString();
                            System.out.println("Update MIN eedge is " + updatedMinEdgemsg);
                            NodeMetaData parent = getNodeFromID(nodeMetaData, nodeMetaData.parentUID);
                            parent.msgQueue.add(updatedMinEdgemsg);
                        }
                    }
                } else if (message.startsWith(Messages.REJECT.value)) {
                    // test message was rejected
                    //remove contendership
                    //REJECT->REJECTERCOMPONENT->ITSFIRSTCHILD->ITSSECONDCHILD->.....LASTCHILDBEFORE->REJECTEDCOMPONENT
                    if (nodeMetaData.uid == nodeMetaData.leaderUID) {
                        System.out.println("***** Removing contendership");
                        synchronizerMessenger(Messages.COMPLETE_NONCONTENDER.value);
                    } else {
                        NodeMetaData parentNode = getNodeFromID(nodeMetaData, nodeMetaData.parentUID);
                        parentNode.msgQueue.add(message);
                    }
                } else if (message.startsWith(Messages.ACCEPT.value)) {
                    //PIGGYBACK to update leader UID and parenrts
                    //SEND ACK_MERGE across all nodes.
                    if (nodeMetaData.uid == nodeMetaData.leaderUID) {
                        System.out.println("Curently a leader with added child node");
                        nodeMetaData.level++;
                        System.out.println("**** Keeping my contendership");
                        synchronizerMessenger(Messages.COMPLETE_CONTENDER.value);
                        myPhaseDone = true;
                    } else {
                        //1th is new leader id
                        //Also a junction where these two components meet is also updated by this.. hopefully..
                        String[] acceptMsgSplit = message.split(",");
                        nodeMetaData.leaderUID = Integer.parseInt(acceptMsgSplit[1]);
                        NodePointers nodePointers = getmyNewParentChildNodes(acceptMsgSplit, nodeMetaData.uid + "");
                        nodeMetaData.parentUID = nodePointers.parentNode;
                        NodeMetaData myNewParent = getNodeFromID(nodeMetaData, nodePointers.parentNode);
                        NodeMetaData myNewChild = getNodeFromID(nodeMetaData, nodePointers.childNode);
                        myNewParent.status = 0;
                        myNewChild.status = 1;
                    }
                } else if (message.startsWith(Messages.COMPLETED.value)) {
                    //Mark one node completed and if all are completed send that to parent
                    String[] msgSplit = message.split(",");
                    int completedUid = Integer.parseInt(msgSplit[1]);
                    NodeMetaData neighborChild = getNodeFromID(nodeMetaData, completedUid);
                    neighborChild.status = 3;
                    boolean allnodescompleted = nodeMetaData.neighbors.stream().filter(n -> n.status == 3).count() == nodeMetaData.neighbors.size();
                    if (allnodescompleted) {
                        //leadernode
                        if (nodeMetaData.parentUID == -1) {
                            //Terminate Tree construction
                            mstPrint(nodeMetaData);
                            System.exit(0);
                        } else {
                            NodeMetaData parentNode = getNodeFromID(nodeMetaData, nodeMetaData.parentUID);
                            String msgToSend = String.format("%s,%s", Messages.COMPLETED.value, nodeMetaData.uid);
                            parentNode.msgQueue.add(msgToSend);
                        }
                    }
                }
            }
        }
    }

    private static NodePointers getmyNewParentChildNodes(String[] acceptMsgSplit, String id) {

        for (int i = 2; i < acceptMsgSplit.length; i++) {
            if (acceptMsgSplit[i].equals(id)) {
                NodePointers np = new NodePointers();
                np.parentNode = Integer.parseInt(acceptMsgSplit[i - 1]);
                np.childNode = Integer.parseInt(acceptMsgSplit[i + 1]);
                return np;
            }
        }
        return null;
    }

    private static String rejectionOrAcceptMsgBuilder(String mainmsg, String[] msgSplist) {
        StringBuilder rejectionMsgBuilder = new StringBuilder();
        rejectionMsgBuilder.append(mainmsg).append(",");
        for (int i = 1; i < msgSplist.length; i++) {
            if (i == 2) {
                continue;
            }
            rejectionMsgBuilder.append(msgSplist[i]);
            if (i < msgSplist.length - 1) {
                rejectionMsgBuilder.append(",");
            }
        }
        if (rejectionMsgBuilder.charAt(rejectionMsgBuilder.length() - 1) == ',') {
            rejectionMsgBuilder.deleteCharAt(rejectionMsgBuilder.length() - 1);
        }
        return rejectionMsgBuilder.toString();
    }

    /***
     * Get the UID of all the neighbor nodes
     * @param neighborUIDs
     * @return
     */
    private static String getNeighborUIDs(List<Integer> neighborUIDs) {
        StringBuilder builder = new StringBuilder();
        neighborUIDs.forEach(uid -> {
            builder.append(uid).append(" ");
        });
        return builder.toString();
    }

    static NodeMetaData getNodeFromID(NodeMetaData nodeMetaData, int uid) {
        return nodeMetaData.neighbors.stream()
                .filter(nodeMetaData1 -> nodeMetaData1.uid == uid)
                .findFirst().orElse(null);
    }

    /**
     * Used to print help message
     */
    static void printHelpMessage() {
        System.out.println("The system needs some parameters to function");
        System.out.println();
        System.out.println("If the environment is test, pass 2 args in the format \"java Node test dc01.utdallas.edu\"");
        System.out.println("the first arg will be the environment, second will be the id of the node to be assumed from config file as all systems run on localhost while development");
        System.out.println("The third argument indicates if the node is initiator or not opts true or false");
        System.out.println();
        System.out.println("If the environment is production, pass 1 args in the format \"java Node prod\"");
    }

    static int getSmallestDistNodeNeighborUID(NodeMetaData nodeMetaData, int parentuid) {
        int smallestDistNeighborDistance = Integer.MAX_VALUE; // distam
        int smallestDistNeighborUID = Integer.MIN_VALUE; // this should not be UID, its okay here


        List<Integer> keys = nodeMetaData.neighborUIDsAndWeights.keySet().stream().toList();
        for (int _tempUID : keys) {
            int _val = nodeMetaData.neighborUIDsAndWeights.get(_tempUID);
            if ((_tempUID != parentuid) && ((smallestDistNeighborDistance > _val)
                    ||
                    (smallestDistNeighborDistance == _val && _tempUID > smallestDistNeighborUID))) {
                smallestDistNeighborDistance = _val;
                smallestDistNeighborUID = _tempUID;
            }
        }
        return smallestDistNeighborUID;
    }

    static void mstPrint(NodeMetaData nodeMetaData) {
        Queue<NodeMetaData> queue = new LinkedList<>();
        queue.add(nodeMetaData);
        HashSet<Integer> hashSet = new HashSet<>();
        hashSet.add(nodeMetaData.uid);
        while (!queue.isEmpty()) {
            NodeMetaData nmd = queue.poll();
            System.out.printf("->%d", nmd.uid);
            nmd.neighbors.forEach(nodeMetaData1 -> {
                if (!hashSet.contains(nodeMetaData1.uid) && nmd.status == 1) {
                    hashSet.add(nodeMetaData1.uid);
                    queue.add(nodeMetaData1);
                }
            });
        }
    }

    static int synchronizerMessenger(String message) throws IOException, InterruptedException {
        Thread.sleep(3000);
        Socket synchronizerSocket = null;
        try {
            synchronizerSocket = new Socket(SYNCHRONIZER_HOST, SYNCPORT);
            DataOutputStream synchronizerOutputStream = new DataOutputStream(synchronizerSocket.getOutputStream());
            DataInputStream synchronizerInputStream = new DataInputStream(synchronizerSocket.getInputStream());
            synchronizerOutputStream.writeInt(message.length());
            synchronizerOutputStream.writeBytes(message);
            int respLength = synchronizerInputStream.readInt();
            if (respLength > 0) {
                byte[] line = new byte[respLength];
                synchronizerInputStream.readFully(line);
                String msgResp = new String(line);
                try {
                    int phase = Integer.parseInt(msgResp);
                    return phase;
                } catch (NumberFormatException e) {
                    return 0;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            synchronizerSocket.close();
        }
        return 0;
    }

    static int getRand() {
        Random rand = new Random();
        return rand.nextInt(5) + 1;
    }

}
