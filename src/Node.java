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
        HashSet<String> mst = new HashSet<>();

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
                    mst.add(String.format("(%d,%d)", nodeMetaData.uid, msgRcvdFromNode.uid));
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
                mstPrint(mst);
            } else {
                System.out.printf("%d uid is part of some component %d and my parent is %d%n", nodeMetaData.uid, nodeMetaData.leaderUID, nodeMetaData.parentUID);
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
                    int smallestdistNeighborUID = getSmallestDistNodeNeighborUID(nodeMetaData, nodeMetaData.parentUID);
                    System.out.println("All nodes are unmarked, sending TEST message to smallest UID which is " + smallestdistNeighborUID);
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
                if(phase>=5){
                    System.out.println("MST completed");
                    System.exit(0);
                }
                Thread.sleep(5000);

                if (inputMessages.isEmpty()) {
                    Thread.sleep(2000);
                    System.out.println("wait....");
                    continue;
                }
                //how will a child node deal with all responses like the above hashmap
                // a component leader can recieve a test message, when will this happen, only if the other incoming neighbor distance is greater
                String message = inputMessages.poll();
                System.out.println("Message received is " + message);
                int myknownminedge = Integer.MAX_VALUE;
                if (message.startsWith(Messages.TEST.value)) {
                    //if you are not a leader, but you are the one to who message is intended to, send samecomponent
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
                        } else if (nodeMetaData.uid == Integer.parseInt(msgSplist[msgSplist.length - 1]) && nodeMetaData.leaderUID == Integer.parseInt(msgSplist[1])) {
                            int lastToLastNode = Integer.parseInt(msgSplist[msgSplist.length - 2]);
                            NodeMetaData getLastNode = getNodeFromID(nodeMetaData, lastToLastNode);
                            getLastNode.msgQueue.add(String.format("%s,%s", Messages.SAMECOMPONENT, nodeMetaData.uid));
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
                            System.out.printf("Send %s to my parent %d%n", acceptMsg, nodeMetaData.parentUID);
                            parentNode.msgQueue.add(acceptMsg);
                            System.out.printf("Added node %d as my parent and %d as my component leader %n",
                                    nodeMetaData.parentUID, nodeMetaData.leaderUID);
                            myPhaseDone = true;
                            System.out.println("**** Removing my contendership");
                            String msgToSend = String.format("%s,%d", Messages.CHANGE_LEADER, nodeMetaData.leaderUID);
                            nodeMetaData.neighbors.stream().filter(nmd -> nmd.status == 1).forEach(nm1 -> {
                                nm1.msgQueue.add(msgToSend);
                            });
                            //Send change leader to child nodes
                            synchronizerMessenger(Messages.COMPLETE_NONCONTENDER.value);
                        } else {
                            //reject
                            String rejectMsg = rejectionOrAcceptMsgBuilder(Messages.REJECT.value, msgSplist);
                            String[] rejSplit = rejectMsg.split(",");
                            System.out.println(rejectMsg); // TODO: 4/4/2023

                            NodeMetaData nodeTosend = null;
                            if (rejSplit.length == 3) {
                                System.out.println("Direct message, send to first node");
                                nodeTosend = getNodeFromID(nodeMetaData, Integer.parseInt(rejSplit[1]));
                            } else {
                                int lastNode = Integer.parseInt(rejSplit[rejSplit.length - 2]);
                                System.out.println("Last node to send is " + lastNode);
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
                        int lastNode = Integer.parseInt(msgSplist[msgSplist.length - 1]);
                        System.out.println("Last node in the message is " + lastNode);
                        if (otherComponentUID == nodeMetaData.leaderUID && lastNode == nodeMetaData.uid) {
                            int lastToLastNode = Integer.parseInt(msgSplist[msgSplist.length - 2]);
                            NodeMetaData nmd = getNodeFromID(nodeMetaData, lastToLastNode);
                            String msgToSend = String.format("%s,%s", Messages.SAMECOMPONENT.value, nodeMetaData.uid);
                            System.out.printf("3.Message to send %s%n", message);
                            nmd.msgQueue.add(msgToSend);
                        } else if (lastNode == nodeMetaData.uid) {
                            message = message + "," + nodeMetaData.parentUID;
                            System.out.printf("4.Message to send %s%n", message);
                            NodeMetaData parentNode = getNodeFromID(nodeMetaData, nodeMetaData.parentUID);
                            parentNode.msgQueue.add(message);
                        } else {
                            int k = -1;
                            for (int i = 1; i < msgSplist.length; i++) {
                                if (nodeMetaData.uid == Integer.parseInt(msgSplist[i])) {
                                    k = i + 1;
                                    break;
                                }
                            }
                            if (k == -1 || k >= msgSplist.length) {
                                System.out.println(k + " : Fatal error");
                            } else {
                                NodeMetaData pathnode = getNodeFromID(nodeMetaData, Integer.parseInt(msgSplist[k]));
                                pathnode.msgQueue.add(message);
                            }
                        }
                    }
                } else if (message.startsWith(Messages.SEARCH.value)) {
                    //If all my edges are not marked, send my parent the minimum of 3
                    // else forward search to marked nodes
                    // if no nodes are -1, send COMPLETION
                    System.out.println("Received Search message from " + message);
                    long unmarkedEdges = nodeMetaData.neighbors.stream().filter(nm -> nm.status == -1).count();
                    long completedneighbors = nodeMetaData.neighbors.stream().filter(nm -> nm.status == 3).count();
                    if (unmarkedEdges == nodeMetaData.neighbors.size() - 1) {
                        System.out.println("Print 1");
                        int shortestdistNeighborUID = getSmallestDistNodeNeighborUID(nodeMetaData, nodeMetaData.parentUID);
                        int weightOfIt = nodeMetaData.neighborUIDsAndWeights.get(shortestdistNeighborUID);
                        //MIN_EDGE,WEIGHT,MYUID,UIDOFOTHERCOMPONENT, PARENT WILL APPEND ITS
                        String msgToSendParent = String.format("%s,%s,%s,%s", Messages.MIN_EDGE.value, weightOfIt,
                                nodeMetaData.uid, shortestdistNeighborUID);
                        NodeMetaData parentNode = getNodeFromID(nodeMetaData, nodeMetaData.parentUID);
                        System.out.printf("5.Message to send %s%n", msgToSendParent);
                        parentNode.msgQueue.add(msgToSendParent);
                    } else if (completedneighbors == nodeMetaData.neighbors.size() - 1) { //1 is parent in this
                        System.out.println("Print 2");
                        String msgToSendParent = String.format("%s,%s", Messages.COMPLETED, nodeMetaData.uid);
                        NodeMetaData parentNode = getNodeFromID(nodeMetaData, nodeMetaData.parentUID);
                        parentNode.msgQueue.add(msgToSendParent);
                    } else if (unmarkedEdges == 1) {
                        NodeMetaData thelastNeighbor = nodeMetaData.neighbors.stream().filter(i -> i.status == -1).findFirst().orElse(null);
                        if (thelastNeighbor != null) {
                            String msgToSendParent = String.format("%s,%s,%s,%s", Messages.MIN_EDGE.value, nodeMetaData.neighborUIDsAndWeights.get(thelastNeighbor.uid),
                                    nodeMetaData.uid, thelastNeighbor.uid);
                            NodeMetaData parentNode = getNodeFromID(nodeMetaData, nodeMetaData.parentUID);
                            System.out.printf("5.Message to send %s%n", msgToSendParent);
                            parentNode.msgQueue.add(msgToSendParent);
                        }
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
//                    if (nodeMetaData.leaderUID == nodeMetaData.uid && myPhaseDone) {
//                        System.out.println("Already added, phase done");
//                        continue;
//                    }
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
                                System.out.println("There is a shorter direct edge neighbor" + currentKnownMinWeighString);
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
                            System.out.println("substring : " + currentKnownMinWeighString.substring(1 + currentKnownMinWeighString.indexOf(",")));

                            System.out.println("Test message generated is " + testMessage);
                            int nextNodeId = Integer.parseInt(testMessage.split(",")[3]);
                            NodeMetaData nextNode = getNodeFromID(nodeMetaData, nextNodeId);
                            nextNode.msgQueue.add(testMessage);
                            myknownminedge = currentknownMinWeightEdge;
                        } else {
                            //Send min edge to parent but add my info last but wight
                            System.out.println("The current know min weight string :" + currentKnownMinWeighString);
                            String[] splitsOFCurrentMINED = currentKnownMinWeighString.split(",");
                            StringBuilder builder = new StringBuilder();
                            for (int i = 0; i < splitsOFCurrentMINED.length; i++) {
                                if (i == 2) {
                                    builder.append(nodeMetaData.uid).append(","); //be aware of this ','
                                }
                                if (i == splitsOFCurrentMINED.length - 1) {
                                    builder.append(splitsOFCurrentMINED[i]);
                                } else {
                                    builder.append(splitsOFCurrentMINED[i]).append(",");
                                }
                            }
                            String updatedMinEdgemsg = builder.toString();
                            System.out.println("Update MIN eedge is " + updatedMinEdgemsg);
                            NodeMetaData parent = getNodeFromID(nodeMetaData, nodeMetaData.parentUID);
                            System.out.printf("Sending message %s to parent %d %n", updatedMinEdgemsg, parent.uid);
                            parent.msgQueue.add(updatedMinEdgemsg);
                        }
                    }
                } else if (message.startsWith(Messages.REJECT.value)) {
                    // test message was rejected
                    //remove contendership
                    //REJECT->REJECTERCOMPONENT->ITSFIRSTCHILD->ITSSECONDCHILD->.....LASTCHILDBEFORE->REJECTEDCOMPONENT
                    if (nodeMetaData.uid == nodeMetaData.leaderUID) {
                        System.out.println("***** Keeping contendership");
                        synchronizerMessenger(Messages.COMPLETE_CONTENDER.value);
                    }
                    //Send samecomponent
                    else {
                        //Message to send REJECT,5,8,184,200
                        String[] msgSplit = message.split(",");
                        if (Integer.parseInt(msgSplit[1]) == nodeMetaData.uid) {
                            continue;
                        }
                        for (int i = 1; i < msgSplit.length; i++) {
                            if (Integer.parseInt(msgSplit[i]) == nodeMetaData.uid) {
                                NodeMetaData nodeToSend = getNodeFromID(nodeMetaData, Integer.parseInt(msgSplit[i - 1]));
                                nodeToSend.msgQueue.add(message);
                            }
                        }
                    }
                } else if (message.startsWith(Messages.ACCEPT.value)) {
                    //PIGGYBACK to update leader UID and parenrts
                    //SEND ACK_MERGE across all nodes.
                    if (nodeMetaData.uid == nodeMetaData.leaderUID) {
                        String[] msgSplit = message.split(",");
                        int lastnode = Integer.parseInt(msgSplit[2]);
                        NodeMetaData child = getNodeFromID(nodeMetaData, lastnode);
                        child.status = 1;
                        System.out.println("Curently a leader with added child node " + child.uid);
                        addToMST(mst, message);
                        nodeMetaData.level++;
                        System.out.println("**** Keeping my contendership");
                        synchronizerMessenger(Messages.COMPLETE_CONTENDER.value);
                        myPhaseDone = true;
                    } else {
                        //1th is new leader id
                        //Also a junction where these two components meet is also updated by this.. hopefully..
                        System.out.println("Lalalalallalal" + message);
                        String[] acceptMsgSplit = message.split(",");
                        nodeMetaData.leaderUID = Integer.parseInt(acceptMsgSplit[1]);
                        System.out.println("Updating new leader" + nodeMetaData.leaderUID);
                        NodePointers nodePointers = getmyNewParentChildNodes(acceptMsgSplit, nodeMetaData.uid + "");
                        System.out.println("New parent " + nodePointers.parentNode + " new child " + nodePointers.childNode);
                        nodeMetaData.parentUID = nodePointers.parentNode;
                        NodeMetaData myNewParent = getNodeFromID(nodeMetaData, nodePointers.parentNode);
                        NodeMetaData myNewChild = getNodeFromID(nodeMetaData, nodePointers.childNode);
                        myNewParent.status = 0;
                        myNewChild.status = 1;
                        System.out.printf("Adding %d as my parent and %d as my child%n", myNewParent.uid, myNewChild.uid);
                        myNewParent.msgQueue.add(message);
                        System.out.println("Informing my children if any to change leader");
                        final String changeLedaer = String.format("%s,%d", Messages.CHANGE_LEADER, nodeMetaData.leaderUID);
                        nodeMetaData.neighbors.stream().filter(i -> i.status == 1).forEach(i -> i.msgQueue.add(changeLedaer));
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
                            mstPrint(mst);
                            System.exit(0);
                        } else {
                            NodeMetaData parentNode = getNodeFromID(nodeMetaData, nodeMetaData.parentUID);
                            String msgToSend = String.format("%s,%s", Messages.COMPLETED.value, nodeMetaData.uid);
                            parentNode.msgQueue.add(msgToSend);
                        }
                    }
                } else if (message.startsWith(Messages.CHANGE_LEADER.value)) {
                    String[] msgSplit = message.split(",");
                    int leaderUID = Integer.parseInt(msgSplit[1]);
                    nodeMetaData.leaderUID = leaderUID;
                    final String _msg = message;
                    nodeMetaData.neighbors.stream().filter(n -> n.status == 1).forEach(n -> n.msgQueue.add(_msg));
                } else if (message.startsWith(Messages.SAMECOMPONENT.value)) {
                    String[] msgSplit = message.split(",");
                    int node = Integer.parseInt(msgSplit[1]);
                    NodeMetaData getNode = getNodeFromID(nodeMetaData, node);
                    getNode.status = 2;
                }
            }
        }
    }

    private static void addToMST(HashSet<String> mst, String message) {
        String[] messSplit = message.split(",");
        for (int i = 1; i < messSplit.length - 1; i++) {
            mst.add(String.format("(%s,%s)", messSplit[i], messSplit[i + 1]));
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
        HashSet<Integer> unknowns = new HashSet<>();
        nodeMetaData.neighbors.stream().filter(i -> i.status == -1).forEach(i -> unknowns.add(i.uid));

        List<Integer> keys = nodeMetaData.neighborUIDsAndWeights.keySet().stream().toList();
        for (int _tempUID : keys) {
            if(!unknowns.contains(_tempUID)) {continue;}
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

    static void mstPrint(HashSet<String> mstList) {
        StringBuilder builder = new StringBuilder();
        mstList.forEach(builder::append);
        System.out.println("MST is :" + builder.toString());
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
