import java.io.File;
import java.io.FileNotFoundException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;

public class ConfigurationReader {

    /***
     * read configuration
     * @param hostName
     * @param configFileName
     * @param env
     * @return
     */
    public NetworkInformation readConfiguration(String hostName, String configFileName, String env) throws FileNotFoundException {
        File configFile = new File(configFileName);
        Scanner fileReader = new Scanner(configFile);
        NetworkInformation networkInformation = new NetworkInformation();
        HashMap<Integer, ArrayList<NeighbourWeights>> neighbourWeightsHashMap = new HashMap<>();
        int numberOfNodes = -1;
        int counter = 0;
        boolean uidObtained = false;
        List<NodeMetaData> allNodeMetaData = new ArrayList<>();
        while (fileReader.hasNextLine()) {
            String line = fileReader.nextLine();
            if (Character.isDigit(line.charAt(0)) || line.charAt(0) == '(') {
                if (numberOfNodes == -1) {
                    numberOfNodes = Integer.parseInt(line);
                    networkInformation.countNodes = numberOfNodes;
                } else if (!uidObtained) {
                    String[] tokens = line.split(" ");
                    NodeMetaData nodeMetaData = new NodeMetaData();//provide fqdn of the server
                    nodeMetaData.host = String.format("%s", tokens[1]);
                    nodeMetaData.hostUrl = nodeMetaData.host;
                    if (env.equals("test")) {//used for local testing
                        nodeMetaData.hostUrl = "localhost";
                    }
                    nodeMetaData.port = Integer.parseInt(tokens[2]);
                    nodeMetaData.uid = Integer.parseInt(tokens[0]);
                    nodeMetaData.neighborUIDsAndWeights = new HashMap<>();
                    nodeMetaData.neighbors = new ArrayList<>();
                    allNodeMetaData.add(nodeMetaData);
                    counter++;
                    if (counter == numberOfNodes) {
                        uidObtained = true;
                        counter = 0;
                    }
                } else {
                    String[] text = line.split(" ");
                    String neighbourInfo = text[0];
                    int weight = Integer.parseInt(text[1]);
                    neighbourInfo = neighbourInfo.replace("(", "").replace(")", "");
                    String[] neighborUids = neighbourInfo.split(",");
                    int uid1 = Integer.parseInt(neighborUids[0]);
                    int uid2 = Integer.parseInt(neighborUids[1]);
                    ArrayList<NeighbourWeights> nw = neighbourWeightsHashMap.getOrDefault(uid1, new ArrayList<>());
                    nw.add(new NeighbourWeights(uid2, weight));
                    neighbourWeightsHashMap.put(uid1, nw);

                    ArrayList<NeighbourWeights> nw2 = neighbourWeightsHashMap.getOrDefault(uid2, new ArrayList<>());
                    nw2.add(new NeighbourWeights(uid1, weight));
                    neighbourWeightsHashMap.put(uid2, nw2);
                }
            }
        }
        fileReader.close();
        NodeMetaData currNodeMetaData = allNodeMetaData.stream().filter(n -> n.host.equals(hostName))
                .findFirst().orElse(null);
        networkInformation.nodeMetaData = currNodeMetaData;
        currNodeMetaData.leaderUID = currNodeMetaData.uid;
        currNodeMetaData.parentUID = -1;
        markNeighbors(allNodeMetaData, currNodeMetaData, neighbourWeightsHashMap);
        return networkInformation;
    }

    private void markNeighbors(List<NodeMetaData> allNodeMetaData, NodeMetaData currNodeMetaData,
                               HashMap<Integer, ArrayList<NeighbourWeights>> neighbourWeightsHashMap) {

        ArrayList<NeighbourWeights> neighbourWeights = neighbourWeightsHashMap.get(currNodeMetaData.uid);
        neighbourWeights.forEach(nw -> {
            NodeMetaData neigh = allNodeMetaData.stream().filter(node -> node.uid == nw.node).findFirst().orElse(null);
            if (neigh != null) {
                neigh.perspectiveWeight = nw.value;
            }
            currNodeMetaData.neighbors.add(neigh);
        });
    }
}
