import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

public class NodeMetaData {
    int uid;
    String host;
    String hostUrl;
    int port;
    List<NodeMetaData> neighbors;
    List<Integer> treeNodes;
    Map<Integer, Integer> neighborUIDsAndWeights;
    int leaderUID;
    int parentUID;
    int perspectiveWeight;
    Queue<String> msgQueue;
    int level;//k
    int status;//-1,0,1 -1 is unknown, 0 is parent, 1 is child

    AtomicBoolean isConnected = new AtomicBoolean(false);

}
