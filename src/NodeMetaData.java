import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class NodeMetaData {
    int uid;
    String host;
    String hostUrl;
    int port;
    List<NodeMetaData> neighbors;
    Map<Integer, Integer> neighborUIDsAndWeights;
    int leaderUID;
    int parentUID;
    int perspectiveWeight;
    List<String> msgQueue;

    AtomicBoolean isConnected = new AtomicBoolean(false);

}
