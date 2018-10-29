package clusterstate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.KeeperException;

public class ZKState {

    public static final ObjectMapper mapper = new ObjectMapper();

    public static String getZKDataString(ZooKeeper zk, String znode) throws Exception {
        Stat stat = new Stat();
        byte[] data = zk.getData(znode, false,stat);
        JsonNode zkDataTree = mapper.readTree(data);
        return zkDataTree.get("host").textValue();
    }

    public static boolean getZKState(zkConnect connector, String quorum) throws Exception {
        String[] nodes = quorum.split(",");
        int count = 0;
        boolean ret = false;
        for (String node : nodes) {
           ZooKeeper zk = connector.connect(node);
           if (zk.getState().isAlive())
              count++;
           zk.close();
        }
        if (count >= 3)
           ret = true;
        return ret;
    }

}
