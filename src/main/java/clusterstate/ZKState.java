package clusterstate;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.KeeperException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ZKState {

    public static final ObjectMapper mapper = new ObjectMapper();
    private static final Logger logger = LoggerFactory.getLogger(ZKState.class);

    public static String getZKDataString(ZooKeeper zk, String znode) throws Exception {
        Stat stat = new Stat();
        byte[] data = zk.getData(znode, false,stat);
        JsonNode zkDataTree = mapper.readTree(data);
        return zkDataTree.get("host").textValue();
    }

    public static boolean checkZK(zkConnect connector, String quorum) throws Exception {
        String[] nodes = quorum.split(",");
        int count = 0;
        boolean ret = false;
        for (String node : nodes) {
           ZooKeeper zk = connector.connect(node);
           if (zk.getState().isAlive())
              count++;
           zk.close();
        }
        logger.info("Quorum has "+nodes.length+" nodes and "+count+" are alive"); 
        if (count >= (nodes.length/2+1))
           ret = true;
        return ret;
    }

}
