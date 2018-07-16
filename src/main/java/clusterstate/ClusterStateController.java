package clusterstate;

import java.util.List;
import java.util.ListIterator;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.PathVariable;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.KeeperException;

@RestController
public class ClusterStateController {

    private static final ClusterDataProvider dataProvider;
    private static final ObjectMapper mapper;
    static
    {
        dataProvider = new DefaultClusterDataProvider();
        mapper = new ObjectMapper();
    }

    public static String defaultMessage() {
        return "Available end points "+  
        "http://host:8080/cluster?id=cluster-id&act=action where "+
        ":action = brokers|kafka-state|zk-quorum|zk-state";
    }

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

    @RequestMapping("/")
    @ResponseBody
    public String index(HttpServletResponse response) {
        response.setContentType("text/plain");
        response.setCharacterEncoding("UTF-8");
        return "Greetings from Cluster Explorer! " + defaultMessage();
    }

    @RequestMapping(value = "/cluster", method = RequestMethod.GET)
    @ResponseBody
    public ClusterStatus zk(@RequestParam(value="id") String id,@RequestParam(value="act") String act,HttpServletResponse response) {
        response.setContentType("text/plain");
        response.setCharacterEncoding("UTF-8");
        try {
            String zkQuorum = dataProvider.getZKQuorum(id);
            if (zkQuorum == null)
               return new ClusterStatus(id,act,"Fail","Unknown cluster");
            zkConnect connector = new zkConnect();
            ZooKeeper zk;
            List<String> zNodes;
            switch(act) {
                case "brokers":
                    zk = connector.connect(zkQuorum);
                    zNodes = zk.getChildren("/brokers/ids", true);
                    String childrenNodes = new String(); //not the best
                    ListIterator<String> ite = zNodes.listIterator();
                    if (ite.hasNext())
                        childrenNodes += getZKDataString(zk, "/brokers/ids/"+ite.next()) + ":" + dataProvider.getKafkaPort(id);
                    while (ite.hasNext())
                    {
                       childrenNodes += "," + getZKDataString(zk, "/brokers/ids/"+ite.next())  + ":" + dataProvider.getKafkaPort(id);
                    }
                    zk.close();
                    return new ClusterStatus(id,act,"OK",childrenNodes);
                case "kafka-state":
                    zk = connector.connect(zkQuorum);
                    zNodes = zk.getChildren("/brokers/ids", true);
                    zk.close();
                    if (zNodes.size() > 3)
                       return new ClusterStatus(id,act,"OK","UP");
                    else
                       return new ClusterStatus(id,act,"OK","DOWN");
                 case "zk-quorum":
                    return new ClusterStatus(id,act,"OK",zkQuorum);
                 case "zk-state":
                    boolean zkState = getZKState(connector, zkQuorum);
                    if (zkState) 
                        return new ClusterStatus(id,act,"OK","UP");
                    else
                        return new ClusterStatus(id,act,"OK","DOWN");
                 default:
                    return new ClusterStatus(id,act,"Fail",defaultMessage());
            }  
        } catch (Exception e) {
            return new ClusterStatus(id,act,"Fail","");       
        }
    }
}
