package clusterstate;

import java.util.List;
import java.util.ListIterator;
import javax.servlet.http.HttpServletResponse;

import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.PathVariable;

import org.apache.zookeeper.ZooKeeper;

@RestController
public class ClusterStateController {

    private static final ClusterZK zkQuorumProvider;
    static
    {
        zkQuorumProvider = new DefaultClusterZK();
    }

    @RequestMapping("/")
    public String index() {
        return "Greetings from Spring Boot!";
    }

    @RequestMapping(value = "/cluster", method = RequestMethod.GET)
    @ResponseBody
    public ClusterStatus zk(@RequestParam(value="id") String id,@RequestParam(value="act") String act,HttpServletResponse response) {
        response.setContentType("text/plain");
        response.setCharacterEncoding("UTF-8");
        try {
            String zkQuorum = zkQuorumProvider.getZKQuorum(id);
            if (zkQuorum == null)
               return new ClusterStatus(id,act,"Fail","Unknown cluster");
            zkConnect connector = new zkConnect();
            ZooKeeper zk = connector.connect(zkQuorum);
            switch(act) {
                case "brokers":
                    List<String> zNodes = zk.getChildren("/", true);
                    String childrenNodes = new String(); //not the best
                    ListIterator<String> ite = zNodes.listIterator();
                    if (ite.hasNext())
                        childrenNodes += ite.next();
                    while (ite.hasNext())
                    {
                       childrenNodes += ","+ite.next();   
                    }
                    return new ClusterStatus(id,act,"OK",childrenNodes);
                 default:
                    return new ClusterStatus(id,act,"Fail","Unrecognized request");
            }  
        } catch (Exception e) {
            return new ClusterStatus(id,act,"Fail","");       
        }
    }

}
