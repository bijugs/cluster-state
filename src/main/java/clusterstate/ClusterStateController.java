package clusterstate;

import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;
import java.util.Properties;
import java.util.Set;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.core.env.Environment;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.annotation.Autowired;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import org.apache.zookeeper.ZooKeeper;

@RestController
public class ClusterStateController {

    private static final ClusterDataProvider dataProvider;
    private static final Logger logger = LoggerFactory.getLogger(ClusterStateController.class);
    private static Configuration conf;

    private boolean isSecure = false;
    private String userName;
    private String keyTab;
    private String hdfsPath;

    static
    {
        dataProvider = new DefaultClusterDataProvider();
    }

    @Autowired
    public ClusterStateController(@Value("${isSecure}")  String isSecure,
                                  @Value("${userName}")  String userName,
                                  @Value("${keyTab}")  String keyTab,
                                  @Value("${hdfsPath}")  String hdfsPath) throws Exception {
        logger.info("In constructor property {}", isSecure);
        if (isSecure.equalsIgnoreCase("true"))
           this.isSecure = true;
        logger.info("In constructor property {}", userName);
        this.userName = userName;
        logger.info("In constructor property {}", keyTab);
        this.keyTab = keyTab;
        this.hdfsPath = hdfsPath;
        conf = new Configuration();
        conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());
        try {
            if (this.isSecure) {
                System.out.println("Performing UGI login since the cluster is secure");
                conf.set("hadoop.security.authentication", "Kerberos");
                UserGroupInformation.setConfiguration(conf);
                if (userName != null && keyTab != null) {
                    System.out.println("Performing UGI login from keyTab");
                    UserGroupInformation.loginUserFromKeytab(userName, keyTab);
                } else { // This may not work, need to be checked
                    System.out.println("Performing UGI login using current user");
                    UserGroupInformation.loginUserFromSubject(null);
                }
             }
        } catch (Exception ex) {
             logger.info("Error when UGI login ");
             throw ex;         
        } 
    }

    public static String defaultMessage() {
        return "Available end points "+  
        "http://host:8080/cluster?id=cluster-id&act=action where "+
        ":action = kafka-brokers|kafka-state|zk-quorum|zk-state";
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
                case "hdfs-test":
                    boolean hdfsFine = HdfsState.checkHdfs(dataProvider,conf,id,hdfsPath);
                    if (hdfsFine)
                       return new ClusterStatus(id,act,"OK","UP");
                    else
                       return new ClusterStatus(id,act,"OK","DOWN");
                case "kafka-brokers":
                    zk = connector.connect(zkQuorum);
                    String childrenNodes = KafkaState.getKafkaBrokers(dataProvider, zk, id);
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
                    boolean zkState = ZKState.getZKState(connector, zkQuorum);
                    if (zkState) 
                        return new ClusterStatus(id,act,"OK","UP");
                    else
                        return new ClusterStatus(id,act,"OK","DOWN");
                 case "kafka-topics":
                    zk = connector.connect(zkQuorum);
                    String topics = KafkaState.getTopics(dataProvider,zk,id);
                    zk.close();
                    if (topics == null)
                        return new ClusterStatus(id,act,"OK","Error");
                    else
                        return new ClusterStatus(id,act,"OK",topics);
                 default:
                    return new ClusterStatus(id,act,"Fail",defaultMessage());
            }  
        } catch (Exception e) {
            return new ClusterStatus(id,act,"Fail","");       
        }
    }
}
