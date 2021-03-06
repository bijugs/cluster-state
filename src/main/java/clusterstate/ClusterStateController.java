package clusterstate;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
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
import org.springframework.scheduling.annotation.Scheduled;

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
    private String krbRealm;
    private String hdfsPath;
    private String hiveDB;
    private String hiveTable;
    private String kafkaTopic;

    static
    {
        dataProvider = new DefaultClusterDataProvider();
    }

    @Scheduled(fixedRate = 14400000, initialDelay = 14400000)
    public void scheduleUGILogin() {
        logger.info("Scheduled UGI Login :: Execution Time - {}", System.currentTimeMillis() );
        try {
            if (isSecure && (userName != null && keyTab != null)) {
               UserGroupInformation.loginUserFromKeytab(userName, keyTab);
               logger.info("Scheduled UGI login successful");
            }
        } catch (Exception ex) {
             logger.info("Scheduled UGI login failed");
             System.exit(1);
        }
    }

    @Autowired
    public ClusterStateController(@Value("${isSecure}")  String isSecure,
                                  @Value("${userName}")  String userName,
                                  @Value("${keyTab}")  String keyTab,
                                  @Value("${krbRealm}")  String krbRealm,
                                  @Value("${hiveDB}")  String hiveDB,
                                  @Value("${hiveTable}")  String hiveTable,
                                  @Value("${hdfsPath}")  String hdfsPath,
                                  @Value("${kafkaTopic}")  String kafkaTopic) throws Exception {
        logger.info("In constructor property {}", isSecure);
        if (isSecure.equalsIgnoreCase("true"))
           this.isSecure = true;
        logger.info("In constructor property {}", userName);
        this.userName = userName;
        logger.info("In constructor property {}", keyTab);
        this.keyTab = keyTab;
        this.hdfsPath = hdfsPath;
        this.krbRealm = krbRealm;
        this.hiveDB = hiveDB;
        this.hiveTable = hiveTable;
        this.kafkaTopic = kafkaTopic;
        try {
            conf = CommonUtils.createConfiguration(this.isSecure, this.krbRealm, this.userName, this.keyTab);
        } catch (Exception ex) {
             logger.info("Error creating configuration");
             throw ex;         
        } 
    }

    public static String defaultMessage() {
        return "Available end points "+  
        "http://host:8080/cluster?id=cluster-id&act=action where "+
        ":action = kafka-brokers|kafka-state|zk-quorum|zk-state";
    }

    public static ClusterStatus createTestResult(boolean success, String id, String act) {
       if (success)
          return new ClusterStatus(id,act,"OK","UP");
       else
          return new ClusterStatus(id,act,"OK","DOWN");
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
                case "hbase-jmx":
                    return createTestResult(false,id,act);
                case "hbase-test":
                    boolean isHbaseFine = HbaseState.testHbase(dataProvider,conf,id);
                    return createTestResult(isHbaseFine,id,act);
                case "hdfs-test":
                    boolean isHdfsFine = HdfsState.testHdfs(dataProvider,conf,id,hdfsPath);
                    return createTestResult(isHdfsFine,id,act);
                case "hive-test":
                    boolean isHiveFine = HiveState.testHive(dataProvider,conf,id,hiveDB,hiveTable,userName,keyTab,krbRealm);
                    return createTestResult(isHiveFine,id,act);
                case "kafka-brokers":
                    zk = connector.connect(zkQuorum);
                    String childrenNodes = KafkaState.getKafkaBrokers(dataProvider, zk, id);
                    zk.close();
                    return new ClusterStatus(id,act,"OK",childrenNodes);
                case "kafka-state":
                    zk = connector.connect(zkQuorum);
                    zNodes = zk.getChildren("/brokers/ids", true);
                    zk.close();
                    boolean isKafkaFine = (zNodes.size() > 3);
                    return createTestResult(isKafkaFine,id,act);
                case "kafka-test":
                    zk = connector.connect(zkQuorum);
                    isKafkaFine = KafkaState.testKafka(dataProvider,zk,id,kafkaTopic);
                    return createTestResult(isKafkaFine,id,act);
                 case "kafka-topics":
                    zk = connector.connect(zkQuorum);
                    String topics = KafkaState.getTopics(dataProvider,zk,id);
                    zk.close();
                    if (topics == null)
                        return new ClusterStatus(id,act,"OK","Error");
                    else
                        return new ClusterStatus(id,act,"OK",topics);
                case "phoenix-test":
                    boolean isPhoenixFine = PhoenixState.testPhoenix(dataProvider,conf,id,userName,keyTab);
                    return createTestResult(isPhoenixFine,id,act);
                case "yarn-state":
                    boolean isYarnFine = YarnState.checkYarn(dataProvider,conf,id,krbRealm);
                    return createTestResult(isYarnFine,id,act);
                 case "zk-quorum":
                    return new ClusterStatus(id,act,"OK",zkQuorum);
                 case "zk-state":
                    boolean isZKFine = ZKState.checkZK(connector, zkQuorum);
                    return createTestResult(isZKFine,id,act);
                 default:
                    return new ClusterStatus(id,act,"Fail",defaultMessage());
            }  
        } catch (Exception e) {
            return new ClusterStatus(id,act,"Fail","");       
        }
    }
}
