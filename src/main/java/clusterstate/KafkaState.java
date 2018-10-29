package clusterstate;

import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;
import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.common.KafkaFuture;

import org.apache.zookeeper.ZooKeeper;

public class KafkaState {

    public static AdminClient getKafkaAdmin(String bootStrap) {
        Properties props = new Properties();
        props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrap);
        AdminClient adminClient = AdminClient.create(props);
        return adminClient;
    }

    public static String getKafkaBrokers(ClusterDataProvider dataProvider, ZooKeeper zk,String clusterId) throws Exception {
        List<String> zNodes = zk.getChildren("/brokers/ids", true);
        String childrenNodes = new String(); //not the best
        ListIterator<String> ite = zNodes.listIterator();
        if (ite.hasNext())
            childrenNodes += ZKState.getZKDataString(zk, "/brokers/ids/"+ite.next()) + ":" + dataProvider.getKafkaPort(clusterId);
        while (ite.hasNext())
        {
            childrenNodes += "," + ZKState.getZKDataString(zk, "/brokers/ids/"+ite.next())  + ":" + dataProvider.getKafkaPort(clusterId);
        }
        return childrenNodes;
    }

    public static String getTopics(ClusterDataProvider dataProvider, ZooKeeper zk,String clusterId) {
        String ret = null;
        try {
            String bootStrapNodes = getKafkaBrokers(dataProvider, zk, clusterId);
            AdminClient adminClient = getKafkaAdmin(bootStrapNodes);
            ListTopicsResult topicResult = adminClient.listTopics();
            KafkaFuture<java.util.Set<java.lang.String>> topicNamesFuture = topicResult.names();
            java.util.Set<java.lang.String> topicNames = topicNamesFuture.get();
            String[] topicArray = topicNames.toArray(new String[0]);
            ret = Arrays.toString(topicArray);
        } catch (Exception e) {

        }
        return ret;
    }
}
