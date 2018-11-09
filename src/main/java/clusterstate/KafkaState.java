package clusterstate;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.TopicExistsException;

import org.apache.zookeeper.ZooKeeper;

public class KafkaState {


    private static final Logger logger = LoggerFactory.getLogger(KafkaState.class);

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
            adminClient.close();
        } catch (Exception e) {

        }
        return ret;
    }

    public static boolean topicExists(ClusterDataProvider dataProvider, ZooKeeper zk, String clusterId, String topicName) {
        boolean ret = false;
        DescribeTopicsResult describeTopicResult = null;
        try {
            String bootStrapNodes = getKafkaBrokers(dataProvider, zk, clusterId);
            AdminClient adminClient = getKafkaAdmin(bootStrapNodes);
            describeTopicResult = adminClient.describeTopics(Collections.singleton(topicName));
            ret = (describeTopicResult.values().get(topicName).get() != null);
            adminClient.close();
        } catch (Exception e) {

        }
        return ret;
    }

    public static boolean createTopic(ClusterDataProvider dataProvider, ZooKeeper zk, String clusterId, String topicName) {
        boolean ret = false;
        try {
            String bootStrapNodes = getKafkaBrokers(dataProvider, zk, clusterId);
            AdminClient adminClient = getKafkaAdmin(bootStrapNodes);
            NewTopic newTopic = new NewTopic(topicName, 3, (short)3);
            CreateTopicsResult createTopicsResult = adminClient.createTopics(Collections.singleton(newTopic));
            createTopicsResult.values().get(topicName).get();
            ret=true;
            adminClient.close();
        } catch (InterruptedException | ExecutionException e) {
            if ((e.getCause() instanceof TopicExistsException)) {
                logger.info("Topic already exists in cluster");
                ret = true;
            }
        } catch (Exception e) {

        }
        return ret;
    }

    public static boolean produceMessage(ClusterDataProvider dataProvider, ZooKeeper zk, String clusterId, String topicName) {
        boolean ret = false;
        try {
            Properties p = new Properties();
            String bootStrapNodes = getKafkaBrokers(dataProvider, zk, clusterId);
            p.put("bootstrap.servers",bootStrapNodes);
            p.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
            p.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
            Producer<String, String> pd = new KafkaProducer<>(p);
            ProducerRecord<String,String> rec = new ProducerRecord<>(topicName ,"key","value");
            RecordMetadata rMD = pd.send(rec).get();
            ret = true;
            pd.close();
        } catch (Exception ex) {

        }
        return ret;
    }

    public static boolean testKafka(ClusterDataProvider dataProvider, ZooKeeper zk, String clusterId, String topicName) {
        boolean ret = false;
        if (!topicExists(dataProvider, zk, clusterId, topicName))
            if (!createTopic(dataProvider, zk, clusterId, topicName))
               return ret;
        ret = produceMessage(dataProvider, zk, clusterId, topicName);
        return ret;
    }
}
