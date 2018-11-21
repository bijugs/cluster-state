package clusterstate;

import javax.management.ObjectName;
import javax.management.MBeanServerConnection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

public class HbaseState {

    private static final Logger logger = LoggerFactory.getLogger(HbaseState.class);

    static String[] attr = {"memStoreSize", "storeFileCount", "flushQueueLength", "compactionQueueLength"};

    public static boolean testHbase(ClusterDataProvider dataProvider, Configuration conf, String clusterId){
        String zkNodes = dataProvider.getZKNodes(clusterId);
        conf.set("hbase.zookeeper.quorum", zkNodes);
        try {
           Connection conn = ConnectionFactory.createConnection(conf);
           TableName[] tables = conn.getAdmin().listTableNames();
           conn.close();
           return (tables.length > 0);
        } catch (Exception ex) {
            logger.info(ex.getMessage());
            return false;
        }
    }

    public static boolean getJMXMetrics(ClusterDataProvider dataProvider, String clusterId) {
        
        boolean ret = false;
        MBeanServerConnection mbsc = CommonUtils.getMBeanServerConnection("hbase-rs-node","10102");
        try {
            if (mbsc != null) {
                for (int i = 0; i < attr.length; i++) {
                    Object val = mbsc.getAttribute(new ObjectName("Hadoop:service=HBase,name=RegionServer,sub=Server"), attr[i]);
                    logger.info("HBase JMX metrix {} {}",attr[i],val);
                }
                ret = true;
            } else {
                logger.info("Couldn't get HBase JMX metrics");
            }
       } catch(Exception e) {
             ret = false;
            logger.info(e.getMessage());
       }
       return ret;
    }
}
