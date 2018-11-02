package clusterstate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

public class HbaseState {

    private static final Logger logger = LoggerFactory.getLogger(HbaseState.class);

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
}
