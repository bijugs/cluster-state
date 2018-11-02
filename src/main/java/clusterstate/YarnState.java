package clusterstate;

import java.net.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;

public class YarnState {

    private static final Logger logger = LoggerFactory.getLogger(YarnState.class);

    public static boolean checkYarn(ClusterDataProvider dataProvider, 
                                    Configuration conf, 
                                    String clusterId,
                                    String krbRealm){
        String[] rmNodes = dataProvider.getYarnRMServers(clusterId);
        String amiPort = dataProvider.getYarnRMAMIPort(clusterId);
        YarnClient yarnClient = null;
        if (amiPort == null || rmNodes == null) {
            logger.info("Yarn RM nodes and AMI port unknown for cluster "+clusterId);
            return false;
        }
        conf.set(YarnConfiguration.RM_PRINCIPAL, "yarn/_HOST@"+krbRealm);
        for (String rmNode : rmNodes) {
            if (rmNode == null)
               break;
            try {
                conf.set(YarnConfiguration.RM_ADDRESS, rmNode);
                yarnClient = YarnClient.createYarnClient();
                yarnClient.init(conf);
                yarnClient.start();
                YarnClusterMetrics clusterMetrics = yarnClient.getYarnClusterMetrics();
                logger.info("Able to start yarn client "+clusterMetrics.getNumNodeManagers());
                yarnClient.stop();
                return true;
            } catch(ConnectException ex) {
                logger.info("Connection refused. Non active RM possible. Will try other RMs for the cluster");
                break;
            } catch(Exception ex) {
                logger.info("Exception connecting to Yarn on cluster "+clusterId);
            } finally {
                if (yarnClient != null)
                    yarnClient.stop();
            }
        }
        return false;
    }
}
