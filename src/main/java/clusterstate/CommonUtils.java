package clusterstate;

import java.io.IOException;
import java.net.MalformedURLException;

import javax.management.ObjectName;
import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommonUtils {
 
    private static final Logger logger = LoggerFactory.getLogger(CommonUtils.class);

    public static MBeanServerConnection getMBeanServerConnection(String serverName, String port) {

        MBeanServerConnection mbsc = null;
        try {
            JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + serverName + ":"+ port +"/jmxrmi");
            JMXConnector jmxc = JMXConnectorFactory.connect(url, null);
            mbsc = jmxc.getMBeanServerConnection();
            return mbsc;
        } catch (MalformedURLException e) {
            logger.info("MalformedURLException while trying to get a MBeanServerConnection");
        } catch (IOException e) {
            logger.info("IOException while trying to get a MBeanServerConnection",e);
        } catch (Exception e) {
            logger.info("Exception while trying to get a MBeanServerConnection",e);
        }
        return mbsc;
    }

    public static Configuration createConfiguration(boolean isSecure, String krbRealm, String userName, String keyTab) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());
        try {
            if (isSecure) {
               System.out.println("Performing UGI login since the cluster is secure");
               conf.set("hadoop.security.authentication", "Kerberos");
               conf.set("hbase.security.authentication", "Kerberos");
               conf.set("hbase.master.kerberos.principal", "hbase/_HOST@"+krbRealm);
               conf.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@"+krbRealm);
               UserGroupInformation.setConfiguration(conf);
               if (userName != null && keyTab != null) {
                  System.out.println("Performing UGI login from keyTab");
                  UserGroupInformation.loginUserFromKeytab(userName, keyTab);
               } else { // This may not work with Springboot, need to be checked
                  System.out.println("Performing UGI login using current user");
                  UserGroupInformation.loginUserFromSubject(null);
               }
            }
        } catch (Exception ex) {
             logger.info("Error when UGI login ");
             throw ex;         
        }
        return conf;
    }
}
