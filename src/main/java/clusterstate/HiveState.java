package clusterstate;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

public class HiveState {

    private static final Logger logger = LoggerFactory.getLogger(HdfsState.class);

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    public static boolean testHive(ClusterDataProvider dataProvider,
                                    Configuration conf,
                                    String clusterId, 
                                    String hiveDB,
                                    String hiveTable,
                                    String userName,
                                    String keyTab,
                                    String krbRealm) {
        Connection con = null;
        String[] hiveServers = dataProvider.getHiveServers(clusterId);
        String hivePort = dataProvider.getHiveServerPort(clusterId);
        boolean ret = false;
        logger.info("testHive: hiveDB {} and hiveTable {}", hiveDB, hiveTable);
        try {
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab(userName, keyTab);
        } catch (Exception ex) {
            logger.info("Error during UGI login");
            return false;
        }
        for (String hiveServer : hiveServers) {
            try {
                Class.forName(driverName);
                con = DriverManager.getConnection("jdbc:hive2://"+hiveServer+":"+hivePort+"/"+hiveDB+";principal=hive/"+hiveServer+"@"+krbRealm, "", "");
                Statement stmt = con.createStatement();
                stmt.execute("drop table " + hiveTable);
                stmt.execute("create table "+ hiveTable + "(key int, value string) row format delimited fields terminated by ',' stored as textfile");
                String sql = "show tables '" + hiveTable + "'";
                ResultSet res = stmt.executeQuery(sql);
                if (res.next())
                   ret = true;
                con.close();
                return ret;
            } catch (ClassNotFoundException e) {
                logger.info("ClassNotFoundException ",e);
                System.exit(1);
            } catch (Exception ex) {
                logger.info("Exception ",ex);
            } 
        }
        return ret;
    }
}
