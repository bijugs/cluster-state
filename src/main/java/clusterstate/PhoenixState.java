package clusterstate;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.SQLException;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

public class PhoenixState {

    private static final Logger logger = LoggerFactory.getLogger(PhoenixState.class);

    public static boolean testPhoenix(ClusterDataProvider dataProvider, 
                                       Configuration conf, 
                                       String clusterId,
                                       String userName,
                                       String keyTab){
        String zkQuorum = dataProvider.getZKNodes(clusterId);
        String zkPort = dataProvider.getZKPort(clusterId);
        boolean isSecure = false;
        Connection con = null;
        Statement stmt = null;
        ResultSet rset = null;
        boolean ret = false;
        Properties prop = new Properties();
        //prop.setProperty("zookeeper.znode.parent","hbase");
        if (conf.getTrimmed("hbase.security.authentication").equalsIgnoreCase("kerberos")) {
            isSecure = true;
            prop.setProperty("hadoop.security.authentication",conf.getTrimmed("hadoop.security.authentication"));
            prop.setProperty("hbase.security.authentication",conf.getTrimmed("hbase.security.authentication"));
            prop.setProperty("hbase.master.kerberos.principal",conf.getTrimmed("hbase.master.kerberos.principal"));
            prop.setProperty("hbase.regionserver.kerberos.principal",conf.getTrimmed("hbase.regionserver.kerberos.principal"));
        }
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        
            while (true) {
                try {
                    if (isSecure && userName != null && keyTab != null)
                        con = DriverManager.getConnection("jdbc:phoenix:"+zkQuorum+":"+zkPort+":/hbase:"+userName+":"+keyTab, prop);
                    else //Works with KRB tickets of executing user?
                        con = DriverManager.getConnection("jdbc:phoenix:"+zkQuorum+":"+zkPort+":/hbase", prop);
                    break;
                } catch (SQLException ex) {
                    System.out.println("SQLState = "+ex.getSQLState());
                    System.out.println("SQLErrCode = "+ex.getErrorCode());
                    if (ex.getErrorCode() == 726 && ex.getSQLState().equalsIgnoreCase("43M10"))
                        prop.setProperty("phoenix.schema.isNamespaceMappingEnabled","true");
                    else
                        return false;
                }
            }
            
            stmt = con.createStatement();
            rset = stmt.executeQuery("select TABLE_SCHEM, TABLE_NAME from system.catalog");
            if (rset.next()) {
               ret = true;   
            }
            stmt.close();
            con.close();
            return ret;
        } catch (SQLException ex) {
            logger.info("SQLException "+ex.getMessage());
            return false;
        } catch (ClassNotFoundException ex) {
            logger.info("ClassNotFoundException "+ex.getMessage());
            System.exit(1);
        }
        return false;
    }
}
