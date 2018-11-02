package clusterstate;

import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.OutputStreamWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.security.UserGroupInformation;

public class HdfsState {

    private static final Logger logger = LoggerFactory.getLogger(HdfsState.class);

    public static boolean testHdfs(ClusterDataProvider dataProvider, Configuration conf, String clusterId, String hdfsPath){
        String nnPort = dataProvider.getNamenodePort(clusterId);
        String[] nameNodes = dataProvider.getNamenodes(clusterId);
        if (nnPort == null || nameNodes == null) {
            logger.info("Namenode and port unknown for cluster "+clusterId);
            return false;
        }
        for (String nameNode : nameNodes) {
            conf.set("fs.defaultFS", "hdfs://"+nameNode);
            try {
                FileSystem  fs = FileSystem.get(conf);
                FSDataOutputStream outStream = null;
                if (!fs.exists(new Path(hdfsPath+"/smoke-test"))) 
                    outStream = fs.create(new Path(hdfsPath+"/smoke-test"),false);
                else
                    outStream = fs.append(new Path(hdfsPath+"/smoke-test"));
                BufferedWriter bw = new BufferedWriter( new OutputStreamWriter( outStream, "UTF-8" ) );   
                bw.write("1,Ant\n");
                bw.write("2,Bat");
                bw.close();
                fs.close();
                return true;
            } catch (RemoteException rEx) {
                if (rEx.getClassName().equals("org.apache.hadoop.ipc.StandbyException"))
                    logger.info("Caught StandbyException "+rEx.getClassName());
                else
                    return false;
            } catch (Exception ex) {
                logger.info(ex.getMessage());
                return false;
            }
        }
        return false;
    }
}
