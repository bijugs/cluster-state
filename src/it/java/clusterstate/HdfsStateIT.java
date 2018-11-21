package clusterstate;

import org.apache.hadoop.conf.Configuration;

import org.junit.Assert;
import org.junit.Test;

public class HdfsStateIT {

    ClusterDataProvider dataProvider = new DefaultClusterDataProvider();

    @Test
    public void itHdfsTest() {
        try {
            Configuration conf = CommonUtils.createConfiguration(true, "ADDEV.BLOOMBERG.COM", null, null);
            boolean testResponse = HdfsState.testHdfs(dataProvider, conf, "dnj2-jr", "/user/"+System.getProperty("user.name"));
            Assert.assertTrue(testResponse);
        } catch (Exception ex) {
           System.out.println("Exception in itHDFSTest");
           System.exit(1);
        }  
    }
}
