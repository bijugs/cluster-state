package clusterstate;

import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.util.Map;
import java.util.HashMap;

/*
 Reads a file named z.txt with following data and loads into a Map
=====
nodes
cluster-name-a|component1|host1,host2,host3
cluster-name-a|component2|host1,host2,host3
cluster-name-b|component1|host1,host2,host3
cluster-name-b|component2|host1,host2,host3
end
port
cluster-name-a|component1|porta
cluster-name-a|component2|portb
cluster-name-b|component1|porta
cluster-name-b|component2|portb
end
*/
public class DefaultClusterDataProvider implements ClusterDataProvider {

    private static final Map<String, String> zkMap; 
    static
    {   
        zkMap = new HashMap<String, String>();
        try {
            BufferedReader in = new BufferedReader(new FileReader(new File("./zk.txt")));
            String line;
            while ((line = in.readLine()) != null) {
               switch(line) {
                  case "nodes":
                     while ((line = in.readLine()) != null) {
                        if (line.equalsIgnoreCase("end"))
                           break;
                        String[] parts = line.split("\\|");
                        zkMap.put(parts[0]+"-"+parts[1],parts[2]);
                     }
                     break;
                  case "port":
                     while ((line = in.readLine()) != null) {
                        if (line.equalsIgnoreCase("end"))
                           break;
                        String[] parts = line.split("\\|");
                        zkMap.put(parts[0]+"-"+parts[1]+"-port",parts[2]);
                     }
                     break;
                  default:
                     //nop
                     break;
               }
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    public String getZKQuorum(String clusterName){
        String key = clusterName+"-zookeeper";
        String zkNodes = zkMap.get(key);
        if (zkNodes == null)
           return null;
        String zkPort = getZKPort(clusterName);
        String[] zkNodeList = zkNodes.split(",");
        String ret = "";
        int i = 0;
        for (i = 0; i < zkNodeList.length-1; i++) {
           ret += zkNodeList[i] + ":" + zkPort + ",";
        }
        ret += zkNodeList[i] + ":" + zkPort;
        return ret;
    }

    public String getKafkaPort(String clusterName){
        String key = clusterName+"-kafka-port";
        return zkMap.get(key);
    }

    public String getZKPort(String clusterName){
        String key = clusterName+"-zookeeper-port";
        return zkMap.get(key);
    }

    public String[] getNamenodes(String clusterName) {
        String key = clusterName+"-namenode";
        String nameNodes = zkMap.get(key);
        if (nameNodes == null)
           return null;
        return nameNodes.split(",");        
    }

    public String getNamenodePort(String clusterName){
        String key = clusterName+"-namenode-port";
        return zkMap.get(key);
    }

    public static void main(String args[]) {
         DefaultClusterDataProvider dp = new DefaultClusterDataProvider();
         System.out.println(dp.zkMap);
    }
}
