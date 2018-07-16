package clusterstate;

import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.util.Map;
import java.util.HashMap;

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
                  case "zkquorum":
                     while ((line = in.readLine()) != null) {
                        if (line.equalsIgnoreCase("end"))
                           break;
                        String[] parts = line.split("\\|");
                        zkMap.put(parts[0]+"-zkquorum",parts[1]);
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
        String key = clusterName+"-zkquorum";
        return zkMap.get(key);
    }

    public String getKafkaPort(String clusterName){
        String key = clusterName+"-kafka-port";
        return zkMap.get(key);
    }

    public static void main(String args[]) {
         DefaultClusterDataProvider dp = new DefaultClusterDataProvider();
         System.out.println(dp.zkMap);
    }
}
