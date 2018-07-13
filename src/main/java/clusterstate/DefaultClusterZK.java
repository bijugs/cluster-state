package clusterstate;

import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.util.Map;
import java.util.HashMap;

public class DefaultClusterZK implements ClusterZK {

    private static final Map<String, String> zkMap; 
    static
    {   
        zkMap = new HashMap<String, String>();
        try {
            BufferedReader in = new BufferedReader(new FileReader(new File("./zk.txt")));
            String line;
            while ((line = in.readLine()) != null) {
                String[] parts = line.split("\\|");
                zkMap.put(parts[0],parts[1]);
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    public String getZKQuorum(String clusterName){
        return zkMap.get(clusterName);
    }

    public static void main(String args[]) {
         DefaultClusterZK z = new DefaultClusterZK();
         System.out.println(zkMap);
    }
}
