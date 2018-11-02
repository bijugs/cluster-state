package clusterstate;

public interface ClusterDataProvider {

    public String getZKQuorum(String clusterName);
    public String getZKNodes(String clusterName);
    public String getKafkaPort(String clusterName);
    public String getZKPort(String clusterName);
    public String[] getNamenodes(String clusterName);
    public String getNamenodePort(String clusterName);
    public String[] getHiveServers(String clusterName);
    public String getHiveServerPort(String clusterName);
    public String[] getYarnRMServers(String clusterName);
    public String getYarnRMAMIPort(String clusterName);
}
