package clusterstate;

public interface ClusterDataProvider {

    public String getZKQuorum(String clusterName);
    public String getKafkaPort(String clusterName);
    public String getZKPort(String clusterName);
    public String[] getNamenodes(String clusterName);
    public String getNamenodePort(String clusterName);
}
