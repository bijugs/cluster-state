package clusterstate;

public interface ClusterDataProvider {

    public String getZKQuorum(String clusterName);
    public String getKafkaPort(String clusterName);
    public String getZKPort(String clusterName);

}
