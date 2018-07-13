package clusterstate;

public class ClusterStatus {
    private final String id;
    private final String act;
    private final String status;
    private final String content;

    public ClusterStatus(String id, String act, String status,String content) {
        this.id = id;
        this.act = act;
        this.status = status;
        this.content = content;
    }

    public String getId() {
        return id;
    }

    public String getAct() {
        return act;
    }

    public String getContent() {
        return content;
    }

    public String getStatus() {
        return status;
    }
}
