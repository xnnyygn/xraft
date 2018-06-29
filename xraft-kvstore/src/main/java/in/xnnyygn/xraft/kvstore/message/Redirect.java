package in.xnnyygn.xraft.kvstore.message;

public class Redirect {

    private final String leaderId;

    public Redirect(String leaderId) {
        this.leaderId = leaderId;
    }

    public String getLeaderId() {
        return leaderId;
    }

    @Override
    public String toString() {
        return "Redirect{" +
                "leaderId='" + leaderId + '\'' +
                '}';
    }

}
