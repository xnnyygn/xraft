package in.xnnyygn.xraft.kvstore.message;

public class GetCommand  {

    private final String key;

    public GetCommand(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    @Override
    public String toString() {
        return "GetCommand{" +
                "key='" + key + '\'' +
                '}';
    }

}
