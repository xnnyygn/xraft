package in.xnnyygn.xraft.kvstore.command;

public class GetCommand {

    private final String key;

    public GetCommand(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

}
