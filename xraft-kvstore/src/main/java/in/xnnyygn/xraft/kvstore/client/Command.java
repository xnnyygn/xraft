package in.xnnyygn.xraft.kvstore.client;

public interface Command {

    String getName();

    void execute(String arguments, CommandContext context);

}
