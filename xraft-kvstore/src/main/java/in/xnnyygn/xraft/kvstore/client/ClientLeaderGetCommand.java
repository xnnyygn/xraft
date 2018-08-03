package in.xnnyygn.xraft.kvstore.client;

public class ClientLeaderGetCommand implements Command {

    @Override
    public String getName() {
        return "client-leader-get";
    }

    @Override
    public void execute(String arguments, CommandContext context) {
        System.out.println(context.getClientLeader());
    }

}
