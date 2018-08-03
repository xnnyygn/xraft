package in.xnnyygn.xraft.kvstore.client;

public class ClientGetLeaderCommand implements Command {

    @Override
    public String getName() {
        return "client-get-leader";
    }

    @Override
    public void execute(String arguments, CommandContext context) {
        System.out.println(context.getClientLeader());
    }

}
