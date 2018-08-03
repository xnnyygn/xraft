package in.xnnyygn.xraft.kvstore.client;

import in.xnnyygn.xraft.core.service.NoAvailableServerException;

public class RaftAddServerCommand implements Command {

    @Override
    public String getName() {
        return "raft-add-server";
    }

    @Override
    public void execute(String arguments, CommandContext context) {
        // <node-id> <host> <port-raft-server>
        String[] pieces = arguments.split("\\s");
        if (pieces.length != 3) {
            throw new IllegalArgumentException("usage " + getName() + " <node-id> <host> <port-raft-server>");
        }

        String nodeId = pieces[0];
        String host = pieces[1];
        int port;
        try {
            port = Integer.parseInt(pieces[2]);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("illegal port [" + pieces[2] + "]");
        }

        try {
            context.getClient().addServer(nodeId, host, port);
        } catch (NoAvailableServerException e) {
            System.err.println(e.getMessage());
        }
    }

}
