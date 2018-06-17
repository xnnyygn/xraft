package in.xnnyygn.xraft.server;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class ServerGroup implements Iterable<AbstractServer> {

    private Map<ServerId, AbstractServer> serverMap;

    public ServerGroup() {
        this.serverMap = new HashMap<>();
    }

    // TODO rename to add
    public void addServer(AbstractServer server) {
        this.serverMap.put(server.getId(), server);
    }

    // TODO rename to getCount
    public int getServerCount() {
        return this.serverMap.size();
    }

    @Deprecated
    public ServerId getSelfId() {
        throw new UnsupportedOperationException();
    }

    public void startAll() {
        for (AbstractServer server : serverMap.values()) {
            if (server instanceof Server) {
                ((Server) server).start();
            }
        }
    }

    public void stopAll() throws Exception {
        for (AbstractServer server : serverMap.values()) {
            if (server instanceof Server) {
                ((Server) server).stop();
            }
        }
    }

    @Override
    public Iterator<AbstractServer> iterator() {
        return this.serverMap.values().iterator();
    }

    // TODO rename to find
    public AbstractServer findServer(ServerId serverId) {
        return this.serverMap.get(serverId);
    }

}
