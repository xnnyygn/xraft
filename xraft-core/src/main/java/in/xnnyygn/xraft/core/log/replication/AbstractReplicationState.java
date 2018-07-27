package in.xnnyygn.xraft.core.log.replication;

public abstract class AbstractReplicationState implements ReplicationState {

    private boolean member;

    public AbstractReplicationState(boolean member) {
        this.member = member;
    }

    @Override
    public boolean isMember() {
        return member;
    }

    public void setMember(boolean member) {
        this.member = member;
    }

}
