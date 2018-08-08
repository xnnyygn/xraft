package in.xnnyygn.xraft.core.log.snapshot;

import in.xnnyygn.xraft.core.rpc.message.InstallSnapshotRpc;

public interface SnapshotBuilder {

    void append(InstallSnapshotRpc rpc);

    Snapshot build();

    void close();

}
