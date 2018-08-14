package in.xnnyygn.xraft.core.log.snapshot;

import in.xnnyygn.xraft.core.rpc.message.InstallSnapshotRpc;

public interface SnapshotBuilder<T extends Snapshot> {

    void append(InstallSnapshotRpc rpc);

    T build();

    void close();

}
