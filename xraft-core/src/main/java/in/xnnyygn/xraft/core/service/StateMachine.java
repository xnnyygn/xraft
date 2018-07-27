package in.xnnyygn.xraft.core.service;

import in.xnnyygn.xraft.core.log.entry.CommandApplier;
import in.xnnyygn.xraft.core.log.snapshot.SnapshotApplier;
import in.xnnyygn.xraft.core.log.snapshot.SnapshotGenerator;

public interface StateMachine extends CommandApplier, SnapshotGenerator, SnapshotApplier {
}
