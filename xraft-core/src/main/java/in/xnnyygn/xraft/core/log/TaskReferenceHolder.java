package in.xnnyygn.xraft.core.log;

import in.xnnyygn.xraft.core.log.entry.GroupConfigEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskReferenceHolder {

    private static final Logger logger = LoggerFactory.getLogger(TaskReferenceHolder.class);
    private EntryTaskReference reference;

    public void set(EntryTaskReference reference) {
        this.reference = reference;
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public boolean awaitPreviousGroupConfigChange(GroupConfigEntry lastCommittedGroupConfigEntry,
                                                  EntryTaskReference newReference, int timeout) {
        if (lastCommittedGroupConfigEntry == null) {
            return true;
        }
        if (reference == null) {
            logger.warn("no task reference of last group config entry {}", lastCommittedGroupConfigEntry.getIndex());
            newReference.done(TaskReference.Result.UNKNOWN);
            return false;
        }
        logger.info("wait for last group config entry {} to be committed", lastCommittedGroupConfigEntry.getIndex());
        try {
            if (!reference.await(timeout)) {
                newReference.done(TaskReference.Result.TIMEOUT);
                return false;
            }
            return true;
        } catch (InterruptedException e) {
            logger.warn("interrupted");
            newReference.done(TaskReference.Result.UNKNOWN);
            return false;
        }
    }

    public void setEntryIndexForAddNode(int entryIndex) {
        if (!(reference instanceof AddNodeTaskReference)) {
            logger.warn("task reference not present or not add node task");
            return;
        }
        reference.setEntryIndex(entryIndex);
    }

    public void doneForAddNode(TaskReference.Result result) {
        if (!(reference instanceof AddNodeTaskReference)) {
            logger.warn("task reference not present or not add node task");
            return;
        }
        done(result);
    }

    public void done(int entryIndex) {
        if (reference == null) {
            logger.warn("task reference not present");
            return;
        }
        if (reference.getEntryIndex() != entryIndex) {
            logger.warn("unexpected task reference entry index, expected " + entryIndex + ", but was " + reference.getEntryIndex());
            return;
        }
        done(TaskReference.Result.OK);
    }

    private void done(TaskReference.Result result) {
        assert reference != null;
        reference.done(result);
        reference = null;
    }

}
