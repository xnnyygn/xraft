package in.xnnyygn.xraft.core.log;

import com.google.common.eventbus.Subscribe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommandApplyAdapter {

    private static Logger logger = LoggerFactory.getLogger(CommandApplyAdapter.class);
    private final CommandApplyListener listener;

    public CommandApplyAdapter(CommandApplyListener listener) {
        this.listener = listener;
    }

    @Subscribe
    public void onReceive(ApplyEntryMessage message) {
        Entry entry = message.getEntry();
        logger.debug("apply entry {}", entry);
        this.listener.applyCommand(entry.getIndex(), entry.getCommand());
    }

}
