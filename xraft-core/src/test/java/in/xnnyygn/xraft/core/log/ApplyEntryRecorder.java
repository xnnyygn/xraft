package in.xnnyygn.xraft.core.log;

import com.google.common.eventbus.Subscribe;

import java.util.ArrayList;
import java.util.List;

class ApplyEntryRecorder {

   private final List<ApplyEntryMessage> messages = new ArrayList<>();

   @Subscribe
   public void applyEntry(ApplyEntryMessage message) {
       messages.add(message);
   }

    public List<ApplyEntryMessage> getMessages() {
        return messages;
    }

}
