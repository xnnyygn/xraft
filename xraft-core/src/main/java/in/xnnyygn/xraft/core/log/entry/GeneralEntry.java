package in.xnnyygn.xraft.core.log.entry;

public class GeneralEntry extends AbstractEntry {

    private final byte[] commandBytes;

    public GeneralEntry(int index, int term, byte[] commandBytes) {
        super(index, term);
        this.commandBytes = commandBytes;
    }

    @Override
    public byte[] getCommandBytes() {
        return this.commandBytes;
    }

}
