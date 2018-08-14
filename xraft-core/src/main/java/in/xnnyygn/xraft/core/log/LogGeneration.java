package in.xnnyygn.xraft.core.log;

import javax.annotation.Nonnull;
import java.io.File;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class LogGeneration extends AbstractLogDir implements Comparable<LogGeneration> {

    private static final Pattern DIR_NAME_PATTERN = Pattern.compile("log-(\\d+)");
    private final int lastIncludedIndex;

    LogGeneration(File baseDir, int lastIncludedIndex) {
        super(new File(baseDir, generateDirName(lastIncludedIndex)));
        this.lastIncludedIndex = lastIncludedIndex;
    }

    LogGeneration(File dir) {
        super(dir);
        Matcher matcher = DIR_NAME_PATTERN.matcher(dir.getName());
        if (!matcher.matches()) {
            throw new IllegalArgumentException("not a directory name of log generation, [" + dir.getName() + "]");
        }
        lastIncludedIndex = Integer.parseInt(matcher.group(1));
    }

    static boolean isValidDirName(String dirName) {
        return DIR_NAME_PATTERN.matcher(dirName).matches();
    }

    private static String generateDirName(int lastIncludedIndex) {
        return "log-" + lastIncludedIndex;
    }

    public int getLastIncludedIndex() {
        return lastIncludedIndex;
    }

    @Override
    public int compareTo(@Nonnull LogGeneration o) {
        return Integer.compare(lastIncludedIndex, o.lastIncludedIndex);
    }

    @Override
    public String toString() {
        return "LogGeneration{" +
                "dir=" + dir +
                ", lastIncludedIndex=" + lastIncludedIndex +
                '}';
    }

}
