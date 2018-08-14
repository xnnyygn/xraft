package in.xnnyygn.xraft.core.log;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;

public class LogGenerationTest {

    @Test
    public void isValidDirName() {
        Assert.assertTrue(LogGeneration.isValidDirName("log-0"));
        Assert.assertTrue(LogGeneration.isValidDirName("log-12"));
        Assert.assertTrue(LogGeneration.isValidDirName("log-123"));
        Assert.assertFalse(LogGeneration.isValidDirName("log-"));
        Assert.assertFalse(LogGeneration.isValidDirName("foo"));
        Assert.assertFalse(LogGeneration.isValidDirName("foo-0"));
    }

    @Test
    public void testCreateFromFile() {
        LogGeneration generation = new LogGeneration(new File("log-6"));
        Assert.assertEquals(6, generation.getLastIncludedIndex());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateFromFileFailed() {
        new LogGeneration(new File("foo-6"));
    }

    @Test
    public void testCreateWithBaseDir() {
        LogGeneration generation = new LogGeneration(new File("data"), 10);
        Assert.assertEquals(10, generation.getLastIncludedIndex());
        Assert.assertEquals("log-10", generation.get().getName());
    }

    @Test
    public void testCompare() {
        File baseDir = new File("data");
        LogGeneration generation = new LogGeneration(baseDir, 10);
        Assert.assertEquals(1, generation.compareTo(new LogGeneration(baseDir, 9)));
        Assert.assertEquals(0, generation.compareTo(new LogGeneration(baseDir, 10)));
        Assert.assertEquals(-1, generation.compareTo(new LogGeneration(baseDir, 11)));
    }

    @Test
    public void testGetFile() {
        LogGeneration generation = new LogGeneration(new File("data"), 20);
        Assert.assertEquals(RootDir.FILE_NAME_SNAPSHOT, generation.getSnapshotFile().getName());
        Assert.assertEquals(RootDir.FILE_NAME_ENTRIES, generation.getEntriesFile().getName());
        Assert.assertEquals(RootDir.FILE_NAME_ENTRY_OFFSET_INDEX, generation.getEntryOffsetIndexFile().getName());
    }

}