package in.xnnyygn.xraft.core.log.snapshot;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;

public class FileSnapshotWriterTest {

    @Test
    public void test() throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        FileSnapshotWriter writer = new FileSnapshotWriter(output, 1, 2);
        byte[] data = "test".getBytes();
        writer.write(data);
        writer.close();

        DataInputStream dataInput = new DataInputStream(new ByteArrayInputStream(output.toByteArray()));
        Assert.assertEquals(1, dataInput.readInt());
        Assert.assertEquals(2, dataInput.readInt());
        byte[] data2 = new byte[4];
        dataInput.read(data2);
        Assert.assertArrayEquals(data, data2);
    }

}