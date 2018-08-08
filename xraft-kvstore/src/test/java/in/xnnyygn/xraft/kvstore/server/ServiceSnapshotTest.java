package in.xnnyygn.xraft.kvstore.server;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ServiceSnapshotTest {

    @Test
    public void test() throws IOException {
        Map<String, byte[]> map = new HashMap<>();
        map.put("foo", "a".getBytes());
        map.put("bar", "b".getBytes());

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        Service.toSnapshot(map, output);
        Map<String, byte[]> map2 = Service.fromSnapshot(new ByteArrayInputStream(output.toByteArray()));
        Assert.assertEquals(2, map2.size());
        Assert.assertArrayEquals("a".getBytes(), map2.get("foo"));
        Assert.assertArrayEquals("b".getBytes(), map2.get("bar"));
    }

}