package in.xnnyygn.xraft.kvstore;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class ServiceSnapshotTest {

    @Test
    public void test() {
        Map<String, byte[]> map = new HashMap<>();
        map.put("foo", "a".getBytes());
        map.put("bar", "b".getBytes());

        byte[] snapshot = Service.toSnapshot(map);
        Map<String, byte[]> map2 = Service.fromSnapshot(snapshot);
        Assert.assertEquals(2, map2.size());
        Assert.assertArrayEquals("a".getBytes(), map2.get("foo"));
        Assert.assertArrayEquals("b".getBytes(), map2.get("bar"));
    }

}