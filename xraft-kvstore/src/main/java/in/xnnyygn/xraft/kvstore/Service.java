package in.xnnyygn.xraft.kvstore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class Service {

    private static final Logger logger = LoggerFactory.getLogger(Service.class);
    private final Map<String, String> map = new HashMap<>();

    public void set(String key, String value) {
        logger.info("set {}", key);
        this.map.put(key, value);
    }

    public String get(String key) {
        logger.info("get {}", key);
        return this.map.get(key);
    }

}
