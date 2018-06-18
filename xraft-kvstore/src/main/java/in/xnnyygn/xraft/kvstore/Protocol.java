package in.xnnyygn.xraft.kvstore;

import java.io.*;

public class Protocol {

    public void toWire(Object payload, OutputStream output) throws IOException {
        ObjectOutputStream objectOutput = new ObjectOutputStream(output);
        objectOutput.writeObject(payload);
    }

    public Object fromWire(InputStream input) throws IOException {
        ObjectInputStream objectInput = new ObjectInputStream(input);
        try {
            return objectInput.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
    }

}
