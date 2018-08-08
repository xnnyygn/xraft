package in.xnnyygn.xraft.core.log;

import java.io.File;

public class NormalLogDir extends AbstractLogDir {

    public NormalLogDir(File dir) {
        super(dir);
    }

    @Override
    public String toString() {
        return "NormalLogDir{" +
                "dir=" + dir +
                '}';
    }

}
