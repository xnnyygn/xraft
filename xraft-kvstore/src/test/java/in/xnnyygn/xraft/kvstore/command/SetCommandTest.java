package in.xnnyygn.xraft.kvstore.command;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class SetCommandTest {

    @Test
    public void test() {
        SetCommand command = new SetCommand("x", "1");
        byte[] commandBytes = command.toBytes();
        System.out.println(Arrays.toString(commandBytes));
        SetCommand command2 = SetCommand.fromBytes(commandBytes);
        Assert.assertEquals(command.getKey(), command2.getKey());
        Assert.assertEquals(command.getValue(), command2.getValue());
    }

}