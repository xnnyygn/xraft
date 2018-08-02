package in.xnnyygn.xraft.kvstore;

public class MessageConstants {

    public static final int MSG_TYPE_SUCCESS = 0;
    public static final int MSG_TYPE_FAILURE = 1;
    public static final int MSG_TYPE_REDIRECT = 2;
    public static final int MSG_TYPE_ADD_SERVER_COMMAND = 10;
    public static final int MSG_TYPE_REMOVE_SERVER_COMMAND = 11;
    public static final int MSG_TYPE_GET_COMMAND = 100;
    public static final int MSG_TYPE_GET_COMMAND_RESPONSE = 101;
    public static final int MSG_TYPE_SET_COMMAND = 102;

}
