namespace java in.xnnyygn.xraft.kvstore

exception Redirect {
    1:string leaderId;
}

struct GetResult {
    1:bool found;
    2:string value;
}

service KVStore {
    void Set(1:string key, 2:string value) throws (1:Redirect redirect);
    GetResult Get(1:string key) throws (1:Redirect redirect);
}