# Raft Distributed KV-Store

The distributed kv store is in kv_store.rs and it uses the template in lib by @jonhoo and the raft interface in src/raft.rs.

## Testing

In order to test it you need to install maelstrom first:
https://github.com/jepsen-io/maelstrom/blob/main/doc/01-getting-ready/index.md#prerequisites

I test it with this workload:
```
./maelstrom test -w lin-kv --bin ../flyio-challenge/target/release/kv_store  --time-limit 60 --node-count 3 --concurrency 4n --rate 30 --nemesis partition --nemesis-interval 10
```


You can see logs and plots of your test runs under localhost:8080 if you run

```
./maelstrom serve
```

Additional info: i followed this link loosely
https://github.com/jepsen-io/maelstrom/blob/main/doc/06-raft/02-leader-election.md