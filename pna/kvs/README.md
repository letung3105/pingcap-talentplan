# KVS

A simple key-value store that is created while following [Pingcap Talent Plan].

# Design decisions

The underlying software architecture of this project largely resembles another widely famous key-value store, [Bitcask]. During the development of the project, the following decisions were made:
1. The crate [`bincode`] is used to serialize data before they are written and persisted on disk.
    + Space-efficient binary format that is suitable for data storage.
    + Fast serialization and deserialization speed.
    + Can be used on different architectures with the default configuration.
    + The library for Rust is stable and has great supports.
    + The data is serialized along with its size, so the in-memory index does not have to store addition information about the data's size.
2. [Protocol buffer] serialization protol is used to serialize communication messages and define the type of message that can be sent between the client and the server.
    + Fast serialization and deserizalization of structured data in binary format.
    + Platform-neutral and language-neutral.
    + Using [prost] for encoding/decoding protocol buffers
3. To facilitate log compaction, the system keeps track of the number of bytes that are no longer accessed, and performs compaction when the number of wasted bytes exceeds some threshold. Similar to [Bitcask], the system creates a new log file when first started and holds exlusively write-access to that file. When the exclusive write-access is dropped for any reason, that log file will become read-only and can no longer be written to. Each log file when created will be assigned with a unique senquence number that increases for every new log file. When log compaction is performed, the system creates 2 new log files where the log file with the first next sequence number will store all the log entries that can still be accessed from previous log files and the log file with the second next sequence number will be used as the new active log file. The in-memory index will be updated so that each entry will point to the new data address after compaction. Finally, all the stale log files will be deleted permanantly from the file system.
    + Old log files are only deleted when the compaced log is created and the in-memory index is updated, as a result, if any error occurs during compaction, the system is still consistency since all log files will not be deleted.
    + Using multiple log files simplifies the compaction process.

# TODOs

Leson plan:
+ [x] Building block 01
+ [x] Project 01
+ [x] Building block 02
+ [x] Project 02
+ [x] Building block 03
+ [ ] Project 03
+ [ ] Building block 04
+ [ ] Project 04
+ [ ] Building block 05
+ [ ] Project 05


<!-- REFERENCES -->
[Pingcap Talent Plan]: https://github.com/pingcap/talent-plan
[Bitcask]: https://github.com/basho/bitcask
[`bincode`]: https://docs.rs/crate/bincode
[Protocol buffer]: https://developers.google.com/protocol-buffers/
[prost]: https://github.com/spacejam/sled