# HaloDB - A Lightweight Key-Value Store in Go

A hobby project implementing a key-value database from scratch in Go. Features B+ tree storage engine, partitioning,
write-ahead logging, and crash recovery.

## 🎯 Why I Built This

This project was built to learn and demonstrate:

- **Distributed Systems**: Understanding how partitioned databases work
- **Database Internals**: B+ tree implementation and storage engines
- **Data Structures**: Balanced tree structures and Bloom filters
- **Concurrency**: Thread-safe operations across partitions
- **Durability**: Crash recovery and ACID properties through WAL
- **Scalability**: Horizontal partitioning for distributed databases

## 🚀 Features

- **B+ Tree Storage Engine** - Efficient range queries and balanced tree structure
- **Hash-based Partitioning** - Horizontal scaling across multiple partitions
- **Write-Ahead Logging (WAL)** - ACID durability and crash recovery
- **In-Memory Memtable** - High-performance write buffering
- **Bloom Filters** - Fast negative lookups
- **Thread-Safe Operations** - Concurrent read/write support
- **CLI Interface** - Easy-to-use command-line tool

### Core Components

- **B+ Tree**: Balanced tree structure for efficient range queries
- **Memtable**: In-memory buffer for fast writes
- **WAL**: Write-ahead log for durability and crash recovery
- **Bloom Filter**: Probabilistic data structure for fast negative lookups
- **Partition Manager**: Hash-based partitioning for horizontal scaling

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   CLI Client    │    │  Partition 0    │    │  Partition N    │
│                 │    │                 │    │                 │
│  - Insert       │───▶│  - B+ Tree      │    │  - B+ Tree      │
│  - Get          │    │  - Memtable     │    │  - Memtable     │
│  - Delete       │    │  - WAL          │    │  - WAL          │
│  - List         │    │  - Bloom Filter │    │  - Bloom Filter │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 🛠️ Installation

```bash
git clone https://github.com/yourusername/halo-db.git
cd halo-db
go build ./cmd/halo-db
```

## 📖 Usage

### CLI Interface

```bash
# Start the database
./halo-db

# Insert a key-value pair
put key1 value1

# Retrieve a value
get key1

# Delete a key
delete key1

# List all keys
list

# Clear all data
clear

# Show statistics
stats

# Show tree info
tree

# Exit
quit
```

## 📊 Performance Characteristics

- **Write Performance**: O(log n) for B+ tree insertion
- **Read Performance**: O(log n) for B+ tree lookup
- **Memory Usage**: Configurable memtable size
- **Durability**: ACID compliance through WAL
- **Scalability**: Horizontal partitioning support

## 🔧 Configuration

Key configuration constants in `pkg/constants/constants.go`:

- `NumPartitions`: Number of partitions (default: 4)
- `MemtableSize`: Maximum memtable entries (default: 1000)
- `MaxKeys`: Maximum keys per B+ tree node (default: 4)

## 📈 Future Enhancements

- [ ] Range queries
- [ ] Background compaction
- [ ] REST API interface
- [ ] Metrics and monitoring
- [ ] Backup and restore functionality
- [ ] TTL (Time To Live) support
- [ ] Replication between partitions 