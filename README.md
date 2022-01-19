<img src="https://user-images.githubusercontent.com/1342803/59061804-5548e280-8872-11e9-819f-14f19f16fcb6.png" height="64" alt="PostgresNIO">
<br>
<a href="https://docs.vapor.codes/4.0/">
    <img src="http://img.shields.io/badge/read_the-docs-2196f3.svg" alt="Documentation">
</a>
<a href="https://discord.gg/vapor">
    <img src="https://img.shields.io/discord/431917998102675485.svg" alt="Team Chat">
</a>
<a href="LICENSE">
    <img src="http://img.shields.io/badge/license-MIT-brightgreen.svg" alt="MIT License">
</a>
<a href="https://github.com/vapor/postgres-nio/actions">
    <img src="https://github.com/vapor/postgres-nio/workflows/test/badge.svg" alt="Continuous Integration">
</a>
<a href="https://swift.org">
    <img src="http://img.shields.io/badge/swift-5.2-brightgreen.svg" alt="Swift 5.2">
</a>
<br>
<br>

🐘 Non-blocking, event-driven Swift client for PostgreSQL built on [SwiftNIO](https://github.com/apple/swift-nio).

### Major Releases

The table below shows a list of PostgresNIO major releases alongside their compatible NIO and Swift versions. 

|Version|NIO|Swift|SPM|
|-|-|-|-|
|1.0|2.0+|5.2+|`from: "1.0.0"`|

Use the SPM string to easily include the dependendency in your `Package.swift` file.

```swift
.package(url: "https://github.com/vapor/postgres-nio.git", from: ...)
```

### Supported Platforms

PostgresNIO supports the following platforms:

- Ubuntu 16.04+
- macOS 10.15+

### Security

Please see [SECURITY.md](https://github.com/vapor/.github/blob/main/SECURITY.md) for details on the security process.

## Overview

PostgresNIO is a client package for connecting to, authorizing, and querying a PostgreSQL server. At the heart of this module are NIO channel handlers for parsing and serializing messages in PostgreSQL's proprietary wire protocol. These channel handlers are combined in a request / response style connection type that provides a convenient, client-like interface for performing queries. 

Support for both simple (text) and parameterized (binary) querying is provided out of the box alongside a `PostgresData` type that handles conversion between PostgreSQL's wire format and native Swift types.

### Motivation

Most Swift implementations of Postgres clients are based on the [libpq](https://www.postgresql.org/docs/11/libpq.html) C library which handles transport internally. Building a library directly on top of Postgres' wire protocol using SwiftNIO should yield a more reliable, maintainable, and performant interface for PostgreSQL databases.

### Goals

This package is meant to be a low-level, unopinionated PostgreSQL wire-protocol implementation for Swift. The hope is that higher level packages can share PostgresNIO as a foundation for interacting with PostgreSQL servers without needing to duplicate complex logic.

Because of this, PostgresNIO excludes some important concepts for the sake of simplicity, such as:

- Connection pooling
- Swift `Codable` integration
- Query building

If you are looking for a PostgreSQL client package to use in your project, take a look at these higher-level packages built on top of PostgresNIO:

- [`vapor/postgres-kit`](https://github.com/vapor/postgresql)

### Dependencies

This package has four dependencies:

- [`apple/swift-nio`](https://github.com/apple/swift-nio) for IO
- [`apple/swift-nio-ssl`](https://github.com/apple/swift-nio-ssl) for TLS
- [`apple/swift-log`](https://github.com/apple/swift-log) for logging
- [`apple/swift-metrics`](https://github.com/apple/swift-metrics) for metrics

This package has no additional system dependencies.

## API Docs

Check out the [PostgresNIO API docs](https://api.vapor.codes/postgres-nio/main/PostgresNIO/) for a detailed look at all of the classes, structs, protocols, and more.

## Getting Started

This section will provide a quick look at using PostgresNIO.

### Creating a Connection

The first step to making a query is creating a new `PostgresConnection`. The minimum requirements to create one are a `SocketAddress` and `EventLoop`. 

```swift
import PostgresNIO

let eventLoop: EventLoop = ...
let conn = try PostgresConnection.connect(
    to: .makeAddressResolvingHost("my.psql.server", port: 5432),
    on: eventLoop
).wait()
defer { try! conn.close().wait() }
```

Note: These examples will make use of `wait()` for simplicity. This is appropriate if you are using PostgresNIO on the main thread, like for a CLI tool or in tests. However, you should never use `wait()` on an event loop.

There are a few ways to create a `SocketAddress`:

- `init(ipAddress: String, port: Int)`
- `init(unixDomainSocketPath: String)`
- `makeAddressResolvingHost(_ host: String, port: Int)`

There are also some additional arguments you can supply to `connect`. 

- `tlsConfiguration` An optional `TLSConfiguration` struct. If supplied, the PostgreSQL connection will be upgraded to use SSL.
- `serverHostname` An optional `String` to use in conjunction with `tlsConfiguration` to specify the server's hostname. 

`connect` will return a future `PostgresConnection`, or an error if it could not connect. Make sure you close the connection before it deinitializes.

### Authentication

Once you have a connection, you will need to authenticate with the server using the `authenticate` method.

```swift
try conn.authenticate(
    username: "your_username",
    database: "your_database",
    password: "your_password"
).wait()
```

This requires a username. You may supply a database name and password if needed. 

### Database Protocol

Interaction with a server revolves around the `PostgresDatabase` protocol. This protocol includes methods like `query(_:)` for executing SQL queries and reading the resulting rows. 

`PostgresConnection` is the default implementation of `PostgresDatabase` provided by this package. Assume `db` here is the connection from the previous example.

```swift
import PostgresNIO

let db: PostgresDatabase = ...
// now we can use client to do queries
```

### Simple Query

Simple (or text) queries allow you to execute a SQL string on the connected PostgreSQL server. These queries do not support binding parameters, so any values sent must be escaped manually.

These queries are most useful for schema or transactional queries, or simple selects. Note that values returned by simple queries will be transferred in the less efficient text format. 

`simpleQuery` has two overloads, one that returns an array of rows, and one that accepts a closure for handling each row as it is returned.

```swift
let rows = try db.simpleQuery("SELECT version()").wait()
print(rows) // [["version": "12.x.x"]]

try db.simpleQuery("SELECT version()") { row in
    print(row) // ["version": "12.x.x"]
}.wait()
```

### Parameterized Query

Parameterized (or binary) queries allow you to execute a SQL string on the connected PostgreSQL server. These queries support passing bound parameters as a separate argument. Each parameter is represented in the SQL string using incrementing placeholders, starting at `$1`. 

These queries are most useful for selecting, inserting, and updating data. Data for these queries is transferred using the highly efficient binary format. 

Just like `simpleQuery`, `query` also offers two overloads. One that returns an array of rows, and one that accepts a closure for handling each row as it is returned.

```swift
let rows = try db.query("SELECT * FROM planets WHERE name = $1", ["Earth"]).wait()
print(rows) // [["id": 42, "name": "Earth"]]

try db.query("SELECT * FROM planets WHERE name = $1", ["Earth"]) { row in
    print(row) // ["id": 42, "name": "Earth"]
}.wait()
```

### Rows and Data

Both `simpleQuery` and `query` return the same `PostgresRow` type. Columns can be fetched from the row using the `column(_: String)` method.

```swift
let row: PostgresRow = ...
let version = row.column("version")
print(version) // PostgresData?
```

`PostgresRow` columns are stored as `PostgresData`. This struct contains the raw bytes returned by PostgreSQL as well as some information for parsing them, such as:

- Postgres column type
- Wire format: binary or text
- Value as array of bytes

`PostgresData` has a variety of convenience methods for converting column data to usable Swift types.

```swift
let data: PostgresData= ...

print(data.string) // String?

// Postgres only supports signed Ints.
print(data.int) // Int?
print(data.int16) // Int16?
print(data.int32) // Int32?
print(data.int64) // Int64?

// 'char' can be interpreted as a UInt8. 
// It will show in db as a character though. 
print(data.uint8) // UInt8?

print(data.bool) // Bool?

print(try data.jsonb(as: Foo.self)) // Foo?

print(data.float) // Float?
print(data.double) // Double?

print(data.date) // Date?
print(data.uuid) // UUID?

print(data.numeric) // PostgresNumeric?
```

`PostgresData` is also used for sending data _to_ the server via parameterized values. To create `PostgresData` from a Swift type, use the available intializer methods. 
