<img src="https://user-images.githubusercontent.com/1342803/59061804-5548e280-8872-11e9-819f-14f19f16fcb6.png" height="64" alt="PostgresNIO">

<a href="https://github.com/swift-server/sswg/blob/main/process/incubation.md#graduated-level">
    <img src="https://img.shields.io/badge/sswg-incubating-green.svg" alt="SSWG Incubating">
</a>
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

🐘 Non-blocking, event-driven Swift client for PostgreSQL built on [SwiftNIO].

Features:

- A `PostgresConnection` which allows you to connect to, authorize with, query, and retrieve results from a PostgreSQL server
- An async/await interface that supports backpressure 
- Automatic conversions between Swift primitive types and the Postgres wire format
- Integrated with the Swift server ecosystem, including use of [SwiftLog].
- Designed to run efficiently on all supported platforms (tested extensively on Linux and Darwin systems)
- Support for `Network.framework` when available (e.g. on Apple platforms)

PostgresNIO does not have a `ConnectionPool` as of today, but this is a feature high on our list. If 
you need a `ConnectionPool` today, please have a look at Vapor's [PostgresKit]. 

## API Docs

Check out the [PostgresNIO API docs](https://api.vapor.codes/postgres-nio/main/PostgresNIO/) for a 
detailed look at all of the classes, structs, protocols, and more.

## Getting started

#### Adding the dependency

Add `PostgresNIO` as dependency to your `Package.swift`:

```swift
  dependencies: [
    .package(url: "https://github.com/vapor/postgres-nio.git", from: "1.8.0"),
    ...
  ]
```

Add `PostgresNIO` to the target you want to use it in:
```swift
  targets: [
    .target(name: "MyFancyTarget", dependencies: [
      .product(name: "PostgresNIO", package: "postgres-nio"),
    ])
  ]
```

#### Creating a connection

PostgresNIO is a server-side

To create a connection, first create a connection configuration object:

```swift
import PostgresNIO

let config = PostgresConnection.Configuration(
   connection: .init(
     host: "localhost",
     port: 5432
   ),
   authentication: .init(
     username: "my_username",
     database: "my_database",
     password: "my_password"
   ),
   tls: .disable
)
```

A connection must be created on a SwiftNIO `EventLoop`. In most server use cases, an`EventLoopGroup` is created at app startup and closed during app shutdown.

```swift
import NIOPosix

let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)

// Much later
try eventLoopGroup.syncShutdown()
```

A `Logger` is also required.

```swift
import Logging

let logger = Logger(label: "postgres-logger")
```

Now we can put it together:

```swift
import PostgresNIO
import NIOPosix
import Logging

let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
let logger = Logger(label: "postgres-logger")

let config = PostgresConnection.Configuration(
   connection: .init(
     host: "localhost",
     port: 5432
   ),
   authentication: .init(
     username: "my_username",
     database: "my_database",
     password: "my_password"
   ),
   tls: .disable
)

let connection = try await PostgresConnection.connect(
  on eventLoop: eventLoopGroup.next(),
  configuration: config,
  id connectionID: 1,
  logger: logger
)

// Close your connection once done
try await connection.close()

// Shutdown the EventLoopGroup, once all connections are closed.
try eventLoopGroup.syncShutdown()
```

#### Querying

Once a connection is established, queries can be sent to the server. This is very straightforward:

```swift
let rows = try await connection.query("SELECT id, username, birthday FROM users", logger: logger)
```

The query will return a [`PostgresRowSequence`], which is an AsyncSequence of [`PostgresRow`]s. The rows can be iterated one-by-one: 

```swift
for try await row in rows {
  // do something with the row
}
```

#### Decoding from PostgresRow

However, in most cases it is much easier to request a row's fields as a set of Swift types:

```swift
for try await (id, username, birthday) in rows.decode((Int, String, Date).self, context: .default) {
  // do something with the datatypes.
}
```

A type must implement the `PostgresDecodable` protocol in order to be decoded from a row. PostgresNIO provides default implementations for most of Swift's builtin types, as well as some types provided by Foundation:

- `Bool`
- `Bytes`, `Data`, `ByteBuffer`
- `Date`
- `UInt8`, `Int16`, `Int32`, `Int64`, `Int`
- `Float`, `Double`
- `String`
- `UUID`

#### Querying with parameters

Sending parameterized queries to the database is also supported (in the coolest way possible):

```swift
let id = 1
let username = "fancyuser"
let birthday = Date()
try await connection.query("""
  INSERT INTO users (id, username, birthday) VALUES (\(id), \(username), \(birthday))
  """, 
  logger: logger
)
```

While this looks at first glance like a classic case of [SQL injection](https://en.wikipedia.org/wiki/SQL_injection) 😱, PostgresNIO's API ensures that this usage is safe. The first parameter of the `query(_:logger:)` method is not a plain `String`, but a `PostgresQuery`, which implements Swift's `ExpressibleByStringInterpolation` protocol. PostgresNIO uses the literal parts of the provided string as the SQL query and replaces each interpolated value with a parameter binding. Only values which implement the `PostgresEncodable` protocol may be interpolated in this way. As with `PostgresDecodable`, PostgresNIO provides default implementations for most common types.

Some queries do not receive any rows from the server (most often `INSERT`, `UPDATE`, and `DELETE` queries with no `RETURNING` clause, not to mention most DDL queries). To support this, the `query(_:logger:)` method is marked `@discardableResult`, so that the compiler does not issue a warning if the return value is not used. 

## Security

Please see [SECURITY.md](https://github.com/vapor/.github/blob/main/SECURITY.md) for details on the security process.

[EventLoopGroupConnectionPool]: https://github.com/vapor/async-kit/blob/main/Sources/AsyncKit/ConnectionPool/EventLoopGroupConnectionPool.swift
[AsyncKit]: https://github.com/vapor/async-kit/
[SwiftNIO]: https://github.com/apple/swift-nio
[SwiftLog]: https://github.com/apple/swift-log
