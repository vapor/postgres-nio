<img src="https://user-images.githubusercontent.com/1342803/59061804-5548e280-8872-11e9-819f-14f19f16fcb6.png" height="64" alt="PostgresNIO">

[![SSWG Incubating Badge](https://img.shields.io/badge/sswg-incubating-green.svg)][SSWG Incubation]
[![Documentation](http://img.shields.io/badge/read_the-docs-2196f3.svg)][Documentation]
[![Team Chat](https://img.shields.io/discord/431917998102675485.svg)][Team Chat]
[![MIT License](http://img.shields.io/badge/license-MIT-brightgreen.svg)][MIT License]
[![Continuous Integration](https://github.com/vapor/postgres-nio/actions/workflows/test.yml/badge.svg)][Continuous Integration]
[![Swift 5.4](http://img.shields.io/badge/swift-5.4-brightgreen.svg)][Swift 5.4]
<br>
<br>

🐘 Non-blocking, event-driven Swift client for PostgreSQL built on [SwiftNIO].

Features:

- A [`PostgresConnection`] which allows you to connect to, authorize with, query, and retrieve results from a PostgreSQL server
- An async/await interface that supports backpressure 
- Automatic conversions between Swift primitive types and the Postgres wire format
- Integrated with the Swift server ecosystem, including use of [SwiftLog].
- Designed to run efficiently on all supported platforms (tested extensively on Linux and Darwin systems)
- Support for `Network.framework` when available (e.g. on Apple platforms)

PostgresNIO does not provide a `ConnectionPool` as of today, but this is a [feature high on our list](https://github.com/vapor/postgres-nio/issues/256). If you need a `ConnectionPool` today, please have a look at Vapor's [PostgresKit]. 

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

A connection must be created on a SwiftNIO `EventLoop`. In most server use cases, an 
`EventLoopGroup` is created at app startup and closed during app shutdown.

```swift
import NIOPosix

let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)

// Much later
try eventLoopGroup.syncShutdown()
```

A [`Logger`] is also required.

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
  on: eventLoopGroup.next(),
  configuration: config,
  id: 1,
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

A type must implement the [`PostgresDecodable`] protocol in order to be decoded from a row. PostgresNIO provides default implementations for most of Swift's builtin types, as well as some types provided by Foundation:

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

While this looks at first glance like a classic case of [SQL injection](https://en.wikipedia.org/wiki/SQL_injection) 😱, PostgresNIO's API ensures that this usage is safe. The first parameter of the [`query(_:logger:)`] method is not a plain `String`, but a [`PostgresQuery`], which implements Swift's `ExpressibleByStringInterpolation` protocol. PostgresNIO uses the literal parts of the provided string as the SQL query and replaces each interpolated value with a parameter binding. Only values which implement the [`PostgresEncodable`] protocol may be interpolated in this way. As with [`PostgresDecodable`], PostgresNIO provides default implementations for most common types.

Some queries do not receive any rows from the server (most often `INSERT`, `UPDATE`, and `DELETE` queries with no `RETURNING` clause, not to mention most DDL queries). To support this, the [`query(_:logger:)`] method is marked `@discardableResult`, so that the compiler does not issue a warning if the return value is not used. 

## Security

Please see [SECURITY.md] for details on the security process.

[SSWG Incubation]: https://github.com/swift-server/sswg/blob/main/process/incubation.md#graduated-level
[Documentation]: https://swiftpackageindex.com/vapor/postgres-nio/documentation
[Team Chat]: https://discord.gg/vapor
[MIT License]: LICENSE
[Continuous Integration]: https://github.com/vapor/postgres-nio/actions
[Swift 5.4]: https://swift.org
[Security.md]: https://github.com/vapor/.github/blob/main/SECURITY.md

[`PostgresConnection`]: https://api.vapor.codes/postgres-nio/main/PostgresNIO/PostgresConnection/
[`query(_:logger:)`]: https://api.vapor.codes/postgres-nio/main/PostgresNIO/PostgresConnection/#postgresconnection.query(_:logger:file:line:)
[`PostgresQuery`]: https://api.vapor.codes/postgres-nio/main/PostgresNIO/PostgresQuery/
[`PostgresRow`]: https://api.vapor.codes/postgres-nio/main/PostgresNIO/PostgresRow/
[`PostgresRowSequence`]: https://api.vapor.codes/postgres-nio/main/PostgresNIO/PostgresRowSequence/
[`PostgresDecodable`]: https://api.vapor.codes/postgres-nio/main/PostgresNIO/PostgresDecodable/
[`PostgresEncodable`]: https://api.vapor.codes/postgres-nio/main/PostgresNIO/PostgresEncodable/

[PostgresKit]: https://github.com/vapor/postgres-kit

[SwiftNIO]: https://github.com/apple/swift-nio
[SwiftLog]: https://github.com/apple/swift-log
[`Logger`]: https://apple.github.io/swift-log/docs/current/Logging/Structs/Logger.html
