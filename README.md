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

- A PostgresConnection that allows you to connect to, authorize and query a PostgreSQL server
- An async/await interface that supports backpressure 
- Converting Swift primitive types to and from the Postgres wire format
- Integrates with the Swift server ecosystem, by using swift-log
- Supports running Linux through NIOPosix on running Darwin through Network.framework and NIOTS

PostgresNIO does not have a ConnectionPool as of today – but this is a feature high on our list. If 
you need a ConnectionPool today, please have a look at Vapor's [PostgresKit]. 

## API Docs

Check out the [PostgresNIO API docs](https://api.vapor.codes/postgres-nio/main/PostgresNIO/) for a 
detailed look at all of the classes, structs, protocols, and more.

## Getting started

#### Adding the dependency

Add `PostgresNIO` as dependency to your Package.swift:

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

To create a connection you should first create a connection configuration object.

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

Next you will need a SwiftNIO EventLoop to create your connection on. In most server use-cases you 
create the EventLoopGroup on your app's startup and you close it once you shutdown your app.

```swift
import NIOPosix

let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)

// Much later
try eventLoopGroup.syncShutdown()
```

Last you will need a Logger.

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

Once you have a connection you can start to query your server. This is very straightforward:

```swift
let rows = try await connection.query("SELECT id, username, birthday FROM users", logger: logger)
```

The query will return a [`PostgresRowSequence`], which is an AsyncSequence of [`PostgresRow`]s. You can 
consume the rows 1 by 1: 

```swift
for try await row in rows {
  // do something with the row
}
```

#### Decoding from PostgresRow

However in most cases you want to cast a row directly into Swift primitive datatypes:

```swift
for try await (id, username, birthday) in rows.decode((Int, String, Date).self, context: .default) {
  // do something with the datatypes.
}
```

To be able to decode a Postgres value to a Swift struct, the type needs to implement the 
`PostgresDecodable` protocol. PostgresNIO has default implementations for a number of Swift 
primitives:

- Bool
- Bytes, Data, ByteBuffer
- Date
- UInt8, Int16, Int32, Int64, Int, Float, Double
- String
- UUID

#### Quering with parameters

Sending parameterized queries to the database is also supported. (In the coolest way possible):

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

Yes, that looks like [SQL injection](https://en.wikipedia.org/wiki/SQL_injection) 😱, but this is 
actually save in PostgresNIO. The first parameter, of the query function is not a String, but a 
PostgresQuery, that implements the `ExpressibleByStringInterpolation` protocol. For this reason, 
PostgresNIO uses the static string as the SQL and the interpolation parameters as the bindings of 
the query. To interpolate values into the query, your values need to implement the 
`PostgresEncodable` protocol. 

For some queries you won't receive any rows from the server (mainly `INSERT`, `UPDATE` and `DELETE`). 
To make the API support this, the result of the `query` function is an `@discardableResult`. 

## Security

Please see [SECURITY.md](https://github.com/vapor/.github/blob/main/SECURITY.md) for details on the security process.

[EventLoopGroupConnectionPool]: https://github.com/vapor/async-kit/blob/main/Sources/AsyncKit/ConnectionPool/EventLoopGroupConnectionPool.swift
[AsyncKit]: https://github.com/vapor/async-kit/
[SwiftNIO]: https://github.com/apple/swift-nio
