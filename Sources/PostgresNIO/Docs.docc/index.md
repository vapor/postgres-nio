# ``PostgresNIO``

@Metadata {
    @TitleHeading(Package)
}

🐘 Non-blocking, event-driven Swift client for PostgreSQL built on SwiftNIO.

## Overview

``PostgresNIO`` allows you to connect to, authorize with, query, and retrieve results from a 
PostgreSQL server. PostgreSQL is an open source relational database.

Use a ``PostgresConnection`` to create a connection to the PostgreSQL server. You can then use it to
run queries and prepared statements against the server. ``PostgresConnection`` also supports 
PostgreSQL's Listen & Notify API.

Developers, who don't want to manage connections themselves, can use the ``PostgresClient``, which 
offers the same functionality as ``PostgresConnection``. ``PostgresClient``
pools connections for rapid connection reuse and hides the complexities of connection 
management from the user, allowing developers to focus on their SQL queries. ``PostgresClient``
implements the `Service` protocol from Service Lifecycle allowing an easy adoption in Swift server
applications.

``PostgresNIO`` embraces Swift structured concurrency, offering async/await APIs which handle
task cancellation. The query interface makes use of backpressure to ensure that memory can not grow 
unbounded for queries that return thousands of rows.

``PostgresNIO`` runs efficiently on Linux and Apple platforms. On Apple platforms developers can
configure ``PostgresConnection`` to use `Network.framework` as the underlying transport framework.

## Quick Start

### 1. Create and Run a PostgresClient

First, create a ``PostgresClient/Configuration`` and initialize your client:

```swift
import PostgresNIO

// Configure the client with individual parameters
let config = PostgresClient.Configuration(
    host: "localhost",
    port: 5432,
    username: "my_username",
    password: "my_password",
    database: "my_database",
    tls: .disable
)

// Or parse from a PostgreSQL URL string
let urlString = "postgresql://username:password@localhost:5432/my_database"
let url = URL(string: urlString)!
let config = PostgresClient.Configuration(
    host: url.host!,
    port: url.port ?? 5432,
    username: url.user!,
    password: url.password,
    database: url.path.trimmingPrefix("/"),
    tls: .disable
)

// Create the client
let client = PostgresClient(configuration: config)

// Run the client (required)
await withTaskGroup(of: Void.self) { taskGroup in
    taskGroup.addTask {
        await client.run()
    }

    // Your application code using the client goes here

    // Shutdown when done
    taskGroup.cancelAll()
}
```

### 2. Running Queries with PostgresQuery

Use string interpolation to safely execute queries with parameters:

```swift
// Simple SELECT query
let minAge = 21
let rows = try await client.query(
    "SELECT * FROM users WHERE age > \(minAge)",
    logger: logger
)

for try await row in rows {
    let id: Int = try row.decode(column: "id", as: Int.self)
    let name: String = try row.decode(column: "name", as: String.self)
    print("User: \(name) (ID: \(id))")
}

// INSERT query
let name = "Alice"
let email = "alice@example.com"
try await client.execute(
    "INSERT INTO users (name, email) VALUES (\(name), \(email))",
    logger: logger
)
```

### 3. Building Dynamic Queries with PostgresBindings

For complex or dynamic queries, manually construct bindings:

```swift
func buildSearchQuery(filters: [String: Any]) -> PostgresQuery {
    var bindings = PostgresBindings()
    var sql = "SELECT * FROM products WHERE 1=1"

    if let name = filters["name"] as? String {
        bindings.append(name)
        sql += " AND name = $\(bindings.count)"
    }

    if let minPrice = filters["minPrice"] as? Double {
        bindings.append(minPrice)
        sql += " AND price >= $\(bindings.count)"
    }

    return PostgresQuery(unsafeSQL: sql, binds: bindings)
}

// Execute the dynamic query
let filters = ["name": "Widget", "minPrice": 9.99]
let query = buildSearchQuery(filters: filters)
let rows = try await client.query(query, logger: logger)
```

### 4. Using Transactions with withTransaction

Execute multiple queries atomically:

```swift
try await client.withTransaction { connection in
    // All queries execute within a transaction

    // Debit from account
    try await connection.execute(
        "UPDATE accounts SET balance = balance - \(amount) WHERE id = \(fromAccount)",
        logger: logger
    )

    // Credit to account
    try await connection.execute(
        "UPDATE accounts SET balance = balance + \(amount) WHERE id = \(toAccount)",
        logger: logger
    )

    // If any query fails, the entire transaction rolls back
    // If this closure completes successfully, the transaction commits
}
```

### 5. Using withConnection for Multiple Queries

Execute multiple queries on the same connection for better performance:

```swift
try await client.withConnection { connection in
    let userRows = try await connection.query(
        "SELECT * FROM users WHERE id = \(userID)",
        logger: logger
    )

    let orderRows = try await connection.query(
        "SELECT * FROM orders WHERE user_id = \(userID)",
        logger: logger
    )

    // Process results...
}
```

For more details, see <doc:running-queries>.

### 6. Using Custom Types with PostgresCodable

Many Swift types already work out of the box. For custom types, implement ``PostgresEncodable`` and ``PostgresDecodable``:

```swift
// Store complex data as JSONB
struct UserProfile: Codable {
    let displayName: String
    let bio: String
    let interests: [String]
}

// Use directly in queries (encodes as JSONB automatically via Codable)
let profile = UserProfile(
    displayName: "Alice",
    bio: "Swift developer",
    interests: ["coding", "hiking"]
)

try await client.execute(
    "UPDATE users SET profile = \(profile) WHERE id = \(userID)",
    logger: logger
)

// Decode from results
let rows = try await client.query(
    "SELECT profile FROM users WHERE id = \(userID)",
    logger: logger
)

for try await row in rows {
    let profile = try row.decode(column: "profile", as: UserProfile.self)
    print("Display name: \(profile.displayName)")
}
```

For advanced usage including custom PostgreSQL types, binary encoding, and RawRepresentable enums, see <doc:postgres-codable>.

## Topics

### Essentials

- ``PostgresClient``
- ``PostgresClient/Configuration``
- ``PostgresConnection``
- <doc:running-queries>

### Advanced

- <doc:postgres-codable>
- <doc:coding>
- <doc:prepared-statement>
- <doc:listen>

### Errors

- ``PostgresError``
- ``PostgresDecodingError``
- ``PSQLError``

### Deprecations

- <doc:deprecated>

[SwiftNIO]: https://github.com/apple/swift-nio
[SwiftLog]: https://github.com/apple/swift-log
