# Running Queries

Interact with the PostgreSQL database by running Queries.

## Overview

You interact with the Postgres database by running SQL queries using ``PostgresQuery``. PostgresNIO provides several methods for executing queries depending on your needs.

### Quick Start: Running Queries

#### Using query() for Simple Queries

The most common way to run a query is with the ``PostgresClient/query(_:logger:)`` method:

```swift
let rows = try await client.query("SELECT * FROM users WHERE age > \(minAge)", logger: logger)

for try await row in rows {
    let id: Int = try row.decode(column: "id", as: Int.self)
    let name: String = try row.decode(column: "name", as: String.self)
    print("User: \(name) (ID: \(id))")
}
```

#### Using execute() for Non-Returning Queries

For queries that don't return rows (INSERT, UPDATE, DELETE without RETURNING), use ``PostgresConnection/execute(_:logger:file:line:)``:

```swift
try await client.execute(
    "UPDATE users SET last_login = \(Date()) WHERE id = \(userID)",
    logger: logger
)
```

#### Using withConnection for Multiple Queries

When you need to run multiple queries on the same connection:

```swift
try await client.withConnection { connection in
    // Execute multiple queries on the same connection
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

#### Using withTransaction for Atomic Operations

When you need multiple queries to succeed or fail together:

```swift
try await client.withTransaction { connection in
    // All queries execute within a transaction

    // Debit from one account
    try await connection.execute(
        "UPDATE accounts SET balance = balance - \(amount) WHERE id = \(fromAccount)",
        logger: logger
    )

    // Credit to another account
    try await connection.execute(
        "UPDATE accounts SET balance = balance + \(amount) WHERE id = \(toAccount)",
        logger: logger
    )

    // If any query fails, the entire transaction rolls back
    // If the closure completes successfully, the transaction commits
}
```

### String Interpolation and Safety

``PostgresQuery`` conforms to [`ExpressibleByStringInterpolation`], allowing you to safely embed values in your SQL queries. Interpolated values are automatically converted to parameterized query bindings, preventing SQL injection:

```swift
let username = "alice"
let query: PostgresQuery = "SELECT * FROM users WHERE username = \(username)"
// Generates: "SELECT * FROM users WHERE username = $1" with binding: ["alice"]
``` 


## Topics

- ``PostgresQuery``
- ``PostgresBindings``
- ``PostgresRow``
- ``PostgresRowSequence``
- ``PostgresRandomAccessRow``
- ``PostgresCell``
- ``PostgresQueryMetadata``

[Queries]: doc:PostgresQuery
[`ExpressibleByStringInterpolation`]: https://developer.apple.com/documentation/swift/expressiblebystringinterpolation
