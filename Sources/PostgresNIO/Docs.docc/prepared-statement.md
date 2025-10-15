# Boosting Performance with Prepared Statements

Prepared statements let PostgreSQL plan a query once and reuse it efficiently. In PostgresNIO, you model a prepared statement as a Swift type that conforms to ``PostgresPreparedStatement`` and execute it with ``PostgresClient/execute(_:logger:file:line:)`` or ``PostgresConnection/execute(_:logger:file:line:)``.

## Define a Prepared Statement

Create a type that provides the SQL, the bindings, and how to decode a row:

```swift
import PostgresNIO

/// Insert a user and return the generated id
struct InsertUser: PostgresPreparedStatement {
    static let sql = """
        INSERT INTO users (name, age, active)
        VALUES ($1, $2, $3)
        RETURNING id
        """
    typealias Row = Int

    var name: String
    var age: Int
    var active: Bool

    func makeBindings() throws -> PostgresBindings {
        var b = PostgresBindings()
        b.append(self.name)
        b.append(self.age)
        b.append(self.active)
        return b
    }

    func decodeRow(_ row: PostgresRow) throws -> Row {
        // Single column: decode as Int
        try row.makeRandomAccess().decode(column: 0, as: Int.self, context: .default)
    }
}

/// Load a single user by id
struct LoadUser: PostgresPreparedStatement {
    static let sql = "SELECT id, name, age, active FROM users WHERE id = $1"
    typealias Row = (Int, String, Int, Bool)

    var id: Int

    func makeBindings() throws -> PostgresBindings {
        var b = PostgresBindings()
        b.append(self.id)
        return b
    }

    func decodeRow(_ row: PostgresRow) throws -> Row {
        try row.decode(Row.self)
    }
}
```

## Use with `.query`

You can freely mix prepared statements with regular queries created via ``PostgresQuery``. For example, create a table using ``PostgresClient/query(_:logger:)`` and then use your prepared statements:

```swift
let logger = Logger(label: "example")

// Create table with a regular query
try await client.query(
    """
    CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        age INT NOT NULL,
        active BOOLEAN NOT NULL
    );
    """,
    logger: logger
)

// Execute prepared INSERT and read the generated id
let insertRows = try await client.execute(InsertUser(name: "Alice", age: 30, active: true), logger: logger)
for try await newID in insertRows {
    print("Inserted user with id: \(newID)")
}

// Execute prepared SELECT
let selectRows = try await client.execute(LoadUser(id: 1), logger: logger)
for try await (id, name, age, active) in selectRows {
    print("Loaded user #\(id): \(name), \(age), active? \(active)")
}
```

## Use with `.withTransaction`

Run multiple prepared statements atomically using ``PostgresClient/withTransaction(logger:file:line:isolation:_:)``:

```swift
try await client.withTransaction { connection in
    // Insert a user and fetch the id
    let ids = try await connection.execute(
        InsertUser(name: "Bob", age: 42, active: false),
        logger: logger
    )

    var newUserID: Int?
    for try await id in ids { newUserID = id }

    // Mix in a regular query within the same transaction
    if let id = newUserID {
        try await connection.query(
            "UPDATE users SET active = \(true) WHERE id = \(id)",
            logger: logger
        )
    }

    // Load and verify the user, still inside the transaction
    let rows = try await connection.execute(LoadUser(id: newUserID!), logger: logger)
    for try await (id, name, age, active) in rows {
        print("Transaction saw: #\(id) \(name) active? \(active)")
    }
}
```

If any call inside the closure throws, the transaction is rolled back. If the closure completes successfully, the transaction is committed.

## Tips

- Prefer prepared statements for frequently executed queries; it reduces parse/plan overhead on the server.
- Use tuple rows (e.g. `(Int, String)`) for ergonomic decoding, or create lightweight model initializers in `decodeRow(_:)`.
- You can omit ``PostgresPreparedStatement/bindingDataTypes`` for automatic inference in most cases.

## Topics

- ``PostgresPreparedStatement``
- ``PostgresClient/execute(_:logger:file:line:)``
- ``PostgresConnection/execute(_:logger:file:line:)``
- ``PostgresClient/query(_:logger:)``
- ``PostgresClient/withTransaction(logger:file:line:isolation:_:)``
