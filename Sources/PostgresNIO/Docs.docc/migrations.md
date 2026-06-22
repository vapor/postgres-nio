# Adopting the new PostgresRow cell API

This article describes how to adopt the new ``PostgresRow`` cell APIs in existing Postgres code 
which use the ``PostgresRow/column(_:)`` API today.  

## TLDR

1. Map your sequence of ``PostgresRow``s to ``PostgresRandomAccessRow``s.
2. Use the ``PostgresRandomAccessRow/subscript(_:)-3facl`` API to receive a ``PostgresCell``
3. Decode the ``PostgresCell`` into a Swift type using the ``PostgresCell/decode(_:file:line:)`` method.

```swift
let rows: [PostgresRow] // your existing return value
for row in rows.map({ PostgresRandomAccessRow($0) }) {
    let id = try row["id"].decode(UUID.self)
    let name = try row["name"].decode(String.self)
    let email = try row["email"].decode(String.self)
    let age = try row["age"].decode(Int.self)
}
```

## Overview

When Postgres [`1.9.0`] was released we changed the default behaviour of ``PostgresRow``s.
Previously for each row we created an internal lookup table, that allowed you to access the rows'
cells by name:

```swift
connection.query("SELECT id, name, email, age FROM users").whenComplete {
    switch $0 {
    case .success(let result):
        for row in result.rows {
            let id = row.column("id").uuid
            let name = row.column("name").string
            let email = row.column("email").string
            let age = row.column("age").int
            // do further processing
        }
    case .failure(let error):
        // handle the error
    }
}
```

During the last year we introduced a new API that let's you consume ``PostgresRow`` by iterating 
its cells. This approach has the performance benefit of not needing to create an internal cell 
lookup table for each row:

```swift
connection.query("SELECT id, name, email, age FROM users").whenComplete {
    switch $0 {
    case .success(let result):
        for row in result.rows {
            let (id, name, email, age) = try row.decode((UUID, String, String, Int).self)
            // do further processing
        }
    case .failure(let error):
        // handle the error
    }
}
```

However, since we still supported the ``PostgresRow/column(_:)`` API, which requires a precomputed 
lookup table within the row, users were not seeing any performance benefits. To allow users to 
benefit of the new fastpath, we changed ``PostgresRow``'s behavior:

By default the ``PostgresRow`` does not create an internal lookup table for its cells on creation 
anymore. Because of this, when using the ``PostgresRow/column(_:)`` API, a throwaway lookup table 
needs to be produced on every call. Since this is wasteful we have deprecated this API. Instead we 
allow users now to explicitly opt-in into the cell lookup API by using the new 
``PostgresRandomAccessRow``.

```swift
connection.query("SELECT id, name, email, age FROM users").whenComplete {
    switch $0 {
    case .success(let result):
        for row in result.rows.map { PostgresRandomAccessRow($0) } {
            let id = try row["id"].decode(UUID.self)
            let name = try row["name"].decode(String.self)
            let email = try row["email"].decode(String.self)
            let age = try row["age"].decode(Int.self)
            // do further processing
        }
    case .failure(let error):
        // handle the error
    }
}
```

[`1.9.0`]: https://github.com/vapor/postgres-nio/releases/tag/1.9.0
