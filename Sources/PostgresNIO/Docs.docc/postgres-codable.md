# PostgresCodable

Encode and decode custom Swift types to and from PostgreSQL wire format.

## Overview

``PostgresEncodable`` and ``PostgresDecodable`` (collectively known as ``PostgresCodable``) allow you to define how your custom Swift types are encoded to and decoded from the PostgreSQL wire format. This enables you to use your custom types directly in queries and decode them from query results.

## Using Built-in Codable Types

Many standard Swift and Foundation types already conform to ``PostgresCodable``:

```swift
// Numeric types
let age: Int = 30
let price: Double = 99.99

// Text types
let name: String = "Alice"

// Other common types
let isActive: Bool = true
let id: UUID = UUID()
let timestamp: Date = Date()

// Collections
let tags: [String] = ["swift", "postgres", "nio"]

// Use them directly in queries
let rows = try await client.query(
    "INSERT INTO users (name, age, active, id, created) VALUES (\(name), \(age), \(isActive), \(id), \(timestamp))",
    logger: logger
)
```

## Using Codable Structs with JSONB

For custom Swift structs that you want to store as JSONB in PostgreSQL, simply conform to `Codable`. PostgresNIO automatically handles the encoding and decoding:

```swift
// Define a Codable struct
struct UserProfile: Codable {
    let displayName: String
    let bio: String
    let interests: [String]
}

// Insert into a JSONB column
let profile = UserProfile(
    displayName: "Alice",
    bio: "Swift developer",
    interests: ["coding", "hiking"]
)

try await client.query(
    "INSERT INTO users (id, profile) VALUES (\(userID), \(profile))",
    logger: logger
)

// Retrieve from a JSONB column
let rows = try await client.query(
    "SELECT profile FROM users WHERE id = \(userID)",
    logger: logger
)

for try await row in rows {
    let randomAccessRow = row.makeRandomAccess()
    let profile = try randomAccessRow.decode(column: "profile", as: UserProfile.self, context: .default)
    print("Display name: \(profile.displayName)")
}
```

This works for any Swift type that conforms to `Codable`, including nested structs, enums, and arrays. No manual encoding or decoding implementation is needed!

```swift
// Complex nested structure - just add Codable!
struct Address: Codable {
    let street: String
    let city: String
    let zipCode: String
}

struct Company: Codable {
    let name: String
    let founded: Date
    let address: Address
    let employees: Int
}

// Works automatically with JSONB columns
let company = Company(
    name: "Acme Inc",
    founded: Date(),
    address: Address(street: "123 Main St", city: "Springfield", zipCode: "12345"),
    employees: 50
)

try await client.query(
    "INSERT INTO companies (data) VALUES (\(company))",
    logger: logger
)
```

## Implementing PostgresEncodable

To make a custom type encodable to PostgreSQL, implement the ``PostgresEncodable`` protocol:

```swift
import NIOCore

struct Point: PostgresEncodable {
    let x: Double
    let y: Double

    // Specify the PostgreSQL data type
    static var psqlType: PostgresDataType { .point }

    // Specify the encoding format (binary or text)
    static var psqlFormat: PostgresFormat { .binary }

    // Encode the value into a ByteBuffer
    func encode<JSONEncoder: PostgresJSONEncoder>(
        into buffer: inout ByteBuffer,
        context: PostgresEncodingContext<JSONEncoder>
    ) throws {
        // Encode as PostgreSQL point format: (x,y)
        buffer.writeDouble(x)
        buffer.writeDouble(y)
    }
}

// Use it in queries
let location = Point(x: 37.7749, y: -122.4194)
try await client.execute(
    "INSERT INTO locations (name, coordinate) VALUES (\(locationName), \(location))",
    logger: logger
)
```

## Implementing PostgresDecodable

To decode a custom type from PostgreSQL results, implement the ``PostgresDecodable`` protocol:

```swift
import NIOCore

struct Point: PostgresDecodable {
    let x: Double
    let y: Double

    // Decode from a ByteBuffer
    init<JSONDecoder: PostgresJSONDecoder>(
        from buffer: inout ByteBuffer,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) throws {
        // Verify we're decoding the expected type
        guard type == .point else {
            throw PostgresDecodingError.Code.typeMismatch
        }

        // Read the coordinates
        guard let x = buffer.readDouble(),
              let y = buffer.readDouble() else {
            throw PostgresDecodingError.Code.missingData
        }

        self.x = x
        self.y = y
    }
}

// Decode from query results
let rows = try await client.query(
    "SELECT coordinate FROM locations WHERE name = \(locationName)",
    logger: logger
)

for try await row in rows {
    let randomAccessRow = row.makeRandomAccess()
    let point = try randomAccessRow.decode(column: "coordinate", as: Point.self, context: .default)
    print("Location: (\(point.x), \(point.y))")
}
```

## Advanced: Manual JSON Encoding and Decoding

> Note: For most use cases, simply conforming your struct to `Codable` is sufficient (see <doc:#Using-Codable-Structs-with-JSONB>). Only implement manual encoding/decoding if you need fine-grained control over the JSON representation or need to handle both JSON and JSONB types differently.

For advanced scenarios where you need manual control over JSON encoding:

```swift
struct UserProfile: Codable, PostgresCodable {
    let displayName: String
    let bio: String
    let interests: [String]

    // Encode as JSONB
    static var psqlType: PostgresDataType { .jsonb }
    static var psqlFormat: PostgresFormat { .binary }

    func encode<JSONEncoder: PostgresJSONEncoder>(
        into buffer: inout ByteBuffer,
        context: PostgresEncodingContext<JSONEncoder>
    ) throws {
        // JSONB format version byte
        buffer.writeInteger(1, as: UInt8.self)
        // Encode as JSON
        let data = try context.jsonEncoder.encode(self)
        buffer.writeBytes(data)
    }

    init<JSONDecoder: PostgresJSONDecoder>(
        from buffer: inout ByteBuffer,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) throws {
        guard type == .jsonb || type == .json else {
            throw PostgresDecodingError.Code.typeMismatch
        }

        var jsonBuffer = buffer
        if type == .jsonb {
            // Skip JSONB version byte
            _ = jsonBuffer.readInteger(as: UInt8.self)
        }

        guard let data = jsonBuffer.readData(length: jsonBuffer.readableBytes) else {
            throw PostgresDecodingError.Code.missingData
        }

        self = try context.jsonDecoder.decode(UserProfile.self, from: data)
    }
}

// Use with queries
let profile = UserProfile(
    displayName: "Alice",
    bio: "Swift developer",
    interests: ["coding", "hiking"]
)

try await client.execute(
    "UPDATE users SET profile = \(profile) WHERE id = \(userID)",
    logger: logger
)
```

## Custom JSON Encoding Context

When you need custom JSON encoding/decoding behavior:

```swift
// Create a custom encoder
let jsonEncoder = JSONEncoder()
jsonEncoder.dateEncodingStrategy = .iso8601
jsonEncoder.keyEncodingStrategy = .convertToSnakeCase

let context = PostgresEncodingContext(jsonEncoder: jsonEncoder)

// Use with bindings
var bindings = PostgresBindings()
try bindings.append(profile, context: context)

let query = PostgresQuery(
    unsafeSQL: "INSERT INTO users (profile) VALUES ($1)",
    binds: bindings
)
```

## RawRepresentable Types

For enums with encodable raw values:

```swift
enum UserStatus: String, PostgresCodable {
    case active
    case inactive
    case suspended

    static var psqlType: PostgresDataType { .text }
    static var psqlFormat: PostgresFormat { .binary }

    func encode<JSONEncoder: PostgresJSONEncoder>(
        into buffer: inout ByteBuffer,
        context: PostgresEncodingContext<JSONEncoder>
    ) throws {
        try rawValue.encode(into: &buffer, context: context)
    }

    init<JSONDecoder: PostgresJSONDecoder>(
        from buffer: inout ByteBuffer,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) throws {
        let rawValue = try String(from: &buffer, type: type, format: format, context: context)
        guard let value = Self(rawValue: rawValue) else {
            throw PostgresDecodingError.Code.failure
        }
        self = value
    }
}

// Use in queries
let status = UserStatus.active
try await client.execute(
    "UPDATE users SET status = \(status) WHERE id = \(userID)",
    logger: logger
)
```

## Decoding Rows with Multiple Columns

Decode multiple values from a single row:

```swift
struct User: PostgresDecodable {
    let id: Int
    let name: String
    let email: String
    let createdAt: Date

    init<JSONDecoder: PostgresJSONDecoder>(
        from buffer: inout ByteBuffer,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) throws {
        // This is for decoding a single complex value
        // For rows with multiple columns, use the row decoding API below
        fatalError("Use row decoding instead")
    }
}

// Decode from query results using the row API
let rows = try await client.query(
    "SELECT id, name, email, created_at FROM users WHERE age > \(minAge)",
    logger: logger
)

for try await row in rows {
    let randomAccessRow = row.makeRandomAccess()
    let id: Int = try randomAccessRow.decode(column: "id", as: Int.self, context: .default)
    let name: String = try randomAccessRow.decode(column: "name", as: String.self, context: .default)
    let email: String = try randomAccessRow.decode(column: "email", as: String.self, context: .default)
    let createdAt: Date = try randomAccessRow.decode(column: "created_at", as: Date.self, context: .default)

    let user = User(id: id, name: name, email: email, createdAt: createdAt)
    print("User: \(user.name)")
}

// Or use tuple decoding for convenience
for try await (id, name, email, createdAt) in rows.decode((Int, String, String, Date).self) {
    print("User: \(name) (ID: \(id))")
}
```

## Topics

### Protocols

- ``PostgresEncodable``
- ``PostgresDecodable``
- ``PostgresCodable``
- ``PostgresNonThrowingEncodable``
- ``PostgresDynamicTypeEncodable``
- ``PostgresThrowingDynamicTypeEncodable``

### Supporting Types

- ``PostgresEncodingContext``
- ``PostgresDecodingContext``
- ``PostgresDataType``
- ``PostgresFormat``
