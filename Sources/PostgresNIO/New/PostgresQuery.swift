import NIOCore

/// A Postgres SQL query, that can be executed on a Postgres server. Contains the raw sql string and bindings.
///
/// `PostgresQuery` supports safe string interpolation to automatically bind parameters and prevent SQL injection.
///
/// ## Basic Usage
///
/// Create a query using string interpolation with automatic parameter binding:
///
/// ```swift
/// let userID = 42
/// let query: PostgresQuery = "SELECT * FROM users WHERE id = \(userID)"
/// // Generates: "SELECT * FROM users WHERE id = $1" with bindings: [42]
/// ```
///
/// ## String Interpolation with Various Types
///
/// String interpolation works with any type conforming to `PostgresEncodable`:
///
/// ```swift
/// let name = "Alice"
/// let age = 30
/// let isActive = true
/// let query: PostgresQuery = """
///     INSERT INTO users (name, age, active)
///     VALUES (\(name), \(age), \(isActive))
///     """
/// ```
///
/// ## Optional Values
///
/// Optional values are automatically handled and encoded as NULL when nil:
///
/// ```swift
/// let email: String? = nil
/// let query: PostgresQuery = "UPDATE users SET email = \(email) WHERE id = \(userID)"
/// // email will be encoded as NULL in the database
/// ```
///
/// ## Unsafe Raw SQL
///
/// For dynamic table/column names or SQL keywords, use `unescaped` interpolation (use with caution):
///
/// ```swift
/// let tableName = "users"
/// let columnName = "created_at"
/// let query: PostgresQuery = "SELECT * FROM \(unescaped: tableName) ORDER BY \(unescaped: columnName) DESC"
/// ```
///
/// ## Manual Construction with PostgresBindings
///
/// You can also create queries manually using `PostgresBindings` without string interpolation.
/// This is useful when building dynamic queries programmatically:
///
/// ```swift
/// var bindings = PostgresBindings()
/// bindings.append("Alice")
/// bindings.append(30)
/// let query = PostgresQuery(unsafeSQL: "INSERT INTO users (name, age) VALUES ($1, $2)", binds: bindings)
/// ```
///
/// ## Building Dynamic Queries
///
/// For complex scenarios where you need to build queries dynamically:
///
/// ```swift
/// func buildSearchQuery(filters: [String: Any]) -> PostgresQuery {
///     var bindings = PostgresBindings()
///     var sql = "SELECT * FROM products WHERE 1=1"
///
///     if let name = filters["name"] as? String {
///         bindings.append(name)
///         sql += " AND name = $\(bindings.count)"
///     }
///
///     if let minPrice = filters["minPrice"] as? Double {
///         bindings.append(minPrice)
///         sql += " AND price >= $\(bindings.count)"
///     }
///
///     if let category = filters["category"] as? String {
///         bindings.append(category)
///         sql += " AND category = $\(bindings.count)"
///     }
///
///     return PostgresQuery(unsafeSQL: sql, binds: bindings)
/// }
///
/// let filters = ["name": "Widget", "minPrice": 9.99]
/// let query = buildSearchQuery(filters: filters)
/// // Generates: "SELECT * FROM products WHERE 1=1 AND name = $1 AND price >= $2"
/// // With bindings: ["Widget", 9.99]
/// ```
///
/// ## Executing Queries
///
/// Once you've created a query, execute it using various methods on `PostgresClient`:
///
/// ### Basic Query Execution
///
/// Execute a query and iterate over results:
///
/// ```swift
/// let client = PostgresClient(configuration: config)
/// let query: PostgresQuery = "SELECT * FROM users WHERE age > \(minAge)"
///
/// let rows = try await client.query(query, logger: logger)
/// for try await row in rows {
///     let randomAccessRow = row.makeRandomAccess()
///     let id: Int = try randomAccessRow.decode(column: "id", as: Int.self, context: .default)
///     let name: String = try randomAccessRow.decode(column: "name", as: String.self, context: .default)
///     print("User: \(name) (ID: \(id))")
/// }
/// ```
///
/// ### Using withConnection
///
/// Execute multiple queries on the same connection:
///
/// ```swift
/// try await client.withConnection { connection in
///     // First query
///     let userID = 42
///     let userRows = try await connection.query(
///         "SELECT * FROM users WHERE id = \(userID)",
///         logger: logger
///     )
///
///     // Second query on the same connection
///     let orderRows = try await connection.query(
///         "SELECT * FROM orders WHERE user_id = \(userID)",
///         logger: logger
///     )
///
///     // Process results...
/// }
/// ```
///
/// ### Using withTransaction
///
/// Execute queries within a transaction for atomicity:
///
/// ```swift
/// try await client.withTransaction { connection in
///     // All queries execute in a transaction
///     let fromAccount = "account123"
///     let toAccount = "account456"
///     let amount = 100.0
///
///     // Debit from account
///     try await connection.query(
///         "UPDATE accounts SET balance = balance - \(amount) WHERE id = \(fromAccount)",
///         logger: logger
///     )
///
///     // Credit to account
///     try await connection.query(
///         "UPDATE accounts SET balance = balance + \(amount) WHERE id = \(toAccount)",
///         logger: logger
///     )
///
///     // If any query fails or throws, the entire transaction is rolled back
///     // If this closure completes successfully, the transaction is committed
/// }
/// ```
///
/// ### Insert and Return Generated IDs
///
/// Insert data and retrieve auto-generated values:
///
/// ```swift
/// let name = "Alice"
/// let email = "alice@example.com"
/// let rows = try await client.query(
///     "INSERT INTO users (name, email) VALUES (\(name), \(email)) RETURNING id",
///     logger: logger
/// )
///
/// for try await row in rows {
///     let randomAccessRow = row.makeRandomAccess()
///     let newID: Int = try randomAccessRow.decode(column: "id", as: Int.self, context: .default)
///     print("Created user with ID: \(newID)")
/// }
/// ```
///
/// - Note: String interpolation is the recommended approach for simple queries as it automatically handles parameter counting and binding.
/// - Warning: Always use parameter binding for user input. Never concatenate user input directly into SQL strings.
/// - SeeAlso: `PostgresBindings` for more details on manual binding construction.
/// - SeeAlso: `PostgresClient` for connection pool management and query execution.
public struct PostgresQuery: Sendable, Hashable {
    /// The query string
    public var sql: String
    /// The query binds
    public var binds: PostgresBindings

    public init(unsafeSQL sql: String, binds: PostgresBindings = PostgresBindings()) {
        self.sql = sql
        self.binds = binds
    }
}

extension PostgresQuery: ExpressibleByStringInterpolation {
    public init(stringInterpolation: StringInterpolation) {
        self.sql = stringInterpolation.sql
        self.binds = stringInterpolation.binds
    }

    public init(stringLiteral value: String) {
        self.sql = value
        self.binds = PostgresBindings()
    }
}

extension PostgresQuery {
    public struct StringInterpolation: StringInterpolationProtocol, Sendable {
        public typealias StringLiteralType = String

        @usableFromInline
        var sql: String
        @usableFromInline
        var binds: PostgresBindings

        public init(literalCapacity: Int, interpolationCount: Int) {
            self.sql = ""
            self.binds = PostgresBindings(capacity: interpolationCount)
        }

        public mutating func appendLiteral(_ literal: String) {
            self.sql.append(contentsOf: literal)
        }

        @inlinable
        public mutating func appendInterpolation<Value: PostgresThrowingDynamicTypeEncodable>(_ value: Value) throws {
            try self.binds.append(value, context: .default)
            self.sql.append(contentsOf: "$\(self.binds.count)")
        }

        @inlinable
        public mutating func appendInterpolation<Value: PostgresThrowingDynamicTypeEncodable>(_ value: Optional<Value>) throws {
            switch value {
            case .none:
                self.binds.appendNull()
            case .some(let value):
                try self.binds.append(value, context: .default)
            }

            self.sql.append(contentsOf: "$\(self.binds.count)")
        }

        @inlinable
        public mutating func appendInterpolation<Value: PostgresDynamicTypeEncodable>(_ value: Value) {
            self.binds.append(value, context: .default)
            self.sql.append(contentsOf: "$\(self.binds.count)")
        }

        @inlinable
        public mutating func appendInterpolation<Value: PostgresDynamicTypeEncodable>(_ value: Optional<Value>) {
            switch value {
            case .none:
                self.binds.appendNull()
            case .some(let value):
                self.binds.append(value, context: .default)
            }

            self.sql.append(contentsOf: "$\(self.binds.count)")
        }

        @inlinable
        public mutating func appendInterpolation<Value: PostgresThrowingDynamicTypeEncodable, JSONEncoder: PostgresJSONEncoder>(
            _ value: Value,
            context: PostgresEncodingContext<JSONEncoder>
        ) throws {
            try self.binds.append(value, context: context)
            self.sql.append(contentsOf: "$\(self.binds.count)")
        }

        @inlinable
        public mutating func appendInterpolation(unescaped interpolated: String) {
            self.sql.append(contentsOf: interpolated)
        }
    }
}

extension PostgresQuery: CustomStringConvertible {
    // See `CustomStringConvertible.description`.
    public var description: String {
        "\(self.sql) \(self.binds)"
    }
}

extension PostgresQuery: CustomDebugStringConvertible {
    // See `CustomDebugStringConvertible.debugDescription`.
    public var debugDescription: String {
        "PostgresQuery(sql: \(String(describing: self.sql)), binds: \(String(reflecting: self.binds)))"
    }
}

struct PSQLExecuteStatement {
    /// The statements name
    var name: String
    /// The binds
    var binds: PostgresBindings

    var rowDescription: RowDescription?
}

/// A collection of parameter bindings for a Postgres query.
///
/// `PostgresBindings` manages the parameters that are safely bound to a SQL query, preventing SQL injection
/// and handling type conversions to the Postgres wire format.
///
/// ## Basic Usage
///
/// Typically, you don't need to create `PostgresBindings` directly when using `PostgresQuery` with string interpolation.
/// However, you can manually construct bindings when needed:
///
/// ```swift
/// var bindings = PostgresBindings()
/// bindings.append("Alice")
/// bindings.append(30)
/// bindings.append(true)
/// // bindings now contains 3 parameters
/// ```
///
/// ## Appending Different Types
///
/// `PostgresBindings` can store any type conforming to `PostgresEncodable`:
///
/// ```swift
/// var bindings = PostgresBindings()
/// bindings.append("John Doe")        // String
/// bindings.append(42)                // Int
/// bindings.append(3.14)              // Double
/// bindings.append(Date())            // Date
/// bindings.append(true)              // Bool
/// bindings.append([1, 2, 3])         // Array
/// ```
///
/// ## Handling Optional Values
///
/// Optional values can be appended and will be encoded as NULL when nil:
///
/// ```swift
/// var bindings = PostgresBindings()
/// let email: String? = nil
/// bindings.append(email)  // Encodes as NULL
///
/// let name: String? = "Alice"
/// bindings.append(name)   // Encodes as "Alice"
/// ```
///
/// ## Manual NULL Values
///
/// You can explicitly append NULL values:
///
/// ```swift
/// var bindings = PostgresBindings()
/// bindings.appendNull()
/// ```
///
/// ## Using with Custom Encoding Context
///
/// For custom JSON encoding, use a custom encoding context:
///
/// ```swift
/// var bindings = PostgresBindings()
/// let jsonEncoder = JSONEncoder()
/// jsonEncoder.dateEncodingStrategy = .iso8601
/// let context = PostgresEncodingContext(jsonEncoder: jsonEncoder)
///
/// struct User: Codable {
///     let name: String
///     let age: Int
/// }
/// let user = User(name: "Alice", age: 30)
/// try bindings.append(user, context: context)
/// ```
///
/// ## Pre-allocating Capacity
///
/// For better performance with known parameter counts:
///
/// ```swift
/// var bindings = PostgresBindings(capacity: 10)  // Pre-allocate space for 10 bindings
/// ```
///
/// ## Using with PostgresQuery
///
/// Combine `PostgresBindings` with `PostgresQuery` for manual query construction.
/// This is particularly useful when building dynamic queries:
///
/// ```swift
/// func buildSearchQuery(filters: [String: Any]) -> PostgresQuery {
///     var bindings = PostgresBindings()
///     var sql = "SELECT * FROM products WHERE 1=1"
///
///     if let name = filters["name"] as? String {
///         bindings.append(name)
///         sql += " AND name = $\(bindings.count)"
///     }
///
///     if let minPrice = filters["minPrice"] as? Double {
///         bindings.append(minPrice)
///         sql += " AND price >= $\(bindings.count)"
///     }
///
///     if let category = filters["category"] as? String {
///         bindings.append(category)
///         sql += " AND category = $\(bindings.count)"
///     }
///
///     return PostgresQuery(unsafeSQL: sql, binds: bindings)
/// }
///
/// // Usage
/// let filters = ["name": "Widget", "minPrice": 9.99]
/// let query = buildSearchQuery(filters: filters)
/// let rows = try await client.query(query, logger: logger)
/// ```
///
/// ## Using with withConnection
///
/// Execute multiple dynamically-built queries on the same connection:
///
/// ```swift
/// try await client.withConnection { connection in
///     // Build and execute first query
///     var bindings1 = PostgresBindings()
///     bindings1.append(userID)
///     let query1 = PostgresQuery(
///         unsafeSQL: "SELECT * FROM users WHERE id = $\(bindings1.count)",
///         binds: bindings1
///     )
///     let userRows = try await connection.query(query1, logger: logger)
///
///     // Build and execute second query on same connection
///     var bindings2 = PostgresBindings()
///     bindings2.append(userID)
///     bindings2.append(startDate)
///     let query2 = PostgresQuery(
///         unsafeSQL: "SELECT * FROM orders WHERE user_id = $1 AND created_at >= $2",
///         binds: bindings2
///     )
///     let orderRows = try await connection.query(query2, logger: logger)
/// }
/// ```
///
/// ## Using with withTransaction
///
/// Build and execute transactional queries with manual bindings:
///
/// ```swift
/// try await client.withTransaction { connection in
///     // Debit query
///     var debitBindings = PostgresBindings()
///     debitBindings.append(amount)
///     debitBindings.append(fromAccountID)
///     let debitQuery = PostgresQuery(
///         unsafeSQL: "UPDATE accounts SET balance = balance - $1 WHERE id = $2",
///         binds: debitBindings
///     )
///     try await connection.query(debitQuery, logger: logger)
///
///     // Credit query
///     var creditBindings = PostgresBindings()
///     creditBindings.append(amount)
///     creditBindings.append(toAccountID)
///     let creditQuery = PostgresQuery(
///         unsafeSQL: "UPDATE accounts SET balance = balance + $1 WHERE id = $2",
///         binds: creditBindings
///     )
///     try await connection.query(creditQuery, logger: logger)
///
///     // Both queries commit together, or roll back on error
/// }
/// ```
///
/// - Note: Bindings are indexed starting from 1 in SQL (e.g., $1, $2, $3).
/// - Note: The `count` property returns the number of bindings currently stored.
/// - SeeAlso: `PostgresQuery` for creating complete queries with bindings.
public struct PostgresBindings: Sendable, Hashable {
    @usableFromInline
    struct Metadata: Sendable, Hashable {
        @usableFromInline
        var dataType: PostgresDataType
        @usableFromInline
        var format: PostgresFormat
        @usableFromInline
        var protected: Bool

        @inlinable
        init(dataType: PostgresDataType, format: PostgresFormat, protected: Bool) {
            self.dataType = dataType
            self.format = format
            self.protected = protected
        }

        @inlinable
        init<Value: PostgresThrowingDynamicTypeEncodable>(value: Value, protected: Bool) {
            self.init(dataType: value.psqlType, format: value.psqlFormat, protected: protected)
        }
    }

    @usableFromInline
    var metadata: [Metadata]
    @usableFromInline
    var bytes: ByteBuffer

    public var count: Int {
        self.metadata.count
    }

    public init() {
        self.metadata = []
        self.bytes = ByteBuffer()
    }

    public init(capacity: Int) {
        self.metadata = []
        self.metadata.reserveCapacity(capacity)
        self.bytes = ByteBuffer()
        self.bytes.reserveCapacity(128 * capacity)
    }

    public mutating func appendNull() {
        self.bytes.writeInteger(-1, as: Int32.self)
        self.metadata.append(.init(dataType: .null, format: .binary, protected: true))
    }

    @inlinable
    public mutating func append<Value: PostgresThrowingDynamicTypeEncodable>(_ value: Value) throws {
        try self.append(value, context: .default)
    }

    @inlinable
    public mutating func append<Value: PostgresThrowingDynamicTypeEncodable>(_ value: Optional<Value>) throws {
        switch value {
        case .none:
            self.appendNull()
        case let .some(value):
            try self.append(value)
        }
    }

    @inlinable
    public mutating func append<Value: PostgresThrowingDynamicTypeEncodable, JSONEncoder: PostgresJSONEncoder>(
        _ value: Value,
        context: PostgresEncodingContext<JSONEncoder>
    ) throws {
        try value.encodeRaw(into: &self.bytes, context: context)
        self.metadata.append(.init(value: value, protected: true))
    }

    @inlinable
    public mutating func append<Value: PostgresThrowingDynamicTypeEncodable, JSONEncoder: PostgresJSONEncoder>(
        _ value: Optional<Value>,
        context: PostgresEncodingContext<JSONEncoder>
    ) throws {
        switch value {
        case .none:
            self.appendNull()
        case let .some(value):
            try self.append(value, context: context)
        }
    }

    @inlinable
    public mutating func append<Value: PostgresDynamicTypeEncodable>(_ value: Value) {
        self.append(value, context: .default)
    }

    @inlinable
    public mutating func append<Value: PostgresDynamicTypeEncodable>(_ value: Optional<Value>) {
        switch value {
        case .none:
            self.appendNull()
        case let .some(value):
            self.append(value)
        }
    }

    @inlinable
    public mutating func append<Value: PostgresDynamicTypeEncodable, JSONEncoder: PostgresJSONEncoder>(
        _ value: Value,
        context: PostgresEncodingContext<JSONEncoder>
    ) {
        value.encodeRaw(into: &self.bytes, context: context)
        self.metadata.append(.init(value: value, protected: true))
    }

    @inlinable
    public mutating func append<Value: PostgresDynamicTypeEncodable, JSONEncoder: PostgresJSONEncoder>(
        _ value: Optional<Value>,
        context: PostgresEncodingContext<JSONEncoder>
    ) {
        switch value {
        case .none:
            self.appendNull()
        case let .some(value):
            self.append(value, context: context)
        }
    }

    @inlinable
    mutating func appendUnprotected<Value: PostgresEncodable, JSONEncoder: PostgresJSONEncoder>(
        _ value: Value,
        context: PostgresEncodingContext<JSONEncoder>
    ) throws {
        try value.encodeRaw(into: &self.bytes, context: context)
        self.metadata.append(.init(value: value, protected: false))
    }

    @inlinable
    mutating func appendUnprotected<Value: PostgresNonThrowingEncodable, JSONEncoder: PostgresJSONEncoder>(
        _ value: Value,
        context: PostgresEncodingContext<JSONEncoder>
    ) {
        value.encodeRaw(into: &self.bytes, context: context)
        self.metadata.append(.init(value: value, protected: false))
    }

    public mutating func append(_ postgresData: PostgresData) {
        switch postgresData.value {
        case .none:
            self.bytes.writeInteger(-1, as: Int32.self)
        case .some(var input):
            self.bytes.writeInteger(Int32(input.readableBytes))
            self.bytes.writeBuffer(&input)
        }
        self.metadata.append(.init(dataType: postgresData.type, format: .binary, protected: true))
    }
}

extension PostgresBindings: CustomStringConvertible, CustomDebugStringConvertible {
    // See `CustomStringConvertible.description`.
    public var description: String {
        """
        [\(zip(self.metadata, BindingsReader(buffer: self.bytes))
            .lazy.map({ Self.makeBindingPrintable(protected: $0.protected, type: $0.dataType, format: $0.format, buffer: $1) })
            .joined(separator: ", "))]
        """
    }

    // See `CustomDebugStringConvertible.description`.
    public var debugDescription: String {
        """
        [\(zip(self.metadata, BindingsReader(buffer: self.bytes))
            .lazy.map({ Self.makeDebugDescription(protected: $0.protected, type: $0.dataType, format: $0.format, buffer: $1) })
            .joined(separator: ", "))]
        """
    }

    private static func makeDebugDescription(protected: Bool, type: PostgresDataType, format: PostgresFormat, buffer: ByteBuffer?) -> String {
        "(\(Self.makeBindingPrintable(protected: protected, type: type, format: format, buffer: buffer)); \(type); format: \(format))"
    }

    private static func makeBindingPrintable(protected: Bool, type: PostgresDataType, format: PostgresFormat, buffer: ByteBuffer?) -> String {
        if protected {
            return "****"
        }

        guard var buffer = buffer else {
            return "null"
        }

        do {
            switch (type, format) {
            case (.int4, _), (.int2, _), (.int8, _):
                let number = try Int64.init(from: &buffer, type: type, format: format, context: .default)
                return String(describing: number)

            case (.bool, _):
                let bool = try Bool.init(from: &buffer, type: type, format: format, context: .default)
                return String(describing: bool)

            case (.varchar, _), (.bpchar, _), (.text, _), (.name, _):
                let value = try String.init(from: &buffer, type: type, format: format, context: .default)
                return String(reflecting: value) // adds quotes

            default:
                return "\(buffer.readableBytes) bytes"
            }
        } catch {
            return "\(buffer.readableBytes) bytes"
        }
    }
}

/// A small helper to inspect encoded bindings
private struct BindingsReader: Sequence {
    typealias Element = Optional<ByteBuffer>

    var buffer: ByteBuffer

    struct Iterator: IteratorProtocol {
        typealias Element = Optional<ByteBuffer>
        private var buffer: ByteBuffer

        init(buffer: ByteBuffer) {
            self.buffer = buffer
        }

        mutating func next() -> Optional<Optional<ByteBuffer>> {
            guard let length = self.buffer.readInteger(as: Int32.self) else {
                return .none
            }

            if length < 0 {
                return .some(.none)
            }

            return .some(self.buffer.readSlice(length: Int(length))!)
        }
    }

    func makeIterator() -> Iterator {
        Iterator(buffer: self.buffer)
    }
}
