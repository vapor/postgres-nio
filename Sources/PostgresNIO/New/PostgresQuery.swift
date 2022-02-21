public struct PostgresQuery: Hashable {
    /// The query string
    public var sql: String
    /// The query binds
    public var binds: PostgresBindings

    init(unsafeSQL sql: String, binds: PostgresBindings = PostgresBindings()) {
        self.sql = sql
        self.binds = binds
    }
}

extension PostgresQuery: ExpressibleByStringInterpolation {
    public typealias StringInterpolation = Interpolation

    public init(stringInterpolation: Interpolation) {
        self.sql = stringInterpolation.sql
        self.binds = stringInterpolation.binds
    }

    public init(stringLiteral value: String) {
        self.sql = value
        self.binds = PostgresBindings()
    }

    public mutating func appendBinding<Value: PSQLEncodable, JSONEncoder: PostgresJSONEncoder>(
        _ value: Value,
        context: PSQLEncodingContext<JSONEncoder>
    ) throws {
        try self.binds.append(value, context: context)
    }
}

extension PostgresQuery {
    public struct Interpolation: StringInterpolationProtocol {
        public typealias StringLiteralType = String

        var sql: String
        var binds: PostgresBindings

        public init(literalCapacity: Int, interpolationCount: Int) {
            self.sql = ""
            self.binds = PostgresBindings(capacity: interpolationCount)
        }

        public mutating func appendLiteral(_ literal: String) {
            self.sql.append(contentsOf: literal)
        }

        public mutating func appendInterpolation<Value: PSQLEncodable>(_ value: Value) throws {
            try self.binds.append(value, context: .default)
            self.sql.append(contentsOf: "$\(self.binds.count)")
        }

        public mutating func appendInterpolation<Value: PSQLEncodable>(_ value: Optional<Value>) throws {
            try self.binds.append(value, context: .default)
            self.sql.append(contentsOf: "$\(self.binds.count)")
        }

        public mutating func appendInterpolation<Value: PSQLEncodable, JSONEncoder: PostgresJSONEncoder>(
            _ value: Value,
            context: PSQLEncodingContext<JSONEncoder>
        ) throws {
            try self.binds.append(value, context: context)
            self.sql.append(contentsOf: "$\(self.binds.count)")
        }
    }
}

public struct PSQLExecuteStatement {
    /// The statements name
    public var name: String
    /// The binds
    public var binds: PostgresBindings

    var rowDescription: RowDescription?
}

public struct PostgresBindings: Hashable {
    struct Metadata: Hashable {
        var dataType: PostgresDataType
        var format: PostgresFormat

        init(dataType: PostgresDataType, format: PostgresFormat) {
            self.dataType = dataType
            self.format = format
        }

        init<Value: PSQLEncodable>(value: Value) {
            self.init(dataType: value.psqlType, format: value.psqlFormat)
        }
    }

    var metadata: [Metadata]
    var bytes: ByteBuffer

    var count: Int {
        self.metadata.count
    }

    public init() {
        self.metadata = []
        self.bytes = ByteBuffer()
    }

    init(capacity: Int) {
        self.metadata = []
        self.metadata.reserveCapacity(capacity)
        self.bytes = ByteBuffer()
        self.bytes.reserveCapacity(128 * capacity)
    }

    public mutating func append<Value: PSQLEncodable, JSONEncoder: PostgresJSONEncoder>(
        _ value: Value,
        context: PSQLEncodingContext<JSONEncoder>
    ) throws {
        try value.encodeRaw(into: &self.bytes, context: context)
        self.metadata.append(.init(value: value))
    }
}
