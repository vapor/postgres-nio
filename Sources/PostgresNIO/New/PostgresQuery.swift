struct PostgresQuery: Hashable {
    /// The query string
    var sql: String
    /// The query binds
    var binds: PostgresBindings

    init(unsafeSQL sql: String, binds: PostgresBindings = PostgresBindings()) {
        self.sql = sql
        self.binds = binds
    }
}

extension PostgresQuery: ExpressibleByStringInterpolation {
    typealias StringInterpolation = Interpolation

    init(stringInterpolation: Interpolation) {
        self.sql = stringInterpolation.sql
        self.binds = stringInterpolation.binds
    }

    init(stringLiteral value: String) {
        self.sql = value
        self.binds = PostgresBindings()
    }

    mutating func appendBinding<Value: PSQLEncodable, JSONEncoder: PostgresJSONEncoder>(
        _ value: Value,
        context: PSQLEncodingContext<JSONEncoder>
    ) throws {
        try self.binds.append(value, context: context)
    }
}

extension PostgresQuery {
    struct Interpolation: StringInterpolationProtocol {
        typealias StringLiteralType = String

        var sql: String
        var binds: PostgresBindings

        init(literalCapacity: Int, interpolationCount: Int) {
            self.sql = ""
            self.binds = PostgresBindings(capacity: interpolationCount)
        }

        mutating func appendLiteral(_ literal: String) {
            self.sql.append(contentsOf: literal)
        }

        mutating func appendInterpolation<Value: PSQLEncodable>(_ value: Value) throws {
            try self.binds.append(value, context: .default)
            self.sql.append(contentsOf: "$\(self.binds.count)")
        }

        mutating func appendInterpolation<Value: PSQLEncodable>(_ value: Optional<Value>) throws {
            try self.binds.append(value, context: .default)
            self.sql.append(contentsOf: "$\(self.binds.count)")
        }

        mutating func appendInterpolation<Value: PSQLEncodable & Encodable, JSONEncoder: PostgresJSONEncoder>(
            _ value: Value,
            context: PSQLEncodingContext<JSONEncoder>
        ) throws {
            try self.binds.append(value, context: context)
            self.sql.append(contentsOf: "$\(self.binds.count)")
        }
    }
}

struct PSQLExecuteStatement {
    /// The statements name
    var name: String
    /// The binds
    var binds: PostgresBindings

    var rowDescription: RowDescription?
}

struct PostgresBindings: Hashable {
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

    init() {
        self.metadata = []
        self.bytes = ByteBuffer()
    }

    init(capacity: Int) {
        self.metadata = []
        self.metadata.reserveCapacity(capacity)
        self.bytes = ByteBuffer()
        self.bytes.reserveCapacity(128 * capacity)
    }

    mutating func append<Value: PSQLEncodable, JSONEncoder: PostgresJSONEncoder>(
        _ value: Value,
        context: PSQLEncodingContext<JSONEncoder>
    ) throws {
        try value.encodeRaw(into: &self.bytes, context: context)
        self.metadata.append(.init(value: value))
    }
}
