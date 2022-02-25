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

        public mutating func appendInterpolation<Value: PostgresEncodable>(_ value: Value) throws {
            try self.binds.append(value, context: .default)
            self.sql.append(contentsOf: "$\(self.binds.count)")
        }

        public mutating func appendInterpolation<Value: PostgresEncodable>(_ value: Optional<Value>) throws {
            switch value {
            case .none:
                self.binds.appendNull()
            case .some(let value):
                try self.binds.append(value, context: .default)
            }
            self.sql.append(contentsOf: "$\(self.binds.count)")
        }

        public mutating func appendInterpolation<Value: PostgresEncodable, JSONEncoder: PostgresJSONEncoder>(
            _ value: Value,
            context: PostgresEncodingContext<JSONEncoder>
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

        init<Value: PostgresEncodable>(value: Value) {
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

    public mutating func appendNull() {
        self.bytes.writeInteger(-1, as: Int32.self)
        self.metadata.append(.init(dataType: .null, format: .binary))
    }

    public mutating func append(_ postgresData: PostgresData) {
        switch postgresData.value {
        case .none:
            self.bytes.writeInteger(-1, as: Int32.self)
        case .some(var input):
            self.bytes.writeInteger(Int32(input.readableBytes))
            self.bytes.writeBuffer(&input)
        }
        self.metadata.append(.init(dataType: postgresData.type, format: .binary))
    }

    public mutating func append<Value: PostgresEncodable, JSONEncoder: PostgresJSONEncoder>(
        _ value: Value,
        context: PostgresEncodingContext<JSONEncoder>
    ) throws {
        try value.encodeRaw(into: &self.bytes, context: context)
        self.metadata.append(.init(value: value))
    }
}
