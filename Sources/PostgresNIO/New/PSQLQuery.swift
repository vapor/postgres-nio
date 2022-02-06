public struct PSQLQuery: Hashable {
    /// The query string
    public var query: String
    /// The query binds
    public var binds: PSQLBindings

    init(_ query: String, binds: PSQLBindings) {
        self.query = query
        self.binds = binds
    }
}

extension PSQLQuery: ExpressibleByStringInterpolation {
    public typealias StringInterpolation = Interpolation

    public init(stringInterpolation: Interpolation) {
        self.query = stringInterpolation.query
        self.binds = stringInterpolation.binds
    }

    public init(stringLiteral value: String) {
        self.query = value
        self.binds = PSQLBindings()
    }

    public mutating func appendBinding<Value: PSQLEncodable, JSONEncoder: PSQLJSONEncoder>(
        _ value: Value,
        context: PSQLEncodingContext<JSONEncoder>
    ) throws {
        try self.binds.append(value, context: context)
    }
}

extension PSQLQuery {
    public struct Interpolation: StringInterpolationProtocol {
        public typealias StringLiteralType = String

        var query: String
        var binds: PSQLBindings

        public init(literalCapacity: Int, interpolationCount: Int) {
            self.query = ""
            self.binds = PSQLBindings()
        }

        public mutating func appendLiteral(_ literal: String) {
            self.query.append(contentsOf: literal)
        }

        public mutating func appendInterpolation<Value: PSQLEncodable>(_ value: Value) throws {
            try self.binds.append(value, context: .default)
            self.query.append(contentsOf: "$\(self.binds.count)")
        }
    }
}

public struct PSQLExecuteStatement {
    /// The statements name
    public var name: String
    /// The binds
    public var binds: PSQLBindings

    var rowDescription: RowDescription?
}

public struct PSQLBindings: Hashable {
    struct Metadata: Hashable {
        var dataType: PSQLDataType
        var format: PSQLFormat

        init(dataType: PSQLDataType, format: PSQLFormat) {
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

    public mutating func append<Value: PSQLEncodable, JSONEncoder: PSQLJSONEncoder>(
        _ value: Value,
        context: PSQLEncodingContext<JSONEncoder>
    ) throws {
        try value.encodeRaw(into: &self.bytes, context: context)
        self.metadata.append(.init(value: value))
    }

    public mutating func _append<JSONEncoder: PSQLJSONEncoder>(
        _ value: PSQLEncodable,
        context: PSQLEncodingContext<JSONEncoder>
    ) throws {
        try value.encodeRaw(into: &self.bytes, context: context)
        self.metadata.append(.init(dataType: value.psqlType, format: value.psqlFormat))
    }
}
