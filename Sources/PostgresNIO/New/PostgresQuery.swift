import NIOCore

/// A Postgres SQL query, that can be executed on a Postgres server. Contains the raw sql string and bindings.
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
    public struct StringInterpolation: StringInterpolationProtocol {
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
        public mutating func appendInterpolation<Value: PostgresEncodable>(_ value: Value) throws {
            try self.binds.append(value, context: .default)
            self.sql.append(contentsOf: "$\(self.binds.count)")
        }

        @inlinable
        public mutating func appendInterpolation<Value: PostgresEncodable>(_ value: Optional<Value>) throws {
            switch value {
            case .none:
                self.binds.appendNull()
            case .some(let value):
                try self.binds.append(value, context: .default)
            }

            self.sql.append(contentsOf: "$\(self.binds.count)")
        }

        @inlinable
        public mutating func appendInterpolation<Value: PostgresNonThrowingEncodable>(_ value: Value) {
            self.binds.append(value, context: .default)
            self.sql.append(contentsOf: "$\(self.binds.count)")
        }

        @inlinable
        public mutating func appendInterpolation<Value: PostgresNonThrowingEncodable>(_ value: Optional<Value>) {
            switch value {
            case .none:
                self.binds.appendNull()
            case .some(let value):
                self.binds.append(value, context: .default)
            }

            self.sql.append(contentsOf: "$\(self.binds.count)")
        }

        @inlinable
        public mutating func appendInterpolation<Value: PostgresEncodable, JSONEncoder: PostgresJSONEncoder>(
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
    /// See ``Swift/CustomStringConvertible/description``.
    public var description: String {
        "\(self.sql) \(self.binds)"
    }
}

extension PostgresQuery: CustomDebugStringConvertible {
    /// See ``Swift/CustomDebugStringConvertible/debugDescription``.
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
        init<Value: PostgresEncodable>(value: Value, protected: Bool) {
            self.init(dataType: Value.psqlType, format: Value.psqlFormat, protected: protected)
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
    public mutating func append<Value: PostgresEncodable, JSONEncoder: PostgresJSONEncoder>(
        _ value: Value,
        context: PostgresEncodingContext<JSONEncoder>
    ) throws {
        try value.encodeRaw(into: &self.bytes, context: context)
        self.metadata.append(.init(value: value, protected: true))
    }

    @inlinable
    public mutating func append<Value: PostgresNonThrowingEncodable, JSONEncoder: PostgresJSONEncoder>(
        _ value: Value,
        context: PostgresEncodingContext<JSONEncoder>
    ) {
        value.encodeRaw(into: &self.bytes, context: context)
        self.metadata.append(.init(value: value, protected: true))
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
    /// See ``Swift/CustomStringConvertible/description``.
    public var description: String {
        """
        [\(zip(self.metadata, BindingsReader(buffer: self.bytes))
            .lazy.map({ Self.makeBindingPrintable(protected: $0.protected, type: $0.dataType, format: $0.format, buffer: $1) })
            .joined(separator: ", "))]
        """
    }

    /// See ``Swift/CustomDebugStringConvertible/description``.
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
        print(protected)
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
