import NIOCore

/// The format the postgres types are encoded in on the wire.
///
/// Currently there a two wire formats supported:
///  - text
///  - binary
public enum PSQLFormat: Int16, Hashable {
    case text = 0
    case binary = 1
}

struct PSQLCell: Equatable {
    
    var bytes: ByteBuffer?
    var columnIndex: Int
    var columnDescription: RowDescription.Column
    
    /// use this only for testing
    init(bytes: ByteBuffer?, columnIndex: Int, columnDescription: RowDescription.Column) {
        self.bytes = bytes
        self.columnIndex = columnIndex
        self.columnDescription = columnDescription
    }
}

extension PSQLCell {
    func decode<T: PSQLDecodable, JSONDecoder: PSQLJSONDecoder>(_: T.Type, context: PSQLDecodingContext<JSONDecoder>, file: String = #file, line: UInt = #line) throws -> T {
        var cellData = self.bytes

        do {
            return try T.decodeRaw(
                from: &cellData,
                type: self.columnDescription.dataType,
                format: self.columnDescription.format,
                context: context
            )
        } catch let code as PSQLCastingError.Code {
            throw PSQLCastingError(
                code: code,
                columnName: self.columnDescription.name,
                columnIndex: self.columnIndex,
                targetType: T.self,
                postgresType: self.columnDescription.dataType,
                postgresData: cellData,
                file: file,
                line: line
            )
        }
    }
}

public struct PSQLDataType: RawRepresentable, Hashable, CustomStringConvertible {
    public typealias RawValue = Int32
    
    /// The raw data type code recognized by PostgreSQL.
    public var rawValue: Int32
    
    /// `0`
    public static let null = PSQLDataType(0)
    /// `16`
    public static let bool = PSQLDataType(16)
    /// `17`
    public static let bytea = PSQLDataType(17)
    /// `18`
    public static let char = PSQLDataType(18)
    /// `19`
    public static let name = PSQLDataType(19)
    /// `20`
    public static let int8 = PSQLDataType(20)
    /// `21`
    public static let int2 = PSQLDataType(21)
    /// `23`
    public static let int4 = PSQLDataType(23)
    /// `24`
    public static let regproc = PSQLDataType(24)
    /// `25`
    public static let text = PSQLDataType(25)
    /// `26`
    public static let oid = PSQLDataType(26)
    /// `114`
    public static let json = PSQLDataType(114)
    /// `194` pg_node_tree
    public static let pgNodeTree = PSQLDataType(194)
    /// `600`
    public static let point = PSQLDataType(600)
    /// `700`
    public static let float4 = PSQLDataType(700)
    /// `701`
    public static let float8 = PSQLDataType(701)
    /// `790`
    public static let money = PSQLDataType(790)
    /// `1000` _bool
    public static let boolArray = PSQLDataType(1000)
    /// `1001` _bytea
    public static let byteaArray = PSQLDataType(1001)
    /// `1002` _char
    public static let charArray = PSQLDataType(1002)
    /// `1003` _name
    public static let nameArray = PSQLDataType(1003)
    /// `1005` _int2
    public static let int2Array = PSQLDataType(1005)
    /// `1007` _int4
    public static let int4Array = PSQLDataType(1007)
    /// `1009` _text
    public static let textArray = PSQLDataType(1009)
    /// `1015` _varchar
    public static let varcharArray = PSQLDataType(1015)
    /// `1016` _int8
    public static let int8Array = PSQLDataType(1016)
    /// `1017` _point
    public static let pointArray = PSQLDataType(1017)
    /// `1021` _float4
    public static let float4Array = PSQLDataType(1021)
    /// `1022` _float8
    public static let float8Array = PSQLDataType(1022)
    /// `1034` _aclitem
    public static let aclitemArray = PSQLDataType(1034)
    /// `1042`
    public static let bpchar = PSQLDataType(1042)
    /// `1043`
    public static let varchar = PSQLDataType(1043)
    /// `1082`
    public static let date = PSQLDataType(1082)
    /// `1083`
    public static let time = PSQLDataType(1083)
    /// `1114`
    public static let timestamp = PSQLDataType(1114)
    /// `1115` _timestamp
    public static let timestampArray = PSQLDataType(1115)
    /// `1184`
    public static let timestamptz = PSQLDataType(1184)
    /// `1266`
    public static let timetz = PSQLDataType(1266)
    /// `1700`
    public static let numeric = PSQLDataType(1700)
    /// `2278`
    public static let void = PSQLDataType(2278)
    /// `2950`
    public static let uuid = PSQLDataType(2950)
    /// `2951` _uuid
    public static let uuidArray = PSQLDataType(2951)
    /// `3802`
    public static let jsonb = PSQLDataType(3802)
    /// `3807` _jsonb
    public static let jsonbArray = PSQLDataType(3807)

    /// Returns `true` if the type's raw value is greater than `2^14`.
    /// This _appears_ to be true for all user-defined types, but I don't
    /// have any documentation to back this up.
    var isUserDefined: Bool {
        self.rawValue >= 1 << 14
    }
    
    init(_ rawValue: Int32) {
        self.rawValue = rawValue
    }

    public init(rawValue: Int32) {
        self.init(rawValue)
    }
    
    /// Returns the known SQL name, if one exists.
    /// Note: This only supports a limited subset of all PSQL types and is meant for convenience only.
    var knownSQLName: String? {
        switch self {
        case .bool: return "BOOLEAN"
        case .bytea: return "BYTEA"
        case .char: return "CHAR"
        case .name: return "NAME"
        case .int8: return "BIGINT"
        case .int2: return "SMALLINT"
        case .int4: return "INTEGER"
        case .regproc: return "REGPROC"
        case .text: return "TEXT"
        case .oid: return "OID"
        case .json: return "JSON"
        case .pgNodeTree: return "PGNODETREE"
        case .point: return "POINT"
        case .float4: return "REAL"
        case .float8: return "DOUBLE PRECISION"
        case .money: return "MONEY"
        case .boolArray: return "BOOLEAN[]"
        case .byteaArray: return "BYTEA[]"
        case .charArray: return "CHAR[]"
        case .nameArray: return "NAME[]"
        case .int2Array: return "SMALLINT[]"
        case .int4Array: return "INTEGER[]"
        case .textArray: return "TEXT[]"
        case .varcharArray: return "VARCHAR[]"
        case .int8Array: return "BIGINT[]"
        case .pointArray: return "POINT[]"
        case .float4Array: return "REAL[]"
        case .float8Array: return "DOUBLE PRECISION[]"
        case .aclitemArray: return "ACLITEM[]"
        case .bpchar: return "BPCHAR"
        case .varchar: return "VARCHAR"
        case .date: return "DATE"
        case .time: return "TIME"
        case .timestamp: return "TIMESTAMP"
        case .timestamptz: return "TIMESTAMPTZ"
        case .timestampArray: return "TIMESTAMP[]"
        case .numeric: return "NUMERIC"
        case .void: return "VOID"
        case .uuid: return "UUID"
        case .uuidArray: return "UUID[]"
        case .jsonb: return "JSONB"
        case .jsonbArray: return "JSONB[]"
        default: return nil
        }
    }
    
    /// See `CustomStringConvertible`.
    public var description: String {
        return self.knownSQLName ?? "UNKNOWN \(self.rawValue)"
    }
}

