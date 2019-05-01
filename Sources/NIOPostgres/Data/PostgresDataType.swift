/// The format code being used for the field.
/// Currently will be zero (text) or one (binary).
/// In a RowDescription returned from the statement variant of Describe,
/// the format code is not yet known and will always be zero.
public enum PostgresFormatCode: Int16, Codable, CustomStringConvertible {
    case text = 0
    case binary = 1
    
    public var description: String {
        switch self {
        case .text: return "text"
        case .binary: return "binary"
        }
    }
}

/// The data type's raw object ID.
/// Use `select * from pg_type where oid = <idhere>;` to lookup more information.
public struct PostgresDataType: Codable, Equatable, ExpressibleByIntegerLiteral, CustomStringConvertible {
    /// `0`
    public static let null = PostgresDataType(0)
    /// `16`
    public static let bool = PostgresDataType(16)
    /// `17`
    public static let bytea = PostgresDataType(17)
    /// `18`
    public static let char = PostgresDataType(18)
    /// `19`
    public static let name = PostgresDataType(19)
    /// `20`
    public static let int8 = PostgresDataType(20)
    /// `21`
    public static let int2 = PostgresDataType(21)
    /// `23`
    public static let int4 = PostgresDataType(23)
    /// `24`
    public static let regproc = PostgresDataType(24)
    /// `25`
    public static let text = PostgresDataType(25)
    /// `26`
    public static let oid = PostgresDataType(26)
    /// `114`
    public static let json = PostgresDataType(114)
    /// `194`
    public static let pg_node_tree = PostgresDataType(194)
    /// `600`
    public static let point = PostgresDataType(600)
    /// `700`
    public static let float4 = PostgresDataType(700)
    /// `701`
    public static let float8 = PostgresDataType(701)
    /// `790`
    public static let money = PostgresDataType(790)
    /// `1000`
    public static let _bool = PostgresDataType(1000)
    /// `1001`
    public static let _bytea = PostgresDataType(1001)
    /// `1002`
    public static let _char = PostgresDataType(1002)
    /// `1003`
    public static let _name = PostgresDataType(1003)
    /// `1005`
    public static let _int2 = PostgresDataType(1005)
    /// `1007`
    public static let _int4 = PostgresDataType(1007)
    /// `1009`
    public static let _text = PostgresDataType(1009)
    /// `1016`
    public static let _int8 = PostgresDataType(1016)
    /// `1017`
    public static let _point = PostgresDataType(1017)
    /// `1021`
    public static let _float4 = PostgresDataType(1021)
    /// `1022`
    public static let _float8 = PostgresDataType(1022)
    /// `1034`
    public static let _aclitem = PostgresDataType(1034)
    /// `1042`
    public static let bpchar = PostgresDataType(1042)
    /// `1043`
    public static let varchar = PostgresDataType(1043)
    /// `1082`
    public static let date = PostgresDataType(1082)
    /// `1083`
    public static let time = PostgresDataType(1083)
    /// `1114`
    public static let timestamp = PostgresDataType(1114)
    /// `1115`
    public static let _timestamp = PostgresDataType(1115)
    /// `1184`
    public static let timestamptz = PostgresDataType(1184)
    /// `1266`
    public static let timetz = PostgresDataType(1266)
    /// `1700`
    public static let numeric = PostgresDataType(1700)
    /// `2278`
    public static let void = PostgresDataType(2278)
    /// `2950`
    public static let uuid = PostgresDataType(2950)
    /// `2951`
    public static let _uuid = PostgresDataType(2951)
    /// `3802`
    public static let jsonb = PostgresDataType(3802)
    /// `3807`
    public static let _jsonb = PostgresDataType(3807)

    /// The raw data type code recognized by PostgreSQL.
    public var raw: Int32
    
    /// See `ExpressibleByIntegerLiteral.init(integerLiteral:)`
    public init(integerLiteral value: Int32) {
        self.init(value)
    }
    
    public init(_ raw: Int32) {
        self.raw = raw
    }
    
    /// Returns the known SQL name, if one exists.
    /// Note: This only supports a limited subset of all PSQL types and is meant for convenience only.
    public var knownSQLName: String? {
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
        case .pg_node_tree: return "PGNODETREE"
        case .point: return "POINT"
        case .float4: return "REAL"
        case .float8: return "DOUBLE PRECISION"
        case .money: return "MONEY"
        case ._bool: return "BOOLEAN[]"
        case ._bytea: return "BYTEA[]"
        case ._char: return "CHAR[]"
        case ._name: return "NAME[]"
        case ._int2: return "SMALLINT[]"
        case ._int4: return "INTEGER[]"
        case ._text: return "TEXT[]"
        case ._int8: return "BIGINT[]"
        case ._point: return "POINT[]"
        case ._float4: return "REAL[]"
        case ._float8: return "DOUBLE PRECISION[]"
        case ._aclitem: return "ACLITEM[]"
        case .bpchar: return "BPCHAR"
        case .varchar: return "VARCHAR"
        case .date: return "DATE"
        case .time: return "TIME"
        case .timestamp: return "TIMESTAMP"
        case .timestamptz: return "TIMESTAMPTZ"
        case ._timestamp: return "TIMESTAMP[]"
        case .numeric: return "NUMERIC"
        case .void: return "VOID"
        case .uuid: return "UUID"
        case ._uuid: return "UUID[]"
        case .jsonb: return "JSONB"
        case ._jsonb: return "JSONB[]"
        default: return nil
        }
    }
    
    /// Returns the array type for this type if one is known.
    internal var arrayType: PostgresDataType? {
        switch self {
        case .bool: return ._bool
        case .bytea: return ._bytea
        case .char: return ._char
        case .name: return ._name
        case .int2: return ._int2
        case .int4: return ._int4
        case .int8: return ._int8
        case .point: return ._point
        case .float4: return ._float4
        case .float8: return ._float8
        case .uuid: return ._uuid
        case .jsonb: return ._jsonb
        case .text: return ._text
        default: return nil
        }
    }
    
    /// See `CustomStringConvertible`.
    public var description: String {
        return knownSQLName ?? "UNKNOWN \(raw)"
    }
}
