/// The format the postgres types are encoded in on the wire.
///
/// Currently there a two wire formats supported:
///  - text
///  - binary
public enum PostgresFormat: Int16, Sendable {
    case text = 0
    case binary = 1
}

extension PostgresFormat: CustomStringConvertible {
    public var description: String {
        switch self {
        case .text: return "text"
        case .binary: return "binary"
        }
    }
}

// TODO: The Codable conformance does not make any sense. Let's remove this with next major break.
extension PostgresFormat: Codable {}

// TODO: Renamed during 1.x. Remove this with next major break.
@available(*, deprecated, renamed: "PostgresFormat")
public typealias PostgresFormatCode = PostgresFormat

/// The data type's raw object ID.
/// Use `select * from pg_type where oid = <idhere>;` to lookup more information.
public struct PostgresDataType: RawRepresentable, Sendable, Hashable, CustomStringConvertible {
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
    /// `22`
    public static let int2vector = PostgresDataType(22)
    /// `23`
    public static let int4 = PostgresDataType(23)
    /// `24`
    public static let regproc = PostgresDataType(24)
    /// `25`
    public static let text = PostgresDataType(25)
    /// `26`
    public static let oid = PostgresDataType(26)
    /// `27`
    public static let tid = PostgresDataType(27)
    /// `28`
    public static let xid = PostgresDataType(28)
    /// `29`
    public static let cid = PostgresDataType(29)
    /// `30`
    public static let oidvector = PostgresDataType(30)
    /// `32`
    public static let pgDDLCommand = PostgresDataType(32)
    /// `114`
    public static let json = PostgresDataType(114)
    /// `142`
    public static let xml = PostgresDataType(142)
    /// `143`
    public static let xmlArray = PostgresDataType(143)
    /// `194` pg_node_tree
    @available(*, deprecated, message: "This is internal to Postgres and should not be used.")
    public static let pgNodeTree = PostgresDataType(194)
    /// `199`
    public static let jsonArray = PostgresDataType(199)
    /// `269`
    public static let tableAMHandler = PostgresDataType(269)
    /// `271`
    public static let xid8Array = PostgresDataType(271)
    /// `325`
    public static let indexAMHandler = PostgresDataType(325)
    /// `600`
    public static let point = PostgresDataType(600)
    /// `601`
    public static let lseg = PostgresDataType(601)
    /// `602`
    public static let path = PostgresDataType(602)
    /// `603`
    public static let box = PostgresDataType(603)
    /// `604`
    public static let polygon = PostgresDataType(604)
    /// `628`
    public static let line = PostgresDataType(628)
    /// `629`
    public static let lineArray = PostgresDataType(629)
    /// `650`
    public static let cidr = PostgresDataType(650)
    /// `651`
    public static let cidrArray = PostgresDataType(651)
    /// `700`
    public static let float4 = PostgresDataType(700)
    /// `701`
    public static let float8 = PostgresDataType(701)
    /// `705`
    public static let unknown = PostgresDataType(705)
    /// `718`
    public static let circle = PostgresDataType(718)
    /// `719`
    public static let circleArray = PostgresDataType(719)
    /// `774`
    public static let macaddr8 = PostgresDataType(774)
    /// `775`
    public static let macaddr8Aray = PostgresDataType(775)
    /// `790`
    public static let money = PostgresDataType(790)
    /// `791`
    @available(*, deprecated, renamed: "moneyArray")
    public static let _money = PostgresDataType(791)
    public static let moneyArray = PostgresDataType(791)
    /// `829`
    public static let macaddr = PostgresDataType(829)
    /// `869`
    public static let inet = PostgresDataType(869)
    /// `1000` _bool
    public static let boolArray = PostgresDataType(1000)
    /// `1001` _bytea
    public static let byteaArray = PostgresDataType(1001)
    /// `1002` _char
    public static let charArray = PostgresDataType(1002)
    /// `1003` _name
    public static let nameArray = PostgresDataType(1003)
    /// `1005` _int2
    public static let int2Array = PostgresDataType(1005)
    /// `1006`
    public static let int2vectorArray = PostgresDataType(1006)
    /// `1007` _int4
    public static let int4Array = PostgresDataType(1007)
    /// `1008`
    public static let regprocArray = PostgresDataType(1008)
    /// `1009` _text
    public static let textArray = PostgresDataType(1009)
    /// `1010`
    public static let tidArray = PostgresDataType(1010)
    /// `1011`
    public static let xidArray = PostgresDataType(1011)
    /// `1012`
    public static let cidArray = PostgresDataType(1012)
    /// `1013`
    public static let oidvectorArray = PostgresDataType(1013)
    /// `1014`
    public static let bpcharArray = PostgresDataType(1014)
    /// `1015` _varchar
    public static let varcharArray = PostgresDataType(1015)
    /// `1016` _int8
    public static let int8Array = PostgresDataType(1016)
    /// `1017` _point
    public static let pointArray = PostgresDataType(1017)
    /// `1018`
    public static let lsegArray = PostgresDataType(1018)
    /// `1019`
    public static let pathArray = PostgresDataType(1019)
    /// `1020`
    public static let boxArray = PostgresDataType(1020)
    /// `1021` _float4
    public static let float4Array = PostgresDataType(1021)
    /// `1022` _float8
    public static let float8Array = PostgresDataType(1022)
    /// `1027`
    public static let polygonArray = PostgresDataType(1027)
    /// `1028`
    public static let oidArray = PostgresDataType(1018)
    /// `1033`
    public static let aclitem = PostgresDataType(1033)
    /// `1034` _aclitem
    public static let aclitemArray = PostgresDataType(1034)
    /// `1040`
    public static let macaddrArray = PostgresDataType(1040)
    /// `1041`
    public static let inetArray = PostgresDataType(1041)
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
    /// `1115` _timestamp
    public static let timestampArray = PostgresDataType(1115)
    /// `1184`
    public static let timestamptz = PostgresDataType(1184)
    /// `1185`
    public static let timestamptzArray = PostgresDataType(1185)
    /// `1186`
    public static let interval = PostgresDataType(1186)
    /// `1187`
    public static let intervalArray = PostgresDataType(1187)
    /// `1231`
    public static let numericArray = PostgresDataType(1231)
    /// `1263`
    public static let cstringArray = PostgresDataType(1263)
    /// `1266`
    public static let timetz = PostgresDataType(1266)
    /// `1270`
    public static let timetzArray = PostgresDataType(1270)
    /// `1560`
    public static let bit = PostgresDataType(1560)
    /// `1561`
    public static let bitArray = PostgresDataType(1561)
    /// `1562`
    public static let varbit = PostgresDataType(1562)
    /// `1563`
    public static let varbitArray = PostgresDataType(1563)
    /// `1700`
    public static let numeric = PostgresDataType(1700)
    /// `1790`
    public static let refcursor = PostgresDataType(1790)
    /// `2201`
    public static let refcursorArray = PostgresDataType(2201)
    /// `2202`
    public static let regprocedure = PostgresDataType(2202)
    /// `2203`
    public static let regoper = PostgresDataType(2203)
    /// `2204`
    public static let regoperator = PostgresDataType(2204)
    /// `2205`
    public static let regclass = PostgresDataType(2205)
    /// `2206`
    public static let regtype = PostgresDataType(2206)
    /// `2207`
    public static let regprocedureArray = PostgresDataType(2207)
    /// `2208`
    public static let regoperArray = PostgresDataType(2208)
    /// `2209`
    public static let regoperatorArray = PostgresDataType(2209)
    /// `2210`
    public static let regclassArray = PostgresDataType(2210)
    /// `2211`
    public static let regtypeArray = PostgresDataType(2211)
    /// `2249`
    public static let record = PostgresDataType(2249)
    /// `2275`
    public static let cstring = PostgresDataType(2275)
    /// `2276`
    public static let any = PostgresDataType(2276)
    /// `2277`
    public static let anyarray = PostgresDataType(2277)
    /// `2278`
    public static let void = PostgresDataType(2278)
    /// `2279`
    public static let trigger = PostgresDataType(2279)
    /// `2280`
    public static let languageHandler = PostgresDataType(2280)
    /// `2281`
    public static let `internal` = PostgresDataType(2281)
    /// `2283`
    public static let anyelement = PostgresDataType(2283)
    /// `2287`
    public static let recordArray = PostgresDataType(2287)
    /// `2776`
    public static let anynonarray = PostgresDataType(2776)
    /// `2950`
    public static let uuid = PostgresDataType(2950)
    /// `2951` _uuid
    public static let uuidArray = PostgresDataType(2951)
    /// `3115`
    public static let fdwHandler = PostgresDataType(3115)
    /// `3220`
    public static let pgLSN = PostgresDataType(3220)
    /// `3221`
    public static let pgLSNArray = PostgresDataType(3221)
    /// `3310`
    public static let tsmHandler = PostgresDataType(3310)
    /// `3500`
    public static let anyenum = PostgresDataType(3500)
    /// `3614`
    public static let tsvector = PostgresDataType(3614)
    /// `3615`
    public static let tsquery = PostgresDataType(3615)
    /// `3642`
    public static let gtsvector = PostgresDataType(3642)
    /// `3643`
    public static let tsvectorArray = PostgresDataType(3643)
    /// `3644`
    public static let gtsvectorArray = PostgresDataType(3644)
    /// `3645`
    public static let tsqueryArray = PostgresDataType(3645)
    /// `3734`
    public static let regconfig = PostgresDataType(3734)
    /// `3735`
    public static let regconfigArray = PostgresDataType(3735)
    /// `3769`
    public static let regdictionary = PostgresDataType(3769)
    /// `3770`
    public static let regdictionaryArray = PostgresDataType(3770)
    /// `3802`
    public static let jsonb = PostgresDataType(3802)
    /// `3807` _jsonb
    public static let jsonbArray = PostgresDataType(3807)
    /// `3831`
    public static let anyrange = PostgresDataType(3831)
    /// `3838`
    public static let eventTrigger = PostgresDataType(3838)
    /// `3904`
    public static let int4Range = PostgresDataType(3904)
    /// `3905` _int4range
    public static let int4RangeArray = PostgresDataType(3905)
    /// `3906`
    public static let numrange = PostgresDataType(3906)
    /// `3907`
    public static let numrangeArray = PostgresDataType(3907)
    /// `3908`
    public static let tsrange = PostgresDataType(3908)
    /// `3909`
    public static let tsrangeArray = PostgresDataType(3909)
    /// `3910`
    public static let tstzrange = PostgresDataType(3910)
    /// `3911`
    public static let tstzrangeArray = PostgresDataType(3911)
    /// `3912`
    public static let daterange = PostgresDataType(3912)
    /// `3913`
    public static let daterangeArray = PostgresDataType(3913)
    /// `3926`
    public static let int8Range = PostgresDataType(3926)
    /// `3927` _int8range
    public static let int8RangeArray = PostgresDataType(3927)
    /// `4072`
    public static let jsonpath = PostgresDataType(4072)
    /// `4073`
    public static let jsonpathArray = PostgresDataType(4073)
    /// `4089`
    public static let regnamespace = PostgresDataType(4089)
    /// `4090`
    public static let regnamespaceArray = PostgresDataType(4090)
    /// `4096`
    public static let regrole = PostgresDataType(4096)
    /// `4097`
    public static let regroleArray = PostgresDataType(4097)
    /// `4191`
    public static let regcollation = PostgresDataType(4191)
    /// `4192`
    public static let regcollationArray = PostgresDataType(4192)
    /// `4451`
    public static let int4multirange = PostgresDataType(4451)
    /// `4532`
    public static let nummultirange = PostgresDataType(4532)
    /// `4533`
    public static let tsmultirange = PostgresDataType(4533)
    /// `4534`
    public static let tstzmultirange = PostgresDataType(4534)
    /// `4535`
    public static let datemultirange = PostgresDataType(4535)
    /// `4536`
    public static let int8multirange = PostgresDataType(4536)
    /// `4537`
    public static let anymultirange = PostgresDataType(4537)
    /// `4538`
    public static let anycompatiblemultirange = PostgresDataType(4538)
    /// `5069`
    public static let xid8 = PostgresDataType(5069)
    /// `5077`
    public static let anycompatible = PostgresDataType(5077)
    /// `5078`
    public static let anycompatiblearray = PostgresDataType(5078)
    /// `5079`
    public static let anycompatiblenonarray = PostgresDataType(5079)
    /// `5080`
    public static let anycompatiblerange = PostgresDataType(5080)
    /// `6150`
    public static let int4multirangeArray = PostgresDataType(6150)
    /// `6151`
    public static let nummultirangeArray = PostgresDataType(6151)
    /// `6152`
    public static let tsmultirangeArray = PostgresDataType(6152)
    /// `6153`
    public static let tstzmultirangeArray = PostgresDataType(6153)
    /// `6155`
    public static let datemultirangeArray = PostgresDataType(6155)
    /// `6157`
    public static let int8multirangeArray = PostgresDataType(6157)

    /// The raw data type code recognized by PostgreSQL.
    public var rawValue: UInt32

    /// Returns `true` if the type's raw value is greater than `2^14`.
    /// This _appears_ to be true for all user-defined types, but I don't
    /// have any documentation to back this up.
    public var isUserDefined: Bool {
        self.rawValue >= 1 << 14
    }

    public init(_ rawValue: UInt32) {
        self.rawValue = rawValue
    }

    public init?(rawValue: UInt32) {
        self.init(rawValue)
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
        case .int2vector: return "INT2VECTOR"
        case .int4: return "INTEGER"
        case .regproc: return "REGPROC"
        case .text: return "TEXT"
        case .oid: return "OID"
        case .tid: return "TID"
        case .xid: return "XID"
        case .cid: return "CID"
        case .oidvector: return "OIDVECTOR"
        case .pgDDLCommand: return "PG_DDL_COMMAND"
        case .json: return "JSON"
        case .xml: return "XML"
        case .xmlArray: return "XML[]"
        case .jsonArray: return "JSON[]"
        case .tableAMHandler: return "TABLE_AM_HANDLER"
        case .xid8Array: return "XID8[]"
        case .indexAMHandler: return "INDEX_AM_HANDLER"
        case .point: return "POINT"
        case .lseg: return "LSEG"
        case .path: return "PATH"
        case .box: return "BOX"
        case .polygon: return "POLYGON"
        case .line: return "LINE"
        case .lineArray: return "LINE[]"
        case .cidr: return "CIDR"
        case .cidrArray: return "CIDR[]"
        case .float4: return "REAL"
        case .float8: return "DOUBLE PRECISION"
        case .circle: return "CIRCLE"
        case .circleArray: return "CIRCLE[]"
        case .macaddr8: return "MACADDR8"
        case .macaddr8Aray: return "MACADDR8[]"
        case .money: return "MONEY"
        case .moneyArray: return "MONEY[]"
        case .macaddr: return "MACADDR"
        case .inet: return "INET"
        case .boolArray: return "BOOLEAN[]"
        case .byteaArray: return "BYTEA[]"
        case .charArray: return "CHAR[]"
        case .nameArray: return "NAME[]"
        case .int2Array: return "SMALLINT[]"
        case .int2vectorArray: return "INT2VECTOR[]"
        case .int4Array: return "INTEGER[]"
        case .regprocArray: return "REGPROC[]"
        case .textArray: return "TEXT[]"
        case .tidArray: return "TID[]"
        case .xidArray: return "XID[]"
        case .cidArray: return "CID[]"
        case .oidvectorArray: return "OIDVECTOR[]"
        case .bpcharArray: return "CHARACTER[]"
        case .varcharArray: return "VARCHAR[]"
        case .int8Array: return "BIGINT[]"
        case .pointArray: return "POINT[]"
        case .lsegArray: return "LSEG[]"
        case .pathArray: return "PATH[]"
        case .boxArray: return "BOX[]"
        case .float4Array: return "REAL[]"
        case .float8Array: return "DOUBLE PRECISION[]"
        case .polygonArray: return "POLYGON[]"
        case .oidArray: return "OID[]"
        case .aclitem: return "ACLITEM"
        case .aclitemArray: return "ACLITEM[]"
        case .macaddrArray: return "MACADDR[]"
        case .inetArray: return "INET[]"
        case .bpchar: return "CHARACTER"
        case .varchar: return "VARCHAR"
        case .date: return "DATE"
        case .time: return "TIME"
        case .timestamp: return "TIMESTAMP"
        case .timestampArray: return "TIMESTAMP[]"
        case .timestamptz: return "TIMESTAMPTZ"
        case .timestamptzArray: return "TIMESTAMPTZ[]"
        case .interval: return "INTERVAL"
        case .intervalArray: return "INTERVAL[]"
        case .numericArray: return "NUMERIC[]"
        case .cstringArray: return "CSTRING[]"
        case .timetz: return "TIMETZ"
        case .timetzArray: return "TIMETZ[]"
        case .bit: return "BIT"
        case .bitArray: return "BIT[]"
        case .varbit: return "VARBIT"
        case .varbitArray: return "VARBIT[]"
        case .numeric: return "NUMERIC"
        case .refcursor: return "REFCURSOR"
        case .refcursorArray: return "REFCURSOR[]"
        case .regprocedure: return "REGPROCEDURE"
        case .regoper: return "REGOPER"
        case .regoperator: return "REGOPERATOR"
        case .regclass: return "REGCLASS"
        case .regtype: return "REGTYPE"
        case .regprocedureArray: return "REGPROCEDURE[]"
        case .regoperArray: return "REGOPER[]"
        case .regoperatorArray: return "REGOPERATOR[]"
        case .regclassArray: return "REGCLASS[]"
        case .regtypeArray: return "REGTYPE[]"
        case .record: return "RECORD"
        case .cstring: return "CSTRING"
        case .any: return "ANY"
        case .anyarray: return "ANYARRAY"
        case .void: return "VOID"
        case .trigger: return "TRIGGER"
        case .languageHandler: return "LANGUAGE_HANDLER"
        case .`internal`: return "INTERNAL"
        case .anyelement: return "ANYELEMENT"
        case .recordArray: return "RECORD[]"
        case .anynonarray: return "ANYNONARRAY"
        case .uuid: return "UUID"
        case .uuidArray: return "UUID[]"
        case .fdwHandler: return "FDW_HANDLER"
        case .pgLSN: return "PG_LSN"
        case .pgLSNArray: return "PG_LSN[]"
        case .tsmHandler: return "TSM_HANDLER"
        case .anyenum: return "ANYENUM"
        case .tsvector: return "TSVECTOR"
        case .tsquery: return "TSQUERY"
        case .gtsvector: return "GTSVECTOR"
        case .tsvectorArray: return "TSVECTOR[]"
        case .gtsvectorArray: return "GTSVECTOR[]"
        case .tsqueryArray: return "TSQUERY[]"
        case .regconfig: return "REGCONFIG"
        case .regconfigArray: return "REGCONFIG[]"
        case .regdictionary: return "REGDICTIONARY"
        case .regdictionaryArray: return "REGDICTIONARY[]"
        case .jsonb: return "JSONB"
        case .jsonbArray: return "JSONB[]"
        case .anyrange: return "ANYRANGE"
        case .eventTrigger: return "EVENT_TRIGGER"
        case .int4Range: return "INT4RANGE"
        case .int4RangeArray: return "INT4RANGE[]"
        case .numrange: return "NUMRANGE"
        case .numrangeArray: return "NUMRANGE[]"
        case .tsrange: return "TSRANGE"
        case .tsrangeArray: return "TSRANGE[]"
        case .tstzrange: return "TSTZRANGE"
        case .tstzrangeArray: return "TSTZRANGE[]"
        case .daterange: return "DATERANGE"
        case .daterangeArray: return "DATERANGE[]"
        case .int8Range: return "INT8RANGE"
        case .int8RangeArray: return "INT8RANGE[]"
        case .jsonpath: return "JSONPATH"
        case .jsonpathArray: return "JSONPATH[]"
        case .regnamespace: return "REGNAMESPACE"
        case .regnamespaceArray: return "REGNAMESPACE[]"
        case .regrole: return "REGROLE"
        case .regroleArray: return "REGROLE[]"
        case .regcollation: return "REGCOLLATION"
        case .regcollationArray: return "REGCOLLATION[]"
        case .int4multirange: return "INT4MULTIRANGE"
        case .nummultirange: return "NUMMULTIRANGE"
        case .tsmultirange: return "TSMULTIRANGE"
        case .tstzmultirange: return "TSTZMULTIRANGE"
        case .datemultirange: return "DATEMULTIRANGE"
        case .int8multirange: return "INT8MULTIRANGE"
        case .anymultirange: return "ANYMULTIRANGE"
        case .anycompatiblemultirange: return "ANYCOMPATIBLEMULTIRANGE"
        case .xid8: return "XID8"
        case .anycompatible: return "ANYCOMPATIBLE"
        case .anycompatiblearray: return "ANYCOMPATIBLEARRAY"
        case .anycompatiblenonarray: return "ANYCOMPATIBLENONARRAY"
        case .anycompatiblerange: return "ANYCOMPATIBLERANG"
        case .int4multirangeArray: return "INT4MULTIRANGE[]"
        case .nummultirangeArray: return "NUMMULTIRANGE[]"
        case .tsmultirangeArray: return "TSMULTIRANGE[]"
        case .tstzmultirangeArray: return "TSTZMULTIRANGE[]"
        case .datemultirangeArray: return "DATEMULTIRANGE[]"
        case .int8multirangeArray: return "INT8MULTIRANGE[]"
        default: return nil
        }
    }
    
    /// Returns the array type for this type if one is known.
    internal var arrayType: PostgresDataType? {
        switch self {
        case .xml: return .xmlArray
        case .json: return .jsonArray
        case .xid8: return .xid8Array
        case .line: return .lineArray
        case .cidr: return .cidrArray
        case .circle: return .circleArray
        case .macaddr8Aray: return .macaddr8
        case .money: return .moneyArray
        case .int2vector: return .int2vectorArray
        case .regproc: return .regprocArray
        case .tid: return .tidArray
        case .xid: return .xidArray
        case .cid: return .cidArray
        case .oidvector: return .oidvectorArray
        case .bpchar: return .bpcharArray
        case .lseg: return .lsegArray
        case .path: return .pathArray
        case .box: return .boxArray
        case .polygon: return .polygonArray
        case .oid: return .oidArray
        case .aclitem: return .aclitemArray
        case .macaddr: return .macaddrArray
        case .inet: return .inetArray
        case .timestamptz: return .timestamptzArray
        case .interval: return .intervalArray
        case .numeric: return .numericArray
        case .cstring: return .cstringArray
        case .timetz: return .timetzArray
        case .bit: return .bitArray
        case .varbit: return .varbitArray
        case .refcursor: return .refcursorArray
        case .regprocedure: return .regprocedureArray
        case .regoper: return .regoperArray
        case .regoperator: return .regoperatorArray
        case .regclass: return .regclassArray
        case .regtype: return .regtypeArray
        case .record: return .recordArray
        case .pgLSN: return .pgLSNArray
        case .tsvector: return .tsvectorArray
        case .gtsvector: return .gtsvectorArray
        case .tsquery: return .tsqueryArray
        case .regconfig: return .regconfigArray
        case .regdictionary: return .regdictionaryArray
        case .numrange: return .numrangeArray
        case .tsrange: return .tsrangeArray
        case .daterange: return .daterangeArray
        case .jsonpath: return .jsonpathArray
        case .regnamespace: return .regnamespaceArray
        case .regrole: return .regroleArray
        case .regcollation: return .regcollationArray
        case .int4multirange: return .int4multirangeArray
        case .tsmultirange: return .tsmultirangeArray
        case .tstzmultirange: return .tstzmultirangeArray
        case .datemultirange: return .datemultirange
        case .int8multirange: return .int8multirangeArray
        case .bool: return .boolArray
        case .bytea: return .byteaArray
        case .char: return .charArray
        case .name: return .nameArray
        case .int2: return .int2Array
        case .int4: return .int4Array
        case .int8: return .int8Array
        case .point: return .pointArray
        case .float4: return .float4Array
        case .float8: return .float8Array
        case .uuid: return .uuidArray
        case .jsonb: return .jsonbArray
        case .text: return .textArray
        case .varchar: return .varcharArray
        case .int4Range: return .int4RangeArray
        case .int8Range: return .int8RangeArray
        default: return nil
        }
    }

    /// Returns the element type for this type if one is known.
    /// Returns nil if this is not an array type.
    internal var elementType: PostgresDataType? {
        switch self {
        case .xmlArray: return .xml
        case .jsonArray: return .json
        case .xid8Array: return .xid8
        case .lineArray: return .line
        case .cidrArray: return .cidr
        case .circleArray: return .circle
        case .macaddr8: return .macaddr8Aray
        case .moneyArray: return .money
        case .int2vectorArray: return .int2vector
        case .regprocArray: return .regproc
        case .tidArray: return .tid
        case .xidArray: return .xid
        case .cidArray: return .cid
        case .oidvectorArray: return .oidvector
        case .bpcharArray: return .bpchar
        case .lsegArray: return .lseg
        case .pathArray: return .path
        case .boxArray: return .box
        case .polygonArray: return .polygon
        case .oidArray: return .oid
        case .aclitemArray: return .aclitem
        case .macaddrArray: return .macaddr
        case .inetArray: return .inet
        case .timestamptzArray: return .timestamptz
        case .intervalArray: return .interval
        case .numericArray: return .numeric
        case .cstringArray: return .cstring
        case .timetzArray: return .timetz
        case .bitArray: return .bit
        case .varbitArray: return .varbit
        case .refcursorArray: return .refcursor
        case .regprocedureArray: return .regprocedure
        case .regoperArray: return .regoper
        case .regoperatorArray: return .regoperator
        case .regclassArray: return .regclass
        case .regtypeArray: return .regtype
        case .recordArray: return .record
        case .pgLSNArray: return .pgLSN
        case .tsvectorArray: return .tsvector
        case .gtsvectorArray: return .gtsvector
        case .tsqueryArray: return .tsquery
        case .regconfigArray: return .regconfig
        case .regdictionaryArray: return .regdictionary
        case .numrangeArray: return .numrange
        case .tsrangeArray: return .tsrange
        case .daterangeArray: return .daterange
        case .jsonpathArray: return .jsonpath
        case .regnamespaceArray: return .regnamespace
        case .regroleArray: return .regrole
        case .regcollationArray: return .regcollation
        case .int4multirangeArray: return .int4multirange
        case .tsmultirangeArray: return .tsmultirange
        case .tstzmultirangeArray: return .tstzmultirange
        case .datemultirange: return .datemultirange
        case .int8multirangeArray: return .int8multirange
        case .boolArray: return .bool
        case .byteaArray: return .bytea
        case .charArray: return .char
        case .nameArray: return .name
        case .int2Array: return .int2
        case .int4Array: return .int4
        case .int8Array: return .int8
        case .pointArray: return .point
        case .float4Array: return .float4
        case .float8Array: return .float8
        case .uuidArray: return .uuid
        case .jsonbArray: return .jsonb
        case .textArray: return .text
        case .varcharArray: return .varchar
        case .int4RangeArray: return .int4Range
        case .int8RangeArray: return .int8Range
        default: return nil
        }
    }

    /// Returns the bound type for this type if one is known.
    /// Returns nil if this is not a range type.
    @usableFromInline
    internal var boundType: PostgresDataType? {
        switch self {
        case .int4Range: return .int4
        case .int8Range: return .int8
        case .numrange: return .numeric
        case .tsrange: return .timestamp
        case .tstzrange: return .timestamptz
        case .daterange: return .date
        default: return nil
        }
    }
    
    /// See `CustomStringConvertible`.
    public var description: String {
        return self.knownSQLName ?? "UNKNOWN \(self.rawValue)"
    }
}

// TODO: The Codable conformance does not make any sense. Let's remove this with next major break.
extension PostgresDataType: Codable {}

// TODO: The ExpressibleByIntegerLiteral conformance does not make any sense and is not used anywhere. Remove with next major break.
extension PostgresDataType: ExpressibleByIntegerLiteral {
    public init(integerLiteral value: UInt32) {
        self.init(value)
    }
}
