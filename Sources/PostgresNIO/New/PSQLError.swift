import NIOCore

public struct PSQLError: Error {
    struct Code: Equatable {
        struct UnsupportedAuthScheme: Equatable {
            enum Base: Equatable {
                case kerberosV5
                case scmCredential
                case gss
                case sspi
                case sasl(mechanisms: [String])
            }

            internal var base: Base

            private init(_ base: Base) {
                self.base = base
            }

            static let kerberosV5 = Self.init(.kerberosV5)
            static let scmCredential = Self.init(.scmCredential)
            static let gss = Self.init(.gss)
            static let sspi = Self.init(.sspi)
            static func sasl(mechanisms: [String]) -> Self { Self.init(.sasl(mechanisms: mechanisms)) }
        }

        internal enum Base: Equatable {
            case sslUnsupported
            case failedToAddSSLHandler
            case server(PSQLBackendMessage.ErrorResponse)
            case decoding(PSQLDecodingError)
            case unexpectedBackendMessage(PSQLBackendMessage)
            case unsupportedAuthMechanism(UnsupportedAuthScheme)
            case authMechanismRequiresPassword
            case saslError

            case tooManyParameters
            case connectionQuiescing
            case connectionClosed
            case connectionError
            case uncleanShutdown
        }

        internal var base: Base

        private init(_ base: Base) {
            self.base = base
        }

        static let sslUnsupported: Code = Self.init(.sslUnsupported)

        static let failedToAddSSLHandler: Code = Self.init(.failedToAddSSLHandler)

        static func server(_ message: PSQLBackendMessage.ErrorResponse) -> Code {
            Self.init(.server(message))
        }

        static func decoding(_ error: PSQLDecodingError) -> Code {
            Self.init(.decoding(error))
        }

        static func unexpectedBackendMessage(_ message: PSQLBackendMessage) -> Code {
            Self.init(.unexpectedBackendMessage(message))
        }

        static func unsupportedAuthMechanism(_ authScheme: Code.UnsupportedAuthScheme) -> Code {
            Self.init(.unsupportedAuthMechanism(authScheme))
        }

        static let authMechanismRequiresPassword: Code = Self.init(.authMechanismRequiresPassword)

        static let sasl: Code = Self.init(.saslError)

        static let tooManyParameters: Code = Self.init(.tooManyParameters)

        static let connectionQuiescing: Code = Self.init(.connectionQuiescing)

        static let connectionClosed: Code = Self.init(.connectionClosed)

        static let channel: Code = Self.init(.connectionError)

        static let uncleanShutdown: Code = Self.init(.uncleanShutdown)
    }
    
    internal var code: Code
    internal var underlying: Error?
    internal var file: String
    internal var line: UInt

    init(_ code: Code, underlying: Error? = nil, file: String = #file, line: UInt = #line) {
        self.code = code
        self.underlying = underlying
        self.file = file
        self.line = line
    }
}

@usableFromInline
/// An error that may happen when a ``PostgresRow`` or ``PostgresCell`` is decoded to native Swift types.
struct PostgresCastingError: Error, Equatable {
    @usableFromInline
    struct Code: Hashable, Error {
        @usableFromInline
        enum Base {
            case missingData
            case typeMismatch
            case failure
        }

        var base: Base

        @usableFromInline
        init(_ base: Base) {
            self.base = base
        }

        @usableFromInline
        static let missingData = Self.init(.missingData)
        @usableFromInline
        static let typeMismatch = Self.init(.typeMismatch)
        @usableFromInline
        static let failure = Self.init(.failure)
    }

    /// The casting error code
    let code: Code

    /// The cell's column name for which the casting failed
    let columnName: String
    /// The cell's column index for which the casting failed
    let columnIndex: Int

    /// The swift type the cell should have been casted into
    let targetType: Any.Type
    /// The cell's postgres data type for which the casting failed
    let postgresType: PostgresDataType
    /// The cell's postgres format for which the casting failed
    let postgresFormat: PostgresFormat
    /// A copy of the cell data which was attempted to be casted
    let postgresData: ByteBuffer?

    /// The file the casting/decoding was attempted in
    let file: String
    /// The line the casting/decoding was attempted in
    let line: Int
    
    var description: String {
        // This may seem very odd... But we are afraid that users might accidentally send the
        // unfiltered errors out to end-users. This may leak security relevant information. For this
        // reason we overwrite the error description by default to this generic "Database error"
        "Database error"
    }

    @usableFromInline
    init(
        code: Code,
        columnName: String,
        columnIndex: Int,
        targetType: Any.Type,
        postgresType: PostgresDataType,
        postgresFormat: PostgresFormat,
        postgresData: ByteBuffer?,
        file: String,
        line: Int
    ) {
        self.code = code
        self.columnName = columnName
        self.columnIndex = columnIndex
        self.targetType = targetType
        self.postgresType = postgresType
        self.postgresFormat = postgresFormat
        self.postgresData = postgresData

        self.file = file
        self.line = line
    }

    @usableFromInline
    static func ==(lhs: PostgresCastingError, rhs: PostgresCastingError) -> Bool {
        return lhs.code == rhs.code
            && lhs.columnName == rhs.columnName
            && lhs.columnIndex == rhs.columnIndex
            && lhs.targetType == rhs.targetType
            && lhs.postgresType == rhs.postgresType
            && lhs.postgresFormat == rhs.postgresFormat
            && lhs.postgresData == rhs.postgresData
            && lhs.file == rhs.file
            && lhs.line == rhs.line
    }
}
