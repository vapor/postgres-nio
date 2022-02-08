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
struct PSQLCastingError: Error, Equatable {
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

    var code: Code
    
    let columnName: String
    let columnIndex: Int
    
    let targetType: Any.Type
    let postgresType: PSQLDataType
    let postgresData: ByteBuffer?

    let file: String
    let line: UInt

    @usableFromInline
    init(
        code: Code,
        columnName: String,
        columnIndex: Int,
        targetType: Any.Type,
        postgresType: PSQLDataType,
        postgresData: ByteBuffer?,
        file: String,
        line: UInt
    ) {
        self.code = code
        self.columnName = columnName
        self.columnIndex = columnIndex
        self.targetType = targetType
        self.postgresType = postgresType
        self.postgresData = postgresData

        self.file = file
        self.line = line
    }
    
    var description: String {
        switch self.code.base {
        case .missingData:
            return """
                Failed to cast Postgres data type \(self.postgresType.description) to Swift type \(self.targetType) \
                because of missing data.
                """

        case .typeMismatch:
            preconditionFailure()

        case .failure:
            return """
                Failed to cast Postgres data type \(self.postgresType.description) to Swift type \(self.targetType).
                """
        }

    }

    @usableFromInline
    static func ==(lhs: PSQLCastingError, rhs: PSQLCastingError) -> Bool {
        lhs.targetType == rhs.targetType
    }
}
