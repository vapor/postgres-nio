import NIOCore

extension PostgresMessage {
    /// Identifies an incoming or outgoing postgres message. Sent as the first byte, before the message size.
    /// Values are not unique across all identifiers, meaning some messages will require keeping state to identify.
    @available(*, deprecated, message: "Will be removed from public API.")
    public struct Identifier: ExpressibleByIntegerLiteral, Equatable, CustomStringConvertible {
        // special
        public static let none: Identifier = 0x00
        // special
        public static let sslSupported: Identifier = 0x53 // 'S'
        // special
        public static let sslUnsupported: Identifier = 0x4E // 'N'
        
        /// Authentication (B)
        public static let authentication: Identifier = 0x52 // 'R'
        
        /// BackendKeyData (B)
        public static let backendKeyData: Identifier = 0x4B // 'K'
        
        /// Bind (F)
        public static let bind: Identifier = 0x42 // 'B'
        
        /// BindComplete (B)
        public static let bindComplete: Identifier = 0x32 // '2'
        
        /// Close (F)
        public static let close: Identifier = 0x43 // 'C'
        
        /// CloseComplete (B)
        public static let closeComplete: Identifier = 0x33 // '3'
        
        /// CommandComplete (B)
        public static let commandComplete: Identifier = 0x43 // 'C'
        
        /// CopyData (F & B)
        public static let copyData: Identifier = 0x64 //  'd'
        
        /// CopyDone (F & B)
        public static let copyDone: Identifier = 0x63 //  'c'
        
        /// CopyFail (F)
        public static let copyFail: Identifier = 0x66 // 'f'
        
        /// CopyInResponse (B)
        public static let copyInResponse: Identifier = 0x47 // 'G'
        
        /// CopyOutResponse (B)
        public static let copyOutResponse: Identifier = 0x48 // 'H'
        
        // CopyBothResponse (B)
        public static let copyBothResponse: Identifier = 0x57 // 'W'
        
        /// DataRow (B)
        public static let dataRow: Identifier = 0x44 // 'D'
        
        /// Describe (F)
        public static let describe: Identifier = 0x44 // 'D'
        
        /// EmptyQueryResponse (B)
        public static let emptyQueryResponse: Identifier = 0x49 // 'I'
        
        /// ErrorResponse (B)
        public static let error: Identifier = 0x45 // 'E'
        
        /// Execute (F)
        public static let execute: Identifier = 0x45 // 'E'
        
        /// Flush (F)
        public static let flush: Identifier = 0x48 // 'H'
        
        /// FunctionCall (F)
        public static let functionCall: Identifier = 0x46 // 'F'
        
        /// FunctionCallResponse (B)
        public static let functionCallResponse: Identifier = 0x56 // 'V'
        
        /// GSSResponse (F)
        public static let gssResponse: Identifier = 0x70 // 'p'
        
        /// NegotiateProtocolVersion (B)
        public static let negotiateProtocolVersion: Identifier = 0x76 // 'v'
        
        /// NoData (B)
        public static let noData: Identifier = 0x6E // 'n'
        
        /// NoticeResponse (B)
        public static let notice: Identifier = 0x4E // 'N'
        
        /// NotificationResponse (B)
        public static let notificationResponse: Identifier = 0x41 // 'A'
        
        /// ParameterDescription (B)
        public static let parameterDescription: Identifier = 0x74 // 't'
        
        /// ParameterStatus (B)
        public static let parameterStatus: Identifier = 0x53 // 'S'
        
        /// Parse (F)
        public static let parse: Identifier = 0x50 // 'P'
        
        /// ParseComplete (B)
        public static let parseComplete: Identifier = 0x31 // '1'
        
        /// PasswordMessage (F)
        public static let passwordMessage: Identifier = 0x70 // 'p'
        
        /// PortalSuspended (B)
        public static let portalSuspended: Identifier = 0x73 // 's'
        
        /// Query (F)
        public static let query: Identifier = 0x51 // 'Q'
        
        /// ReadyForQuery (B)
        public static let readyForQuery: Identifier = 0x5A // 'Z'
        
        /// RowDescription (B)
        public static let rowDescription: Identifier = 0x54 // 'T'
        
        /// SASLInitialResponse (F)
        public static let saslInitialResponse: Identifier = 0x70 // 'p'
        
        /// SASLResponse (F)
        public static let saslResponse: Identifier = 0x70 // 'p'
        
        /// Sync (F)
        public static let sync: Identifier = 0x53 // 'S'
        
        /// Terminate (F)
        public static let terminate: Identifier = 0x58 // 'X'
        
        public let value: UInt8
        
        /// See `CustomStringConvertible`.
        public var description: String {
            return String(Unicode.Scalar(self.value))
        }
        
        /// See `ExpressibleByIntegerLiteral`.
        public init(integerLiteral value: UInt8) {
            self.value = value
        }
    }
}

extension ByteBuffer {
    @available(*, deprecated, message: "Will be removed from public API")
    mutating func write(identifier: PostgresMessage.Identifier) {
        self.writeInteger(identifier.value)
    }
}
