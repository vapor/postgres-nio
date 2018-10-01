import NIO

extension PostgresMessage {
    /// Identifies an incoming or outgoing postgres message. Sent as the first byte, before the message size.
    /// Values are not unique across all identifiers, meaning some messages will require keeping state to identify.
    struct Identifier: ExpressibleByIntegerLiteral, Equatable, CustomStringConvertible {
        /// AuthenticationOk (B)
        static let authenticationOk: Identifier = 0x52 // 'R'
        
        /// AuthenticationKerberosV5 (B)
        static let authenticationKerberosV5: Identifier = 0x52 // 'R'
        
        /// AuthenticationCleartextPassword (B)
        static let authenticationCleartextPassword: Identifier = 0x52 // 'R'
        
        /// AuthenticationMD5Password (B)
        static let authenticationMD5Password: Identifier = 0x52 // 'R'
        
        /// AuthenticationSCMCredential (B)
        static let authenticationSCMCredential: Identifier = 0x52 // 'R'
        
        /// AuthenticationGSS (B)
        static let authenticationGSS: Identifier = 0x52 // 'R'
        
        /// AuthenticationSSPI (B)
        static let authenticationSSPI: Identifier = 0x52 // 'R'
        
        /// AuthenticationGSSContinue (B)
        static let authenticationGSSContinue: Identifier = 0x52 // 'R'
        
        /// AuthenticationSASL (B)
        static let authenticationSASL: Identifier = 0x52 // 'R'
        
        /// AuthenticationSASLContinue (B)
        static let authenticationSASLContinue: Identifier = 0x52 // 'R'
        
        /// AuthenticationSASLFinal (B)
        static let authenticationSASLFinal: Identifier = 0x52 // 'R'
        
        /// BackendKeyData (B)
        static let backendKeyData: Identifier = 0x4B // 'K'
        
        /// Bind (F)
        static let bind: Identifier = 0x42 // 'B'
        
        /// BindComplete (B)
        static let bindComplete: Identifier = 0x32 // '2'
        
        /// Close (F)
        static let close: Identifier = 0x43 // 'C'
        
        /// CloseComplete (B)
        static let closeComplete: Identifier = 0x33 // '3'
        
        /// CommandComplete (B)
        static let commandComplete: Identifier = 0x43 // 'C'
        
        /// CopyData (F & B)
        static let copyData: Identifier = 0x64 //  'd'
        
        /// CopyDone (F & B)
        static let copyDone: Identifier = 0x63 //  'c'
        
        /// CopyFail (F)
        static let copyFail: Identifier = 0x66 // 'f'
        
        /// CopyInResponse (B)
        static let copyInResponse: Identifier = 0x47 // 'G'
        
        /// CopyOutResponse (B)
        static let copyOutResponse: Identifier = 0x48 // 'H'
        
        // CopyBothResponse (B)
        static let copyBothResponse: Identifier = 0x57 // 'W'
        
        /// DataRow (B)
        static let dataRow: Identifier = 0x44 // 'D'
        
        /// Describe (F)
        static let describe: Identifier = 0x44 // 'D'
        
        /// EmptyQueryResponse (B)
        static let emptyQueryResponse: Identifier = 0x49 // 'I'
        
        /// ErrorResponse (B)
        static let errorResponse: Identifier = 0x45 // 'E'
        
        /// Execute (F)
        static let execute: Identifier = 0x45 // 'E'
        
        /// Flush (F)
        static let flush: Identifier = 0x48 // 'H'
        
        /// FunctionCall (F)
        static let functionCall: Identifier = 0x46 // 'F'
        
        /// FunctionCallResponse (B)
        static let functionCallResponse: Identifier = 0x56 // 'V'
        
        /// GSSResponse (F)
        static let gssResponse: Identifier = 0x70 // 'p'
        
        /// NegotiateProtocolVersion (B)
        static let negotiateProtocolVersion: Identifier = 0x76 // 'v'
        
        /// NoData (B)
        static let noData: Identifier = 0x6E // 'n'
        
        /// NoticeResponse (B)
        static let noticeResponse: Identifier = 0x4E // 'N'
        
        /// NotificationResponse (B)
        static let notificationResponse: Identifier = 0x41 // 'A'
        
        /// ParameterDescription (B)
        static let parameterDescription: Identifier = 0x74 // 't'
        
        /// ParameterStatus (B)
        static let parameterStatus: Identifier = 0x53 // 'S'
        
        /// Parse (F)
        static let parse: Identifier = 0x50 // 'P'
        
        /// ParseComplete (B)
        static let parseComplete: Identifier = 0x31 // '1'
        
        /// PasswordMessage (F)
        static let passwordMessage: Identifier = 0x70 // 'p'
        
        /// PortalSuspended (B)
        static let portalSuspended: Identifier = 0x73 // 's'
        
        /// Query (F)
        static let query: Identifier = 0x51 // 'Q'
        
        /// ReadyForQuery (B)
        static let readyForQuery: Identifier = 0x5A // 'Z'
        
        /// RowDescription (B)
        static let rowDescription: Identifier = 0x54 // 'T'
        
        /// SASLInitialResponse (F)
        static let saslInitialResponse: Identifier = 0x70 // 'p'
        
        /// SASLResponse (F)
        static let saslResponse: Identifier = 0x70 // 'p'
        
        /// Sync (F)
        static let sync: Identifier = 0x53 // 'S'
        
        /// Terminate (F)
        static let terminate: Identifier = 0x58 // 'X'
        
        let value: UInt8
        
        /// See `CustomStringConvertible`.
        var description: String {
            return String(Character(Unicode.Scalar(value)))
        }
        
        /// See `ExpressibleByIntegerLiteral`.
        init(integerLiteral value: UInt8) {
            self.value = value
        }
    }
}

extension ByteBuffer {
    mutating func write(identifier: PostgresMessage.Identifier) {
        write(integer: identifier.value)
    }
}
