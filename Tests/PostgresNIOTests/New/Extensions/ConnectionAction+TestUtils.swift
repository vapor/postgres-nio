import class Foundation.JSONEncoder
@testable import PostgresNIO

extension ConnectionStateMachine.ConnectionAction: Equatable {
    public static func == (lhs: Self, rhs: Self) -> Bool {
        switch (lhs, rhs) {
        case (.read, read):
            return true
        case (.wait, .wait):
            return true
        case (.provideAuthenticationContext, .provideAuthenticationContext):
            return true
        case (.sendStartupMessage, sendStartupMessage):
            return true
        case (.sendSSLRequest, sendSSLRequest):
            return true
        case (.establishSSLConnection, establishSSLConnection):
            return true
        case (.fireErrorAndCloseConnetion, fireErrorAndCloseConnetion):
            return true
        case (.sendPasswordMessage(let lhsMethod, let lhsAuthContext), sendPasswordMessage(let rhsMethod, let rhsAuthContext)):
            return lhsMethod == rhsMethod && lhsAuthContext == rhsAuthContext
        case (.sendParseDescribeBindExecuteSync(let lquery, let lbinds), sendParseDescribeBindExecuteSync(let rquery, let rbinds)):
            guard lquery == rquery else {
                return false
            }

            guard lbinds.count == rbinds.count else {
                return false
            }

            var lhsIterator = lbinds.makeIterator()
            var rhsIterator = rbinds.makeIterator()

            for _ in 0..<lbinds.count {
                let lhs = lhsIterator.next()!
                let rhs = rhsIterator.next()!

                guard lhs.psqlType == rhs.psqlType else {
                    return false
                }
                
                var lhsbuffer = ByteBuffer()
                var rhsbuffer = ByteBuffer()
                let encodingContext = PSQLEncodingContext(jsonEncoder: JSONEncoder())
                
                do {
                    try lhs._encode(into: &lhsbuffer, context: encodingContext)
                    try rhs._encode(into: &rhsbuffer, context: encodingContext)
                } catch {
                    return false
                }
                
                guard lhsbuffer == rhsbuffer else {
                    return false
                }
            }
            
            return true
        default:
            return false
        }
    }
}

extension ConnectionStateMachine {
    
    static func readyForQuery(transactionState: PSQLBackendMessage.TransactionState = .idle) -> Self {
        let paramaters = [
            "DateStyle": "ISO, MDY",
            "application_name": "",
            "server_encoding": "UTF8",
            "integer_datetimes": "on",
            "client_encoding": "UTF8",
            "TimeZone": "Etc/UTC",
            "is_superuser": "on",
            "server_version": "13.1 (Debian 13.1-1.pgdg100+1)",
            "session_authorization": "postgres",
            "IntervalStyle": "postgres",
            "standard_conforming_strings": "on"
        ]
        
        let connectionContext = ConnectionContext(
            processID: 2730,
            secretKey: 882037977,
            parameters: paramaters,
            transactionState: transactionState)
        
        return ConnectionStateMachine(.readyForQuery(connectionContext))
    }
    
    
}
