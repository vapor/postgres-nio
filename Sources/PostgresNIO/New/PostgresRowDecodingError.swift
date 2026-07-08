public struct PostgresRowDecodingError: Error {
    public struct Code: CustomStringConvertible, Hashable, Sendable {
        enum Base {
            case columnCountMismatch
        }
        
        var base: Base
        
        init(_ base: Base) {
            self.base = base
        }
        
        public static let columnCountMismatch = Self.init(.columnCountMismatch)
        
        public var description: String {
            switch base {
            case .columnCountMismatch:
                "columnCountMismatch"
            }
        }
    }
    
    /// The actual error code
    public let code: Code
    
    /// The expected number of columns to be returned (per `decode(…)`)
    public var expectedColumns: Int
    /// The actual number of columns returned from the database
    public var returnedColumns: Int
    
    /// The file the decoding was attempted in.
    public var file: String
    /// The line the decoding was attempted in.
    public var line: Int
    
    @usableFromInline
    init(
        code: Code, expectedColumns: Int, returnedColumns: Int, file: String, line: Int
    ) {
        self.code = code
        self.expectedColumns = expectedColumns
        self.returnedColumns = returnedColumns
        self.file = file
        self.line = line
    }
}

extension PostgresRowDecodingError: CustomStringConvertible {
    public var description: String {
        // This may seem very odd... But we are afraid that users might accidentally send the
        // unfiltered errors out to end-users. This may leak security relevant information. For this
        // reason we overwrite the error description by default to this generic error
        """
        PostgresRowDecodingError – Generic description to prevent accidental leakage of sensitive data. For debugging \
        details, use `String(reflecting: error)`.
        """
    }
}

extension PostgresRowDecodingError: CustomDebugStringConvertible {
    public var debugDescription: String {
        """
        PostgresRowDecodingError(code: \(code)\
        , expectedColumns: \(expectedColumns)\
        , returnedColumns: \(returnedColumns)\
        , file: \(String(reflecting: file))\
        , line: \(line)\
        )
        """
    }
}
