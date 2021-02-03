import Logging

extension PSQLConnection {
    @usableFromInline
    enum LoggerMetaDataKey: String {
        case connectionID = "psql_connection_id"
        case query = "psql_query"
        case name = "psql_name"
        case error = "psql_error"
        case notice = "psql_notice"
        case binds = "psql_binds"
        
        case connectionState = "psql_connection_state"
        case message = "psql_message"
        case messageID = "psql_message_id"
        case messagePayload = "psql_message_payload"
        
        
        case database = "psql_database"
        case username = "psql_username"
        
        case userEvent = "psql_user_event"
    }
}

@usableFromInline
struct PostgresLoggingMetadata: ExpressibleByDictionaryLiteral {
    @usableFromInline
    typealias Key = PSQLConnection.LoggerMetaDataKey
    @usableFromInline
    typealias Value = Logger.MetadataValue
    
    @usableFromInline var _baseRepresentation: Logger.Metadata
    
    @usableFromInline
    init(dictionaryLiteral elements: (PSQLConnection.LoggerMetaDataKey, Logger.MetadataValue)...) {
        let values = elements.lazy.map { (key, value) -> (String, Self.Value) in
            (key.rawValue, value)
        }
        
        self._baseRepresentation = Logger.Metadata(uniqueKeysWithValues: values)
    }
    
    @usableFromInline
    subscript(postgresLoggingKey loggingKey: PSQLConnection.LoggerMetaDataKey) -> Logger.Metadata.Value? {
        get {
            return self._baseRepresentation[loggingKey.rawValue]
        }
        set {
            self._baseRepresentation[loggingKey.rawValue] = newValue
        }
    }
    
    @inlinable
    var representation: Logger.Metadata {
        self._baseRepresentation
    }
}


extension Logger {
    
    static let psqlNoOpLogger = Logger(label: "psql_do_not_log", factory: { _ in SwiftLogNoOpLogHandler() })
    
    @usableFromInline
    subscript(postgresMetadataKey metadataKey: PSQLConnection.LoggerMetaDataKey) -> Logger.Metadata.Value? {
        get {
            return self[metadataKey: metadataKey.rawValue]
        }
        set {
            self[metadataKey: metadataKey.rawValue] = newValue
        }
    }
    
}

extension Logger {
    
    /// Log a message passing with the `Logger.Level.trace` log level.
    ///
    /// If `.trace` is at least as severe as the `Logger`'s `logLevel`, it will be logged,
    /// otherwise nothing will happen.
    ///
    /// - parameters:
    ///    - message: The message to be logged. `message` can be used with any string interpolation literal.
    ///    - metadata: One-off metadata to attach to this log message
    ///    - source: The source this log messages originates to. Currently, it defaults to the folder containing the
    ///              file that is emitting the log message, which usually is the module.
    ///    - file: The file this log message originates from (there's usually no need to pass it explicitly as it
    ///            defaults to `#file`).
    ///    - function: The function this log message originates from (there's usually no need to pass it explicitly as
    ///                it defaults to `#function`).
    ///    - line: The line this log message originates from (there's usually no need to pass it explicitly as it
    ///            defaults to `#line`).
    @usableFromInline
    func trace(_ message: @autoclosure () -> Logger.Message,
               metadata: @autoclosure () -> PostgresLoggingMetadata,
               source: @autoclosure () -> String? = nil,
               file: String = #file, function: String = #function, line: UInt = #line) {
        self.log(level: .trace, message(), metadata: metadata().representation, source: source(), file: file, function: function, line: line)
    }

    /// Log a message passing with the `Logger.Level.debug` log level.
    ///
    /// If `.debug` is at least as severe as the `Logger`'s `logLevel`, it will be logged,
    /// otherwise nothing will happen.
    ///
    /// - parameters:
    ///    - message: The message to be logged. `message` can be used with any string interpolation literal.
    ///    - metadata: One-off metadata to attach to this log message.
    ///    - source: The source this log messages originates to. Currently, it defaults to the folder containing the
    ///              file that is emitting the log message, which usually is the module.
    ///    - file: The file this log message originates from (there's usually no need to pass it explicitly as it
    ///            defaults to `#file`).
    ///    - function: The function this log message originates from (there's usually no need to pass it explicitly as
    ///                it defaults to `#function`).
    ///    - line: The line this log message originates from (there's usually no need to pass it explicitly as it
    ///            defaults to `#line`).
    @usableFromInline
    func debug(_ message: @autoclosure () -> Logger.Message,
               metadata: @autoclosure () -> PostgresLoggingMetadata,
               source: @autoclosure () -> String? = nil,
               file: String = #file, function: String = #function, line: UInt = #line) {
        self.log(level: .debug, message(), metadata: metadata().representation, source: source(), file: file, function: function, line: line)
    }

    /// Log a message passing with the `Logger.Level.info` log level.
    ///
    /// If `.info` is at least as severe as the `Logger`'s `logLevel`, it will be logged,
    /// otherwise nothing will happen.
    ///
    /// - parameters:
    ///    - message: The message to be logged. `message` can be used with any string interpolation literal.
    ///    - metadata: One-off metadata to attach to this log message.
    ///    - source: The source this log messages originates to. Currently, it defaults to the folder containing the
    ///              file that is emitting the log message, which usually is the module.
    ///    - file: The file this log message originates from (there's usually no need to pass it explicitly as it
    ///            defaults to `#file`).
    ///    - function: The function this log message originates from (there's usually no need to pass it explicitly as
    ///                it defaults to `#function`).
    ///    - line: The line this log message originates from (there's usually no need to pass it explicitly as it
    ///            defaults to `#line`).
    @usableFromInline
    func info(_ message: @autoclosure () -> Logger.Message,
              metadata: @autoclosure () -> PostgresLoggingMetadata,
              source: @autoclosure () -> String? = nil,
              file: String = #file, function: String = #function, line: UInt = #line) {
        self.log(level: .info, message(), metadata: metadata().representation, source: source(), file: file, function: function, line: line)
    }

    /// Log a message passing with the `Logger.Level.notice` log level.
    ///
    /// If `.notice` is at least as severe as the `Logger`'s `logLevel`, it will be logged,
    /// otherwise nothing will happen.
    ///
    /// - parameters:
    ///    - message: The message to be logged. `message` can be used with any string interpolation literal.
    ///    - metadata: One-off metadata to attach to this log message.
    ///    - source: The source this log messages originates to. Currently, it defaults to the folder containing the
    ///              file that is emitting the log message, which usually is the module.
    ///    - file: The file this log message originates from (there's usually no need to pass it explicitly as it
    ///            defaults to `#file`).
    ///    - function: The function this log message originates from (there's usually no need to pass it explicitly as
    ///                it defaults to `#function`).
    ///    - line: The line this log message originates from (there's usually no need to pass it explicitly as it
    ///            defaults to `#line`).
    @usableFromInline
    func notice(_ message: @autoclosure () -> Logger.Message,
                metadata: @autoclosure () -> PostgresLoggingMetadata,
                source: @autoclosure () -> String? = nil,
                file: String = #file, function: String = #function, line: UInt = #line) {
        self.log(level: .notice, message(), metadata: metadata().representation, source: source(), file: file, function: function, line: line)
    }

    /// Log a message passing with the `Logger.Level.warning` log level.
    ///
    /// If `.warning` is at least as severe as the `Logger`'s `logLevel`, it will be logged,
    /// otherwise nothing will happen.
    ///
    /// - parameters:
    ///    - message: The message to be logged. `message` can be used with any string interpolation literal.
    ///    - metadata: One-off metadata to attach to this log message.
    ///    - source: The source this log messages originates to. Currently, it defaults to the folder containing the
    ///              file that is emitting the log message, which usually is the module.
    ///    - file: The file this log message originates from (there's usually no need to pass it explicitly as it
    ///            defaults to `#file`).
    ///    - function: The function this log message originates from (there's usually no need to pass it explicitly as
    ///                it defaults to `#function`).
    ///    - line: The line this log message originates from (there's usually no need to pass it explicitly as it
    ///            defaults to `#line`).
    @usableFromInline
    func warning(_ message: @autoclosure () -> Logger.Message,
                 metadata: @autoclosure () -> PostgresLoggingMetadata,
                 source: @autoclosure () -> String? = nil,
                 file: String = #file, function: String = #function, line: UInt = #line) {
        self.log(level: .warning, message(), metadata: metadata().representation, source: source(), file: file, function: function, line: line)
    }

    /// Log a message passing with the `Logger.Level.error` log level.
    ///
    /// If `.error` is at least as severe as the `Logger`'s `logLevel`, it will be logged,
    /// otherwise nothing will happen.
    ///
    /// - parameters:
    ///    - message: The message to be logged. `message` can be used with any string interpolation literal.
    ///    - metadata: One-off metadata to attach to this log message.
    ///    - source: The source this log messages originates to. Currently, it defaults to the folder containing the
    ///              file that is emitting the log message, which usually is the module.
    ///    - file: The file this log message originates from (there's usually no need to pass it explicitly as it
    ///            defaults to `#file`).
    ///    - function: The function this log message originates from (there's usually no need to pass it explicitly as
    ///                it defaults to `#function`).
    ///    - line: The line this log message originates from (there's usually no need to pass it explicitly as it
    ///            defaults to `#line`).
    @usableFromInline
    func error(_ message: @autoclosure () -> Logger.Message,
               metadata: @autoclosure () -> PostgresLoggingMetadata,
               source: @autoclosure () -> String? = nil,
               file: String = #file, function: String = #function, line: UInt = #line) {
        self.log(level: .error, message(), metadata: metadata().representation, source: source(), file: file, function: function, line: line)
    }

    /// Log a message passing with the `Logger.Level.critical` log level.
    ///
    /// `.critical` messages will always be logged.
    ///
    /// - parameters:
    ///    - message: The message to be logged. `message` can be used with any string interpolation literal.
    ///    - metadata: One-off metadata to attach to this log message.
    ///    - source: The source this log messages originates to. Currently, it defaults to the folder containing the
    ///              file that is emitting the log message, which usually is the module.
    ///    - file: The file this log message originates from (there's usually no need to pass it explicitly as it
    ///            defaults to `#file`).
    ///    - function: The function this log message originates from (there's usually no need to pass it explicitly as
    ///                it defaults to `#function`).
    ///    - line: The line this log message originates from (there's usually no need to pass it explicitly as it
    ///            defaults to `#line`).
    @usableFromInline
    func critical(_ message: @autoclosure () -> Logger.Message,
                  metadata: @autoclosure () -> PostgresLoggingMetadata,
                  source: @autoclosure () -> String? = nil,
                  file: String = #file, function: String = #function, line: UInt = #line) {
        self.log(level: .critical, message(), metadata: metadata().representation, source: source(), file: file, function: function, line: line)
    }
}

