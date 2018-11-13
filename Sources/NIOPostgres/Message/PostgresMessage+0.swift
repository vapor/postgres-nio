// note: Please list enum cases alphabetically.

/// A frontend or backend Postgres message.
enum PostgresMessage {
    /// One of the various authentication request message formats.
    case authentication(Authentication)
    
    /// Identifies the message as cancellation key data.
    /// The frontend must save these values if it wishes to be able to issue CancelRequest messages later.
    case backendKeyData(BackendKeyData)
    
    /// Identifies the message as a Bind command.
    case bind(Bind)
    
    /// Identifies the message as a Bind-complete indicator.
    case bindComplete
    
    /// Identifies the message as a command-completed response.
    case commandComplete(CommandComplete)
    
    /// Identifies the message as a data row.
    case dataRow(DataRow)
    
    /// Identifies the message as a Describe command.
    case describe(Describe)
    
    /// Identifies the message as an error.
    case error(Error)
    
    /// Identifies the message as an Execute command.
    case execute(Execute)
    
    /// Identifies the message as a no-data indicator.
    case noData
    
    /// Identifies the message as a parameter description.
    case parameterDescription(ParameterDescription)
    
    /// Identifies the message as a Parse command.
    case parse(Parse)
    
    /// Identifies the message as a Parse-complete indicator.
    case parseComplete
    
    /// Identifies the message as a run-time parameter status report.
    case parameterStatus(ParameterStatus)
    
    /// Identifies the message as a password response.
    case password(Password)
    
    /// Identifies the message type. ReadyForQuery is sent whenever the backend is ready for a new query cycle.
    case readyForQuery(ReadyForQuery)
    
    /// Identifies the message as a row description.
    case rowDescription(RowDescription)
    
    /// Identifies the message as a simple query.
    case simpleQuery(SimpleQuery)
    
    case sslRequest(SSLRequest)
    
    case sslResponse(SSLResponse)
    
    /// Startup message
    case startup(Startup)
    
    /// Identifies the message as a Sync command.
    case sync
}
