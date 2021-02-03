import NIO

extension PostgresConnection {
    public static func connect(
        to socketAddress: SocketAddress,
        tlsConfiguration: TLSConfiguration? = nil,
        serverHostname: String? = nil,
        logger: Logger = .init(label: "codes.vapor.postgres"),
        on eventLoop: EventLoop
    ) -> EventLoopFuture<PostgresConnection> {
        
        let coders = PSQLConnection.Configuration.Coders(
            jsonEncoder: PostgresJSONEncoderWrapper(_defaultJSONEncoder),
            jsonDecoder: PostgresJSONDecoderWrapper(_defaultJSONDecoder)
        )
        
        let configuration = PSQLConnection.Configuration(
            connection: .resolved(address: socketAddress, serverName: serverHostname),
            authentication: nil,
            tlsConfiguration: tlsConfiguration,
            coders: coders)
        
        return PSQLConnection.connect(
            configuration: configuration,
            logger: logger,
            on: eventLoop
        ).map { connection in
            PostgresConnection(underlying: connection, logger: logger)
        }.flatMapErrorThrowing { error in
            guard let psqlError = error as? PSQLError else {
                throw error
            }
            throw psqlError.toPostgresError()
        }
    }
}
