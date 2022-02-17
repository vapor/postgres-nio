import NIOCore
import NIOSSL
import Logging

extension PostgresConnection {
    public static func connect(
        to socketAddress: SocketAddress,
        tlsConfiguration: TLSConfiguration? = nil,
        serverHostname: String? = nil,
        logger: Logger = .init(label: "codes.vapor.postgres"),
        on eventLoop: EventLoop
    ) -> EventLoopFuture<PostgresConnection> {
        
        let coders = PSQLConnection.Configuration.Coders(
            jsonEncoder: PostgresJSONEncoderWrapper(_defaultJSONEncoder)
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
            throw error.asAppropriatePostgresError
        }
    }
}
