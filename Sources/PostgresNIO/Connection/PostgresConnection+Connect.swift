import Logging
import NIO

extension PostgresConnection {
    public static func connect(
        to socketAddress: SocketAddress,
        tlsConfiguration: TLSConfiguration? = nil,
        serverHostname: String? = nil,
        logger: Logger = .init(label: "codes.vapor.postgres"),
        on eventLoop: EventLoop
    ) -> EventLoopFuture<PostgresConnection> {
        let bootstrap = ClientBootstrap(group: eventLoop)
            .channelOption(ChannelOptions.socket(SocketOptionLevel(SOL_SOCKET), SO_REUSEADDR), value: 1)
        return bootstrap.connect(to: socketAddress).flatMap { channel in
            let requestHandler = PostgresRequestHandler(logger: logger)
            return channel.pipeline.addHandlers([
                ByteToMessageHandler(PostgresMessageDecoder()),
                MessageToByteHandler(PostgresMessageEncoder()),
                requestHandler,
                PostgresErrorHandler(logger: logger)
            ]).map {
                return PostgresConnection(channel: channel, requestHandler: requestHandler, logger: logger)
            }
        }.flatMap { (conn: PostgresConnection) in
            if let tlsConfiguration = tlsConfiguration {
                return conn.requestTLS(
                    using: tlsConfiguration,
                    serverHostname: serverHostname,
                    logger: logger
                ).map { conn }
            } else {
                return eventLoop.makeSucceededFuture(conn)
            }
        }
    }
}


private final class PostgresErrorHandler: ChannelInboundHandler {
    typealias InboundIn = Never
    
    let logger: Logger
    init(logger: Logger) {
        self.logger = logger
    }
    
    func errorCaught(context: ChannelHandlerContext, error: Error) {
        self.logger.error("Uncaught error: \(error)")
        context.close(promise: nil)
        context.fireErrorCaught(error)
    }
}
