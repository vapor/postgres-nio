import Logging
import NIO

extension PostgresConnection {
    public static func connect(
        to socketAddress: SocketAddress,
        tlsConfiguration: TLSConfiguration? = nil,
        serverHostname: String? = nil,
        on eventLoop: EventLoop
    ) -> EventLoopFuture<PostgresConnection> {
        let bootstrap = ClientBootstrap(group: eventLoop)
            .channelOption(ChannelOptions.socket(SocketOptionLevel(SOL_SOCKET), SO_REUSEADDR), value: 1)
        
        let logger = Logger(label: "codes.vapor.nio-postgres")
        return bootstrap.connect(to: socketAddress).flatMap { channel in
            return channel.pipeline.addHandlers([
                ByteToMessageHandler(PostgresMessageDecoder()),
                MessageToByteHandler(PostgresMessageEncoder()),
                PostgresRequestHandler(logger: logger),
                PostgresErrorHandler(logger: logger)
            ]).map {
                return PostgresConnection(channel: channel, logger: logger)
            }
        }.flatMap { conn in
            if let tlsConfiguration = tlsConfiguration {
                return conn.requestTLS(using: tlsConfiguration, serverHostname: serverHostname)
                    .map { conn }
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
        print("close 1")
        context.close(promise: nil)
        context.fireErrorCaught(error)
    }
}
