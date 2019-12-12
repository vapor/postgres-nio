import Logging
import NIO
import NIOTransportServices

extension PostgresConnection {
    public static func connect(
        to socketAddress: SocketAddress,
        tlsConfiguration: TLSConfiguration? = nil,
        serverHostname: String? = nil,
        logger: Logger = .init(label: "codes.vapor.postgres"),
        on eventLoop: EventLoop
    ) -> EventLoopFuture<PostgresConnection> {
        #if canImport(Network)
        if eventLoop is QoSEventLoop {
            return self.connectNIOTS(
                to: socketAddress,
                tlsConfiguration: tlsConfiguration,
                serverHostname: serverHostname,
                logger: logger,
                on: eventLoop
            )
        } else {
            return self.connectNIO(
                to: socketAddress,
                tlsConfiguration: tlsConfiguration,
                serverHostname: serverHostname,
                logger: logger,
                on: eventLoop
            )
        }
        #else
        return self.connectNIO(
            to: socketAddress,
            tlsConfiguration: tlsConfiguration,
            serverHostname: serverHostname,
            logger: logger,
            on: eventLoop
        )
        #endif
    }

    #if canImport(Network)
    private static func connectNIOTS(
        to socketAddress: SocketAddress,
        tlsConfiguration: TLSConfiguration? = nil,
        serverHostname: String? = nil,
        logger: Logger = .init(label: "codes.vapor.postgres"),
        on eventLoop: EventLoop
    ) -> EventLoopFuture<PostgresConnection> {
        var bootstrap = NIOTSConnectionBootstrap(group: eventLoop)
            .connectTimeout(.hours(1))
            .channelOption(ChannelOptions.socket(IPPROTO_TCP, TCP_NODELAY), value: 1)
            .channelInitializer { channel in
                channel.configurePostgres(logger: logger)
            }
            .connectTimeout(.seconds(10))
        if tlsConfiguration != nil {
            bootstrap = bootstrap.tlsOptions(.init())
        }
        return bootstrap.connect(to: socketAddress).map { channel in
            PostgresConnection(channel: channel, logger: logger)
        }
    }
    #endif
    
    private static func connectNIO(
        to socketAddress: SocketAddress,
        tlsConfiguration: TLSConfiguration? = nil,
        serverHostname: String? = nil,
        logger: Logger = .init(label: "codes.vapor.postgres"),
        on eventLoop: EventLoop
    ) -> EventLoopFuture<PostgresConnection> {
        let bootstrap = ClientBootstrap(group: eventLoop)
            .channelOption(
                ChannelOptions.socket(SocketOptionLevel(SOL_SOCKET), SO_REUSEADDR),
                value: 1
            )
            .connectTimeout(.seconds(10))
        return bootstrap.connect(to: socketAddress).flatMap { channel in
            channel.configurePostgres(logger: logger).map { _ in
                PostgresConnection(channel: channel, logger: logger)
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

extension Channel {
    func configurePostgres(logger: Logger) -> EventLoopFuture<Void> {
        self.pipeline.addHandlers([
            ByteToMessageHandler(PostgresMessageDecoder()),
            MessageToByteHandler(PostgresMessageEncoder()),
            PostgresRequestHandler(logger: logger),
            PostgresErrorHandler(logger: logger)
        ])
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
