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
        
        return bootstrap.connect(to: socketAddress).flatMap { channel in
            return channel.pipeline.addHandlers([
                ByteToMessageHandler(PostgresMessageDecoder()),
                MessageToByteHandler(PostgresMessageEncoder()),
                PostgresConnectionHandler(),
            ]).map {
                return PostgresConnection(channel: channel)
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
