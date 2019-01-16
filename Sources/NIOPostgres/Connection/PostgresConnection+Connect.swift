import NIO

extension PostgresConnection {
    public static func connect(to socketAddress: SocketAddress, on eventLoop: EventLoop) -> EventLoopFuture<PostgresConnection> {
        let bootstrap = ClientBootstrap(group: eventLoop)
            .channelOption(ChannelOptions.socket(SocketOptionLevel(SOL_SOCKET), SO_REUSEADDR), value: 1)
        
        return bootstrap.connect(to: socketAddress).then { channel in
            return channel.pipeline.addHandlers([
                ByteToMessageHandler(PostgresMessageDecoder()),
                PostgresMessageEncoder()
            ], first: false).map {
                return .init(channel: channel)
            }
        }
    }
}
