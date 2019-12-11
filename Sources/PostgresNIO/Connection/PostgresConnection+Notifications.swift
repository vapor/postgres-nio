import NIO
import Logging

/// Context for receiving NotificationResponse messages on a connection, used for PostgreSQL's `LISTEN`/`NOTIFY` support.
public final class PostgresListenContext {
    var stopper: (() -> Void)?

    /// Detach this listener so it no longer receives notifications. Other listeners, including those for the same channel, are unaffected. `UNLISTEN` is not sent; you are responsible for issuing an `UNLISTEN` query yourself if it is appropriate for your application.
    public func stop() {
        stopper?()
        stopper = nil
    }
}

extension PostgresConnection {
    @discardableResult
    public func addListener(channel: String, handler notificationHandler: @escaping (PostgresListenContext, PostgresMessage.NotificationResponse) -> Void) -> PostgresListenContext {
        let listenContext = PostgresListenContext()
        let channelHandler = PostgresNotificationHandler(logger: self.logger, channel: channel, notificationHandler: notificationHandler, listenContext: listenContext)
        let pipeline = self.channel.pipeline
        _ = pipeline.addHandler(channelHandler, name: nil, position: .last)
        listenContext.stopper = { [pipeline, unowned channelHandler] in
            _ = pipeline.removeHandler(channelHandler)
        }
        return listenContext
    }
}

final class PostgresNotificationHandler: ChannelInboundHandler, RemovableChannelHandler {
    typealias InboundIn = PostgresMessage
    typealias InboundOut = PostgresMessage

    let logger: Logger
    let channel: String
    let notificationHandler: (PostgresListenContext, PostgresMessage.NotificationResponse) -> Void
    let listenContext: PostgresListenContext

    init(logger: Logger, channel: String, notificationHandler: @escaping (PostgresListenContext, PostgresMessage.NotificationResponse) -> Void, listenContext: PostgresListenContext) {
        self.logger = logger
        self.channel = channel
        self.notificationHandler = notificationHandler
        self.listenContext = listenContext
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let request = self.unwrapInboundIn(data)
        // Slightly complicated: We need to dispatch downstream _before_ we handle the notification ourselves, because the notification handler could try to stop the listen, which removes ourselves from the pipeline and makes fireChannelRead not work any more.
        context.fireChannelRead(self.wrapInboundOut(request))
        if request.identifier == .notificationResponse {
            do {
                var data = request.data
                let notification = try PostgresMessage.NotificationResponse.parse(from: &data)
                if notification.channel == channel {
                    self.notificationHandler(self.listenContext, notification)
                }
            } catch let error {
                self.logger.error("\(error)")
            }
        }
    }
}
