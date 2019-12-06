import NIO

public final class PostgresListenContext {
    var stopper: (() -> Void)?

    public func stop() {
        stopper?()
        stopper = nil
    }
}

extension PostgresConnection {
    @discardableResult
    public func listen(channel: String, handler notificationHandler: @escaping (PostgresMessage.NotificationResponse, PostgresListenContext) -> Void) -> PostgresListenContext {
        let listenContext = PostgresListenContext()
        let channelHandler = PostgresNotificationHandler(notificationHandler: notificationHandler, listenContext: listenContext)
        let pipeline = self.channel.pipeline
        _ = pipeline.addHandler(channelHandler, name: nil, position: .before(requestHandler))
        listenContext.stopper = { [pipeline, unowned channelHandler] in
            _ = pipeline.removeHandler(channelHandler)
        }
        return listenContext
    }
}

final class PostgresNotificationHandler: ChannelInboundHandler, RemovableChannelHandler {
    typealias InboundIn = PostgresMessage
    typealias InboundOut = PostgresMessage

    let notificationHandler: (PostgresMessage.NotificationResponse, PostgresListenContext) -> Void
    let listenContext: PostgresListenContext

    init(notificationHandler: @escaping (PostgresMessage.NotificationResponse, PostgresListenContext) -> Void, listenContext: PostgresListenContext) {
        self.notificationHandler = notificationHandler
        self.listenContext = listenContext
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let request = unwrapInboundIn(data)
        // Slightly complicated: We need to dispatch downstream _before_ we handle the notification ourselves, because the notification handler could try to stop the listen, which removes ourselves from the pipeline and makes fireChannelRead not work any more.
        context.fireChannelRead(wrapInboundOut(request))
        if request.identifier == .notificationResponse {
            do {
                var data = request.data
                let notification = try PostgresMessage.NotificationResponse.parse(from: &data)
                notificationHandler(notification, listenContext)
            } catch let error {
                errorCaught(context: context, error: error)
            }
        }
    }
}
