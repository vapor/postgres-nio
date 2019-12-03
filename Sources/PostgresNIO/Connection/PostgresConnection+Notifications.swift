import NIO

public final class PostgresNotificationHandlerMap {
    private var handlers: [String: (PostgresMessage.NotificationResponse) -> Void] = [:]

    internal init() {}

    public subscript(channel name: String) -> ((PostgresMessage.NotificationResponse) -> Void)? {
        get { handlers[name] }
        set { handlers[name] = newValue }
    }
}

/// NIO handler to filter out NotificationResponse messages, and divert them to an appropriate entry in the PostgresNotificationHandlerMap.
final class PostgresNotificationHandler: ChannelDuplexHandler {
    typealias InboundIn = PostgresMessage
    typealias InboundOut = PostgresMessage
    typealias OutboundIn = PostgresMessage
    typealias OutboundOut = PostgresMessage

    private let handlerMap: PostgresNotificationHandlerMap

    init(handlerMap: PostgresNotificationHandlerMap) {
        self.handlerMap = handlerMap
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        var request = unwrapInboundIn(data)
        switch request.identifier {
        case .notificationResponse:
            do {
                let notification = try PostgresMessage.NotificationResponse.parse(from: &request.data)
                if let handler = handlerMap[channel: notification.channel] {
                    handler(notification)
                }
            } catch let error {
                errorCaught(context: context, error: error)
            }
            // We absorb the NotificationResponse message, and do not send it to the PostgresRequestHandler.
        default:
            // All other messages are forwarded on.
            context.fireChannelRead(wrapInboundOut(request))
        }
    }
}
