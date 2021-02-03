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
    /// Add a handler for NotificationResponse messages on a certain channel. This is used in conjunction with PostgreSQL's `LISTEN`/`NOTIFY` support: to listen on a channel, you add a listener using this method to handle the NotificationResponse messages, then issue a `LISTEN` query to instruct PostgreSQL to begin sending NotificationResponse messages.
    @discardableResult
    public func addListener(channel: String, handler notificationHandler: @escaping (PostgresListenContext, PostgresMessage.NotificationResponse) -> Void) -> PostgresListenContext {
        
        let listenContext = PostgresListenContext()
        
        self.underlying.channel.pipeline.handler(type: PSQLChannelHandler.self).whenSuccess { handler in
            if self.notificationListeners[channel] != nil {
                self.notificationListeners[channel]!.append((listenContext, notificationHandler))
            }
            else {
                self.notificationListeners[channel] = [(listenContext, notificationHandler)]
            }
        }
        
        listenContext.stopper = { [weak self, weak listenContext] in
            // self is weak, since the connection can long be gone, when the listeners stop is
            // triggered. listenContext must be weak to prevent a retain cycle
            
            self?.underlying.channel.eventLoop.execute {
                guard let self = self else {
                    // the connection is already gone
                    return
                }
                
                guard var listeners = self.notificationListeners[channel] else {
                    // we don't have the listeners for this topic ¯\_(ツ)_/¯
                    return
                }
                
                guard let index = listeners.firstIndex(where: { $0.0 === listenContext }) else {
                    return
                }
                
                listeners.remove(at: index)
                if listeners.count == 0 {
                    self.notificationListeners.removeValue(forKey: channel)
                } else {
                    self.notificationListeners[channel] = listeners
                }
            }
        }
        
        return listenContext
    }
}

extension PostgresConnection: PSQLChannelHandlerNotificationDelegate {
    func notificationReceived(_ notification: PSQLBackendMessage.NotificationResponse) {
        self.underlying.eventLoop.assertInEventLoop()
        
        guard let listeners = self.notificationListeners[notification.channel] else {
            return
        }
        
        let postgresNotification = PostgresMessage.NotificationResponse(
            backendPID: notification.backendPID,
            channel: notification.channel,
            payload: notification.payload)
        
        listeners.forEach { (listenContext, handler) in
            handler(listenContext, postgresNotification)
        }
    }
}
