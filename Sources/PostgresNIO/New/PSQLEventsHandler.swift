import NIOCore
import NIOTLS
import Logging

enum PSQLOutgoingEvent {    
    /// the event we send down the channel to inform the ``PostgresChannelHandler`` to authenticate
    ///
    /// this shall be removed with the next breaking change and always supplied with `PSQLConnection.Configuration`
    case authenticate(AuthContext)

    case gracefulShutdown
}

enum PSQLEvent {
    
    /// the event that is used to inform upstream handlers that ``PostgresChannelHandler`` has established a connection
    case readyForStartup
    
    /// the event that is used to inform upstream handlers that ``PostgresChannelHandler`` is currently idle
    case readyForQuery
}


final class PSQLEventsHandler: ChannelInboundHandler {
    typealias InboundIn = Never
    
    let logger: Logger
    var readyForStartupFuture: EventLoopFuture<Void>! {
        self.readyForStartupPromise!.futureResult
    }
    var authenticateFuture: EventLoopFuture<Void>! {
        self.authenticatePromise!.futureResult
    }
    

    private enum State {
        case initialized
        case connected
        case readyForStartup
        case authenticated
    }
    
    private var readyForStartupPromise: EventLoopPromise<Void>!
    private var authenticatePromise: EventLoopPromise<Void>!
    private var state: State = .initialized
    
    init(logger: Logger) {
        self.logger = logger
    }
    
    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        switch event {
        case PSQLEvent.readyForStartup:
            guard case .connected = self.state else {
                preconditionFailure()
            }
            self.state = .readyForStartup
            self.readyForStartupPromise.succeed(Void())
        case PSQLEvent.readyForQuery:
            switch self.state {
            case .initialized, .connected:
                preconditionFailure("Expected to get a `readyForStartUp` before we get a `readyForQuery` event")
            case .readyForStartup:
                // for the first time, we are ready to query, this means startup/auth was
                // successful
                self.state = .authenticated
                self.authenticatePromise.succeed(Void())
            case .authenticated:
                break
            }
        case TLSUserEvent.shutdownCompleted:
            break
        default:
            preconditionFailure()
        }
    }
    
    func handlerAdded(context: ChannelHandlerContext) {
        self.readyForStartupPromise = context.eventLoop.makePromise(of: Void.self)
        self.authenticatePromise = context.eventLoop.makePromise(of: Void.self)

        if context.channel.isActive, case .initialized = self.state {
            self.state = .connected
        }
    }
    
    func channelActive(context: ChannelHandlerContext) {
        if case .initialized = self.state {
            self.state = .connected
        }
        context.fireChannelActive()
    }
    
    func errorCaught(context: ChannelHandlerContext, error: Error) {
        switch self.state {
        case .initialized:
            preconditionFailure("Unexpected message for state")
        case .connected:
            self.readyForStartupPromise.fail(error)
            self.authenticatePromise.fail(error)
        case .readyForStartup:
            self.authenticatePromise.fail(error)
        case .authenticated:
            break
        }
        
        context.fireErrorCaught(error)
    }
    
    func handlerRemoved(context: ChannelHandlerContext) {
        struct HandlerRemovedConnectionError: Error {}
        
        if case .initialized = self.state {
            self.readyForStartupPromise.fail(HandlerRemovedConnectionError())
            self.authenticatePromise.fail(HandlerRemovedConnectionError())
        }
    }
}
