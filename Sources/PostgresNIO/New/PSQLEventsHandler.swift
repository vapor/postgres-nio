//
//  File.swift
//  
//
//  Created by Fabian Fett on 19.01.21.
//

import NIOTLS

enum PSQLOutgoingEvent {    
    /// the event we send down the channel to inform the `PSQLChannelHandler` to authenticate
    ///
    /// this shall be removed with the next breaking change and always supplied with `PSQLConnection.Configuration`
    case authenticate(AuthContext)
}

enum PSQLEvent {
    
    /// the event that is used to inform upstream handlers that `PSQLChannelHandler` has established a connection
    case readyForStartup
    
    /// the event that is used to inform upstream handlers that `PSQLChannelHandler` is currently idle
    case readyForQuery
}


final class PSQLEventsHandler: ChannelInboundHandler {
    typealias InboundIn = Never
    
    let logger: Logger
    var readyForStartupFuture: EventLoopFuture<Void> {
        self.readyForStartupPromise.futureResult
    }
    var authenticateFuture: EventLoopFuture<Void> {
        self.authenticatePromise.futureResult
    }
    

    private enum State {
        case initialized
        case connected
        case readyForStartup
        case authenticated
    }
    
    private var readyForStartupPromise: EventLoopPromise<Void>
    private var authenticatePromise: EventLoopPromise<Void>
    private var state: State = .initialized
    
    init(logger: Logger, eventLoop: EventLoop) {
        self.logger = logger
        
        self.readyForStartupPromise = eventLoop.makePromise(of: Void.self)
        self.authenticatePromise = eventLoop.makePromise(of: Void.self)
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
                preconditionFailure("how can that happen?")
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
        precondition(!context.channel.isActive)
    }
    
    func channelActive(context: ChannelHandlerContext) {
        guard case .initialized = self.state else {
            preconditionFailure("Invalid state")
        }
        
        self.state = .connected
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
    }
    
    func handlerRemoved(context: ChannelHandlerContext) {
        let error = PSQLError.sslUnsupported
        switch self.state {
        case .connected:
            self.readyForStartupPromise.fail(error)
            self.authenticatePromise.fail(error)
        case .initialized:
            self.readyForStartupPromise.fail(error)
            self.authenticatePromise.fail(error)
        case .readyForStartup:
            self.authenticatePromise.fail(error)
        case .authenticated:
            break
        }
    }
}

