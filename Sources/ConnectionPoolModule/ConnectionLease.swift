//
//  ConnectionLease.swift
//  postgres-nio
//
//  Created by Fabian Fett on 05.05.25.
//

public struct ConnectionLease<Connection: PooledConnection>: Sendable {

    public var connection: Connection
    
    @usableFromInline
    let _release: @Sendable () -> ()

    @inlinable
    public init(connection: Connection, release: @escaping @Sendable () -> Void) {
        self.connection = connection
        self._release = release
    }

    @inlinable
    public func release() {
        self._release()
    }
}
