//
//  ConnectionLease.swift
//  postgres-nio
//
//  Created by Fabian Fett on 05.05.25.
//

struct ConnectionLease<Connection: PooledConnection> {

    var connection: Connection
    var _release: () -> ()

    init(connection: Connection, release: @escaping () -> Void) {
        self.connection = connection
        self._release = release
    }

    func release() {
        self._release()
    }

}
