//
//  PostgresStructuredFactory.swift
//  postgres-nio
//
//  Created by Fabian Fett on 23.02.26.
//

import Atomics
import _ConnectionPoolModule

struct PostgresStructuredFactory: StructuredConnectionProvider {

    let connectionID = ManagedAtomic<Int>(0)

    let logger: Logger

    typealias Connection = PostgresConnection
    typealias ConnectionID = Int

    // onConnected -> better name
    // make it clear what we expect from ConnectionIDGenerator –>
    // break events pool ref cycle at the end of onConnected
    //     -> provideConnection?

    func withConnection(
        onConnected: nonisolated(nonsending) (consuming PostgresConnection, Int, (_ConnectionPoolModule.EventsCallbacks) -> Void) async -> Void
    ) async throws {
        let connection = try await PostgresConnection.connect(
            connectionID: self.connectionID.loadThenWrappingIncrement(ordering: .relaxed),
            configuration: .init(.init(host: "localhost", username: "postgres", password: "foo", database: "foo", tls: .disable)),
            logger: self.logger,
            on: NIOSingletons.posixEventLoopGroup.any()
        ).get()

        await withTaskGroup { taskGroup in
            await onConnected(connection, 1) { events in

                taskGroup.addTask {

                }

                connection.closeFuture.whenComplete { _ in
                    events.connectionClosed(nil)
                }
            }

            taskGroup.cancelAll()
        }


        try await connection.close()
    }
}
