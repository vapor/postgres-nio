//
//  PostgresStructuredFactory.swift
//  postgres-nio
//
//  Created by Fabian Fett on 23.02.26.
//

import _ConnectionPoolModule

struct PostgresStructuredFactory: StructuredConnectionProvider {
    let logger: Logger

    typealias Connection = PostgresConnection
    typealias ConnectionID = Int

    func withConnection(_ id: Int, onConnected: nonisolated(nonsending) (consuming PostgresConnection, Int, (EventsCallbacks<Int>) -> Void) async -> Void) async throws {
        let connection = try await PostgresConnection.connect(
            connectionID: id,
            configuration: .init(.init(host: "localhost", username: "postgres", password: "foo", database: "foo", tls: .disable)),
            logger: self.logger,
            on: NIOSingletons.posixEventLoopGroup.any()
        ).get()

        await withTaskGroup(of: Void.self) { taskGroup in
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
