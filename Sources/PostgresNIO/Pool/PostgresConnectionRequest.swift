//
//  PostgresConnectionRequest.swift
//  postgres-nio
//
//  Created by Fabian Fett on 15.05.25.
//

import _ConnectionPoolModule

struct PostgresConnectionRequest: ConnectionRequestProtocol {

    static let idGenerator = ConnectionIDGenerator()

    private enum `Type` {
        case connection(CheckedContinuation<ConnectionLease<PostgresConnection>, any Error>)
        case query(PostgresQuery, Logger, CheckedContinuation<PostgresRowSequence, any Error>)
    }

    typealias ID = Int

    var id: ID
    private var type: `Type`

    init(
        id: Int,
        continuation: CheckedContinuation<ConnectionLease<PostgresConnection>, any Error>
    ) {
        self.id = id
        self.type = .connection(continuation)
    }

    init(
        id: Int,
        query: PostgresQuery,
        continuation: CheckedContinuation<PostgresRowSequence, any Error>,
        logger: Logger
    ) {
        self.id = id
        self.type = .query(query, logger, continuation)
    }

    public func complete(with result: Result<ConnectionLease<PostgresConnection>, ConnectionPoolError>) {
        switch self.type {
        case .connection(let checkedContinuation):
            checkedContinuation.resume(with: result)

        case .query(let query, var logger, let checkedContinuation):
            switch result {
            case .success(let lease):
                logger[postgresMetadataKey: .connectionID] = "\(lease.connection.id)"

                let promise = lease.connection.channel.eventLoop.makePromise(of: PSQLRowStream.self)
                let context = ExtendedQueryContext(
                    query: query,
                    logger: logger,
                    promise: promise
                )

                lease.connection.channel.write(HandlerTask.extendedQuery(context), promise: nil)
                promise.futureResult.whenFailure { error in
                    lease.release()
                    checkedContinuation.resume(throwing: error)
                }

                promise.futureResult.whenSuccess { rowSequence in
                    let asyncSequence = rowSequence.asyncSequence {
                        lease.release()
                    }
                    checkedContinuation.resume(returning: asyncSequence)
                }

            case .failure(let error):
                checkedContinuation.resume(throwing: error)
            }
        }

    }
}
