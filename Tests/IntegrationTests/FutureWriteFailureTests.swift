import Atomics
import Logging
import NIOCore
import NIOPosix
import XCTest
@testable import PostgresNIO

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
final class FutureWriteFailureTests: XCTestCase {
    private let logger = Logger(label: "FutureWriteFailureTests") { _ in SwiftLogNoOpLogHandler() }

    func testFutureStatementAfterBackendTerminationFails() async throws {
        try await self.assertTerminatedTransactionFails(next: "SELECT 1")
    }

    func testFutureCommitAfterBackendTerminationFails() async throws {
        try await self.assertTerminatedTransactionFails(next: "COMMIT")
    }

    private func assertTerminatedTransactionFails(next query: PostgresQuery) async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        self.addTeardownBlock { try await group.shutdownGracefully() }
        let connection = try await PostgresConnection.test(on: group.next()).get()
        let control = try await PostgresConnection.test(on: group.next()).get()
        do {
            let begin: EventLoopFuture<PostgresQueryResult> = connection.query("BEGIN", logger: self.logger)
            _ = try await begin.get()
            let result: EventLoopFuture<PostgresQueryResult> = connection.query("SELECT pg_backend_pid()", logger: self.logger)
            let resultRows = try await result.get().rows
            let first = try XCTUnwrap(resultRows.first)
            let pid = try first.decode(Int32.self)
            let rows = try await control.query("SELECT pg_terminate_backend(\(pid))", logger: self.logger)
            var terminated = false
            for try await row in rows { terminated = try row.decode(Bool.self) }
            XCTAssertTrue(terminated)
            try await connection.closeFuture.get()
            let started = NIODeadline.now()
            do {
                let future = connection.query(query, logger: self.logger) { _ in
                    XCTFail("Terminated transaction delivered a row")
                }
                _ = try await future.get()
                XCTFail("Expected the terminated transaction to fail")
            } catch let error as PSQLError {
                XCTAssertEqual(error.code, .clientClosedConnection)
            } catch {
                XCTFail("Expected a PostgreSQL closed connection error")
            }
            XCTAssertLessThan(NIODeadline.now() - started, .seconds(2))
            try await control.close()
        } catch {
            try? await connection.close()
            try? await control.close()
            throw error
        }
    }

    func testPoolQueryWriteFailureReleasesLeaseAndReconnects() async throws {
        try await self.assertPoolWriteFailureReleasesLease(prepared: false)
    }

    func testPoolPreparedWriteFailureReleasesLeaseAndReconnects() async throws {
        try await self.assertPoolWriteFailureReleasesLease(prepared: true)
    }

    private func assertPoolWriteFailureReleasesLease(prepared: Bool) async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        self.addTeardownBlock { try await group.shutdownGracefully() }
        var config = PostgresClient.Configuration.makeTestConfiguration()
        config.options.minimumConnections = 0
        config.options.maximumConnections = 1
        config.options.keepAliveBehavior = nil
        let client = PostgresClient(configuration: config, eventLoopGroup: group)
        try await withThrowingTaskGroup(of: Void.self) { tasks in
            tasks.addTask { await client.run() }
            do {
                let reject = RejectNextTaskWrite()
                let closed = try await client.withConnection { connection in
                    try await connection.channel.pipeline.addHandler(reject)
                    return connection.closeFuture
                }
                do {
                    if prepared {
                        let rows = try await client.execute(PoolPreparedStatement())
                        for try await _ in rows { XCTFail("Rejected statement delivered a row") }
                    } else {
                        let rows = try await client.query("SELECT 1")
                        for try await _ in rows { XCTFail("Rejected query delivered a row") }
                    }
                    XCTFail("Expected the rejected write to fail")
                } catch let error as PSQLError {
                    XCTAssertEqual(error.code, .clientClosedConnection)
                    XCTAssertEqual(error.underlying as? ChannelError, .ioOnClosedChannel)
                } catch {
                    XCTFail("Expected a PostgreSQL write failure")
                }
                XCTAssertTrue(reject.rejected.load(ordering: .relaxed))
                try await closed.get()
                let rows = try await client.query("SELECT 42")
                var values: [Int] = []
                for try await row in rows { values.append(try row.decode(Int.self)) }
                XCTAssertEqual(values, [42])
                tasks.cancelAll()
            } catch {
                tasks.cancelAll()
                throw error
            }
        }
    }

    private struct PoolPreparedStatement: PostgresPreparedStatement {
        static let sql = "SELECT 1"
        typealias Row = Int
        func makeBindings() -> PostgresBindings { .init() }
        func decodeRow(_ row: PostgresRow) throws -> Int { try row.decode(Int.self) }
    }

    private final class RejectNextTaskWrite: ChannelOutboundHandler, Sendable {
        typealias OutboundIn = HandlerTask
        typealias OutboundOut = HandlerTask
        let rejected = ManagedAtomic(false)

        func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
            if !self.rejected.exchange(true, ordering: .relaxed) {
                context.close(promise: nil)
                promise?.fail(ChannelError.ioOnClosedChannel)
            } else {
                context.write(data, promise: promise)
            }
        }
    }
}
