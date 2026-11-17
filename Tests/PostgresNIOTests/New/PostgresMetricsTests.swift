@testable import PostgresNIO
import Logging
import Metrics
import MetricsTestKit
import NIOCore
import Testing

@Suite struct PostgresMetricsTests {
    func makeMetrics(_ connection: PostgresConnection.InternalConfiguration.Connection, database: String? = "app") -> PostgresMetrics {
        let configuration = PostgresConnection.InternalConfiguration(
            connection: connection, username: "user", password: nil, database: database, tls: .disable, options: .init()
        )
        return PostgresMetrics(configuration: configuration, remoteAddress: nil)
    }

    @Test func noBackendSkipsTiming() {
        let metrics = self.makeMetrics(.unresolvedTCP(host: "db", port: 5432))
        withMetricsFactory(NOOPMetricsHandler.instance) {
            #expect(metrics.startTiming() == nil)
        }
    }

    @Test func successReportsBaseDimensionsWithoutDefaultPort() throws {
        let factory = TestMetrics()
        let metrics = self.makeMetrics(.unresolvedTCP(host: "db", port: 5432))
        withMetricsFactory(factory) {
            metrics.record(start: metrics.startTiming(), error: nil)
        }

        let timer = try factory.expectTimer("db.client.operation.duration", [
            ("db.system.name", "postgresql"), ("db.namespace", "app"), ("server.address", "db"),
        ])
        #expect(timer.values.count == 1)
    }

    @Test func failureAddsErrorTypeAndNonDefaultPort() throws {
        let factory = TestMetrics()
        let metrics = self.makeMetrics(.unresolvedTCP(host: "db", port: 6543), database: nil)
        withMetricsFactory(factory) {
            metrics.record(start: metrics.startTiming(), error: PSQLError.server(.init(fields: [.sqlState: "42P01"])))
            metrics.record(start: metrics.startTiming(), error: CancellationError())
        }

        let base = [("db.system.name", "postgresql"), ("server.address", "db"), ("server.port", "6543")]
        #expect(try factory.expectTimer("db.client.operation.duration", base + [("error.type", "42P01"), ("db.response.status_code", "42P01")]).values.count == 1)
        #expect(try factory.expectTimer("db.client.operation.duration", base + [("error.type", "_OTHER")]).values.count == 1)
    }

    @Test func eachQueryUsesTheFactoryOfItsTask() throws {
        let first = TestMetrics()
        let second = TestMetrics()
        let metrics = self.makeMetrics(.unresolvedUDS(path: "/tmp/.s.PGSQL.5432"))
        withMetricsFactory(first) {
            metrics.record(start: metrics.startTiming(), error: nil)
        }
        withMetricsFactory(second) {
            metrics.record(start: metrics.startTiming(), error: nil)
            metrics.record(start: metrics.startTiming(), error: nil)
        }

        let dimensions = [("db.system.name", "postgresql"), ("db.namespace", "app"), ("server.address", "/tmp/.s.PGSQL.5432")]
        #expect(try first.expectTimer("db.client.operation.duration", dimensions).values.count == 1)
        #expect(try second.expectTimer("db.client.operation.duration", dimensions).values.count == 2)
    }

    @Test func poolMetricsUseTheGivenFactory() throws {
        let factory = TestMetrics()
        let metrics = PostgresClientMetrics(logger: Logger(label: "test"), factory: factory, minimumConnections: 2, maximumConnections: 8)
        metrics.poolInstruments?.waitTime.recordNanoseconds(1)

        let pool = [("db.client.connection.pool.name", "postgresnio-connection-pool")]
        #expect(try factory.expectTimer("db.client.connection.wait_time", pool).values == [1])
        #expect(try factory.expectRecorder("db.client.connection.max", pool).values == [8])
        #expect(try factory.expectRecorder("db.client.connection.idle.max", pool).values == [8])
        #expect(try factory.expectRecorder("db.client.connection.idle.min", pool).values == [2])
    }

    @Test func poolMetricsAreDisabledWithoutABackend() {
        let metrics = PostgresClientMetrics(
            logger: Logger(label: "test"), factory: NOOPMetricsHandler.instance, minimumConnections: 0, maximumConnections: 1
        )
        #expect(metrics.poolInstruments == nil)
    }
}

@Suite struct PostgresMetricsErrorTypeTests {
    @Test func serverErrorReportsItsSQLState() {
        #expect(PSQLError.server(.init(fields: [.sqlState: "42P01"])).metricsErrorType == "42P01")
    }

    @Test func malformedSQLStateIsNotReported() {
        #expect(PSQLError.server(.init(fields: [.sqlState: "not a code"])).metricsErrorType == nil)
        #expect(PSQLError.server(.init(fields: [.sqlState: "42p01"])).metricsErrorType == nil)
    }

    @Test func cancelledQueryReportsCancelled() {
        #expect(PSQLError.queryCancelled.metricsErrorType == "cancelled")
        #expect(PSQLError.queryCancelled.metricsStatusCode == nil)
    }

    @Test func otherClientErrorsAreNotReported() {
        #expect(PSQLError.serverClosedConnection(underlying: nil).metricsErrorType == nil)
    }
}
