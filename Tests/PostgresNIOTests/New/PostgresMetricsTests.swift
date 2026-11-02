@testable import PostgresNIO
import Logging
import MetricsTestKit
import NIOCore
import Testing

@Suite struct PostgresMetricsTests {
    func makeMetrics(
        _ connection: PostgresConnection.InternalConfiguration.Connection,
        database: String? = "app",
        factory: TestMetrics
    ) -> PostgresMetrics? {
        var options = PostgresConnection.Configuration.Options()
        options.metrics = .init(factory: factory)
        let configuration = PostgresConnection.InternalConfiguration(
            connection: connection, username: "user", password: nil, database: database, tls: .disable, options: options
        )
        return PostgresMetrics(configuration: configuration, remoteAddress: nil)
    }

    @Test func noFactoryDisablesMetrics() {
        var options = PostgresConnection.Configuration.Options()
        options.metrics = .init(factory: nil)
        let configuration = PostgresConnection.InternalConfiguration(
            connection: .unresolvedTCP(host: "db", port: 5432), username: "user", password: nil, database: nil, tls: .disable, options: options
        )
        #expect(PostgresMetrics(configuration: configuration, remoteAddress: nil) == nil)
    }

    @Test func successReportsBaseDimensionsWithoutDefaultPort() throws {
        let factory = TestMetrics()
        let metrics = try #require(self.makeMetrics(.unresolvedTCP(host: "db", port: 5432), factory: factory))
        metrics.record(start: metrics.startTiming(), error: nil)

        let timer = try factory.expectTimer("db.client.operation.duration", [
            ("db.system.name", "postgresql"), ("db.namespace", "app"), ("server.address", "db"),
        ])
        #expect(timer.values.count == 1)
    }

    @Test func failureAddsErrorTypeAndNonDefaultPort() throws {
        let factory = TestMetrics()
        let metrics = try #require(self.makeMetrics(.unresolvedTCP(host: "db", port: 6543), database: nil, factory: factory))
        metrics.record(start: metrics.startTiming(), error: PSQLError.server(.init(fields: [.sqlState: "42P01"])))
        metrics.record(start: metrics.startTiming(), error: CancellationError())

        let base = [("db.system.name", "postgresql"), ("server.address", "db"), ("server.port", "6543")]
        #expect(try factory.expectTimer("db.client.operation.duration", base + [("error.type", "42P01"), ("db.response.status_code", "42P01")]).values.count == 1)
        #expect(try factory.expectTimer("db.client.operation.duration", base + [("error.type", "_OTHER")]).values.count == 1)
    }

    @Test func queryLabelsAndDimensionsCanBeOverridden() throws {
        let factory = TestMetrics()
        var options = PostgresConnection.Configuration.Options()
        options.metrics = .init(factory: factory)
        options.metrics.labels.operationDuration = "query.duration"
        options.metrics.dimensions.databaseSystemKey = "system"
        options.metrics.dimensions.errorTypeKey = "failure"
        options.metrics.dimensions.otherErrorTypeValue = "unknown"
        let configuration = PostgresConnection.InternalConfiguration(
            connection: .unresolvedUDS(path: "/tmp/pg"), username: "user", password: nil, database: nil, tls: .disable, options: options
        )
        let metrics = try #require(PostgresMetrics(configuration: configuration, remoteAddress: nil))
        metrics.record(start: metrics.startTiming(), error: CancellationError())

        let timer = try factory.expectTimer("query.duration", [
            ("system", "postgresql"), ("server.address", "/tmp/pg"), ("failure", "unknown"),
        ])
        #expect(timer.values.count == 1)
    }

    @Test func poolLabelsAndDimensionsCanBeOverridden() throws {
        let factory = TestMetrics()
        var configuration = PostgresMetricsConfiguration(factory: factory)
        configuration.labels.waitTime = "pool.wait"
        configuration.dimensions.connectionPoolNameValue = "replica"
        let metrics = PostgresClientMetrics(
            logger: Logger(label: "test"), configuration: configuration, minimumConnections: 2, maximumConnections: 8
        )
        metrics.poolInstruments?.waitTime.recordNanoseconds(1)

        let pool = [("db.client.connection.pool.name", "replica")]
        let timer = try factory.expectTimer("pool.wait", pool)
        #expect(timer.values == [1])
        #expect(try factory.expectRecorder("db.client.connection.max", pool).values == [8])
        #expect(try factory.expectRecorder("db.client.connection.idle.max", pool).values == [8])
        #expect(try factory.expectRecorder("db.client.connection.idle.min", pool).values == [2])
    }

    @Test func timersAreReusedPerOutcome() throws {
        let factory = TestMetrics()
        let metrics = try #require(self.makeMetrics(.unresolvedUDS(path: "/tmp/.s.PGSQL.5432"), factory: factory))
        metrics.record(start: metrics.startTiming(), error: nil)
        metrics.record(start: metrics.startTiming(), error: nil)

        let timer = try factory.expectTimer("db.client.operation.duration", [
            ("db.system.name", "postgresql"), ("db.namespace", "app"), ("server.address", "/tmp/.s.PGSQL.5432"),
        ])
        #expect(timer.values.count == 2)
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
