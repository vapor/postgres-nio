import Testing
import Tracing
@testable import PostgresNIO

@Suite struct PostgresTracingTests {
    @Test func serverErrorIsClassifiedByItsSQLState() {
        let error = PSQLError.server(.init(fields: [.sqlState: "23505"]))
        #expect(error.tracingErrorType == "23505")
        #expect(error.tracingStatusCode == "23505")
    }

    @Test(arguments: ["2350", "235050", "23a05", "2350!", ""])
    func malformedSQLStateIsDropped(sqlState: String) {
        let error = PSQLError.server(.init(fields: [.sqlState: sqlState]))
        #expect(error.tracingErrorType == nil)
        #expect(error.tracingStatusCode == nil)
    }

    @Test func cancelledQueryIsCancelled() {
        #expect(PSQLError.queryCancelled.tracingErrorType == "cancelled")
        #expect(PSQLError.queryCancelled.tracingStatusCode == nil)
    }

    @Test func noOpTracerDisablesTracing() {
        var configuration = PostgresConnection.Configuration(
            host: "localhost", username: "test", password: nil, database: nil, tls: .disable
        )
        configuration.options.tracing.tracer = NoOpTracer()
        #expect(PostgresTracing(configuration: .init(configuration), remoteAddress: nil) == nil)
    }

    @Test func clientErrorsHaveNoCode() {
        #expect(PSQLError.serverClosedConnection(underlying: nil).tracingErrorType == nil)
        #expect(PSQLError.serverClosedConnection(underlying: nil).tracingStatusCode == nil)
    }
}
