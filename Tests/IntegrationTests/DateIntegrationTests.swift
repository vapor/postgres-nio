import Foundation
import Logging
import NIOCore
import NIOPosix
import PostgresNIO
import Testing

@Suite
struct DateIntegrationTests {
    @Test(
        .bug("https://github.com/vapor/postgres-nio/issues/632"),
        arguments: [
            Double.greatestFiniteMagnitude,
            -.greatestFiniteMagnitude,
            .infinity,
            -.infinity,
            .nan,
            // Int64.min, which would become -infinity
            -9_223_372_036_854.775390625,
        ]
    )
    func outOfRangeDatesAreRejectedByTheServer(secondsSincePSQLDateStart: Double) async throws {
        let date = Date(timeInterval: secondsSincePSQLDateStart, since: Date(timeIntervalSince1970: 946_684_800))

        try await withTestConnection(on: MultiThreadedEventLoopGroup.singleton.any()) { connection in
            do {
                let rows = try await connection.query("SELECT \(date)::timestamptz", logger: .psqlTest)
                for try await row in rows {
                    Issue.record("Expected the server to reject the date, got \(row)")
                }
                Issue.record("Expected the server to reject the date")
            } catch let error as PSQLError {
                #expect(error.code == .server)
                // PostgresError.Code.datetimeFieldOverflow
                #expect(error.serverInfo?[.sqlState] == "22008")
                #expect(error.serverInfo?[.message] == "timestamp out of range")
            }
        }
    }
}
