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
            Date(timeIntervalSince1970: .greatestFiniteMagnitude),
            Date(timeIntervalSince1970: -.greatestFiniteMagnitude),
            Date(timeIntervalSince1970: .infinity),
            Date(timeIntervalSince1970: -.infinity),
            Date(timeIntervalSince1970: .nan),
            // Int64.min, which would become -infinity
            Date(timeInterval: -9_223_372_036_854.775390625, since: Date(timeIntervalSince1970: 946_684_800)),
        ]
    )
    func outOfRangeDatesAreRejectedByTheServer(date: Date) async throws {
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
