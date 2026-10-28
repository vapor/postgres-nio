@_spi(ConnectionPool) import PostgresNIO
import Testing
import NIOPosix
import NIOSSL
import Logging
import Atomics
import Foundation

@Suite(.serialized)
struct PostgresClientTests {

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func getConnection() async throws {
        var mlogger = Logger(label: "test")
        mlogger.logLevel = .debug
        let logger = mlogger

        let clientConfig = PostgresClient.Configuration.makeTestConfiguration()
        let client = PostgresClient(
            configuration: clientConfig, eventLoopGroup: MultiThreadedEventLoopGroup.singleton, backgroundLogger: logger
        )

        try await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                await client.run()
            }

            let iterations = 1000

            for _ in 0..<iterations {
                taskGroup.addTask {
                    try await client.withConnection() { connection in
                        _ = try await connection.query("SELECT 1", logger: logger)
                    }
                }
            }

            for _ in 0..<iterations {
                try await taskGroup.next()
            }

            taskGroup.cancelAll()
        }
    }
    
    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func transaction() async throws {
        var mlogger = Logger(label: "test")
        mlogger.logLevel = .debug
        let logger = mlogger
        
        let tableName = "test_client_transactions"
        
        let clientConfig = PostgresClient.Configuration.makeTestConfiguration()
        let client = PostgresClient(
            configuration: clientConfig, eventLoopGroup: MultiThreadedEventLoopGroup.singleton, backgroundLogger: logger
        )
        
        try await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                await client.run()
            }
            
            try await client.query(
                """
                CREATE TABLE IF NOT EXISTS "\(unescaped: tableName)" (
                    id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                    uuid UUID NOT NULL
                );
                """,
                logger: logger
            )
            
            let iterations = 1000
            
            for _ in 0..<iterations {
                taskGroup.addTask {
                    let _ = try await client.withTransaction(logger: logger) { transaction in
                        try await transaction.query(
                        """
                        INSERT INTO "\(unescaped: tableName)" (uuid) VALUES (\(UUID()));
                        """,
                        logger: logger
                        )
                    }
                }
            }
            
            for _ in 0..<iterations {
                try await taskGroup.next()
            }
            
            let rows = try await client.query(#"SELECT COUNT(1)::INT AS table_size FROM "\#(unescaped: tableName)";"#, logger: logger).decode(Int.self)
            for try await (count) in rows {
                #expect(count == iterations)
            }
            
            /// Test roll back
            taskGroup.addTask {
                let error = try await #require(throws: PostgresTransactionError.self) {
                    let _ = try await client.withTransaction(logger: logger) { transaction in
                        /// insert valid data
                        try await transaction.query(
                            """
                            INSERT INTO "\(unescaped: tableName)" (uuid) VALUES (\(UUID()));
                            """,
                            logger: logger
                        )
                        
                        /// insert invalid data
                        try await transaction.query(
                            """
                            INSERT INTO "\(unescaped: tableName)" (uuid) VALUES (\(iterations));
                            """,
                            logger: logger
                        )
                    }
                }

                #expect((error.closureError as? PSQLError)?.code == .server)
                #expect((error.closureError as? PSQLError)?.serverInfo?[.severity] == "ERROR")
            }
            try await taskGroup.next()
            
            let row = try await client.query(#"SELECT COUNT(1)::INT AS table_size FROM "\#(unescaped: tableName)";"#, logger: logger).decode(Int.self)
            
            for try await (count) in row {
                #expect(count == iterations)
            }
            
            try await client.query(
                """
                DROP TABLE "\(unescaped: tableName)";
                """,
                logger: logger
            )
            
            taskGroup.cancelAll()
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func applicationNameIsForwardedCorrectly() async throws {
        var mlogger = Logger(label: "test")
        mlogger.logLevel = .debug
        let logger = mlogger

        var clientConfig = PostgresClient.Configuration.makeTestConfiguration()
        let applicationName = "postgres_nio_test_run"
        clientConfig.options.additionalStartupParameters = [("application_name", applicationName)]
        let client = PostgresClient(
            configuration: clientConfig, eventLoopGroup: MultiThreadedEventLoopGroup.singleton, backgroundLogger: logger
        )

        try await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                await client.run()
            }

            let rows = try await client.query("select * from pg_stat_activity;");
            var applicationNameFound = 0
            for try await row in rows {
                let randomAccessRow = row.makeRandomAccess()
                if try randomAccessRow["application_name"].decode(String?.self) == applicationName {
                    applicationNameFound += 1
                }
            }

            #expect(applicationNameFound >= 1)

            taskGroup.cancelAll()
        }
    }


    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func queryDirectly() async throws {
        var mlogger = Logger(label: "test")
        mlogger.logLevel = .debug
        let logger = mlogger

        let clientConfig = PostgresClient.Configuration.makeTestConfiguration()
        let client = PostgresClient(
            configuration: clientConfig, eventLoopGroup: MultiThreadedEventLoopGroup.singleton, backgroundLogger: logger
        )

        try await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                await client.run()
            }

            for _ in 0..<10000 {
                taskGroup.addTask {
                    await #expect(throws: Never.self) { 
                        try await client.query("SELECT 1", logger: logger) 
                    }
                }
            }

            for _ in 0..<10000 {
                try await taskGroup.next()
            }

            taskGroup.cancelAll()
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func queryTable() async throws {
        let tableName = "test_client_prepared_statement"

        var mlogger = Logger(label: "test")
        mlogger.logLevel = .debug
        let logger = mlogger

        let clientConfig = PostgresClient.Configuration.makeTestConfiguration()
        let client = PostgresClient(
            configuration: clientConfig, eventLoopGroup: MultiThreadedEventLoopGroup.singleton, backgroundLogger: logger
        )

        try await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                await client.run()
            }

            try await client.query(
                """
                CREATE TABLE IF NOT EXISTS "\(unescaped: tableName)" (
                    id SERIAL PRIMARY KEY,
                    uuid UUID NOT NULL
                );
                """,
                logger: logger
            )

            for _ in 0..<1000 {
                try await client.query(
                    """
                    INSERT INTO "\(unescaped: tableName)" (uuid) VALUES (\(UUID()));
                    """,
                    logger: logger
                )
            }

            let rows = try await client.query(#"SELECT id, uuid FROM "\#(unescaped: tableName)";"#, logger: logger).decode((Int, UUID).self)
            for try await (id, uuid) in rows {
                logger.info("id: \(id), uuid: \(uuid.uuidString)")
            }

            struct Example: PostgresPreparedStatement {
                static let sql = "SELECT id, uuid FROM test_client_prepared_statement WHERE id < $1"
                typealias Row = (Int, UUID)
                var id: Int
                func makeBindings() -> PostgresBindings {
                    var bindings = PostgresBindings()
                    bindings.append(self.id)
                    return bindings
                }
                func decodeRow(_ row: PostgresNIO.PostgresRow) throws -> Row {
                    try row.decode(Row.self)
                }
            }

            for try await (id, uuid) in try await client.execute(Example(id: 200), logger: logger) {
                logger.info("id: \(id), uuid: \(uuid.uuidString)")
            }

            try await client.query(
                """
                DROP TABLE "\(unescaped: tableName)";
                """,
                logger: logger
            )

            taskGroup.cancelAll()
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func lTree() async throws {
        let tableName = "test_client_ltree"

        var mlogger = Logger(label: "test")
        mlogger.logLevel = .debug
        let logger = mlogger

        let clientConfig = PostgresClient.Configuration.makeTestConfiguration()
        let client = PostgresClient(
            configuration: clientConfig, eventLoopGroup: MultiThreadedEventLoopGroup.singleton, backgroundLogger: logger
        )

        try await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                await client.run()
            }

            try await client.query("CREATE EXTENSION IF NOT EXISTS ltree;")

            try await client.query("DROP TABLE IF EXISTS \"\(unescaped: tableName)\";")

            try await client.query(
                """
                CREATE TABLE IF NOT EXISTS "\(unescaped: tableName)" (
                    id SERIAL PRIMARY KEY,
                    label ltree NOT NULL
                );
                """
            )

            try await client.query(
                """
                INSERT INTO "\(unescaped: tableName)" (label) VALUES ('foo.bar.baz')
                """
            )

            let rows = try await client.query(
                """
                SELECT id, label FROM "\(unescaped: tableName)" WHERE label ~ 'foo.*'
                """
            )

            var count = 0
            for try await _ in rows.decode((Int, String).self) {
                count += 1
            }
            #expect(count == 1)

            taskGroup.cancelAll()
        }
    }

}

// MARK: - Structured queries

extension PostgresClientTests {
    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func structuredQueryReturnsBodyResult() async throws {
        try await self.withClient { client, logger in
            let sum = try await client.query("SELECT generate_series(1, 100)", logger: logger) { rows in
                var sum = 0
                for try await (value) in rows.decode(Int.self) {
                    sum += value
                }
                return sum
            }
            #expect(sum == 5050)
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func structuredQueryVoidBody() async throws {
        try await self.withClient { client, logger in
            try await client.query("SELECT 1", logger: logger) { rows in
                var count = 0
                for try await _ in rows { count += 1 }
                #expect(count == 1)
            }
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func structuredQueryReleasesLeaseAfterBodyReturns() async throws {
        try await self.withClient(maximumConnections: 1) { client, logger in
            for i in 0..<20 {
                if i.isMultiple(of: 2) {
                    let count = try await client.query("SELECT generate_series(1, 10)", logger: logger) { rows in
                        var count = 0
                        for try await _ in rows { count += 1 }
                        return count
                    }
                    #expect(count == 10)
                } else {
                    try await client.query("SELECT generate_series(1, 10)", logger: logger) { _ in }
                }
            }

            let rows = try await client.query("SELECT 1", logger: logger)
            for try await (value) in rows.decode(Int.self) {
                #expect(value == 1)
            }
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func structuredQueryReleasesLeaseWhenBodyStopsEarly() async throws {
        try await self.withClient(maximumConnections: 1) { client, logger in
            let first = try await client.query("SELECT generate_series(1, 1000000)", logger: logger) { rows -> Int? in
                for try await (value) in rows.decode(Int.self) {
                    return value
                }
                return nil
            }
            #expect(first == 1)

            let second = try await client.query("SELECT 2", logger: logger) { rows -> Int? in
                var last: Int?
                for try await (value) in rows.decode(Int.self) { last = value }
                return last
            }
            #expect(second == 2)
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func structuredQueryReleasesLeaseWhenBodyThrows() async throws {
        struct MyError: Error {}

        try await self.withClient(maximumConnections: 1) { client, logger in
            do {
                try await client.query("SELECT generate_series(1, 1000000)", logger: logger) { rows in
                    for try await _ in rows {
                        throw MyError()
                    }
                }
                Issue.record("Expected body error to propagate")
            } catch is MyError {}

            let value = try await client.query("SELECT 3", logger: logger) { rows -> Int? in
                var last: Int?
                for try await (value) in rows.decode(Int.self) { last = value }
                return last
            }
            #expect(value == 3)
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func structuredQueryEscapedSequenceIsInvalidated() async throws {
        try await self.withClient(maximumConnections: 1) { client, logger in
            let escaped = try await client.query("SELECT generate_series(1, 1000000)", logger: logger) { (rows: PostgresRowSequence) in
                rows
            }

            var rowsSeen = 0
            do {
                for try await _ in escaped { rowsSeen += 1 }
                Issue.record("Expected escaped sequence to throw")
            } catch let error as PSQLError {
                #expect(error.code == .rowSequenceUsedOutsideScope)
            }
            #expect(rowsSeen < 1_000_000, "Escaped sequence must not deliver the whole result")

            try await client.query("SELECT 1", logger: logger) { rows in
                for try await _ in rows {}
            }
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func structuredQueryServerErrorIsThrownWithMetadata() async throws {
        try await self.withClient { client, logger in
            let query: PostgresQuery = "SELECT * FROM table_that_does_not_exist"
            do {
                _ = try await client.query(query, logger: logger) { (_: PostgresRowSequence) in
                    Issue.record("Body must not run when the query fails upfront")
                }
                Issue.record("Expected query to throw")
            } catch let error as PSQLError {
                #expect(error.code == .server)
                #expect(error.serverInfo?[.sqlState] == "42P01")
                #expect(error.query == query)
                #expect(error.file == #fileID)
                #expect(error.line != nil)
            }
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func structuredQueryTooManyParameters() async throws {
        try await self.withClient { client, logger in
            var bindings = PostgresBindings()
            for _ in 0...Int(UInt16.max) {
                bindings.append(1)
            }
            let query = PostgresQuery(unsafeSQL: "SELECT 1", binds: bindings)

            do {
                _ = try await client.query(query, logger: logger) { (_: PostgresRowSequence) in
                    Issue.record("Body must not run")
                }
                Issue.record("Expected query to throw")
            } catch let error as PSQLError {
                #expect(error.code == .tooManyParameters)
            }
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func structuredQueryConcurrentlyThroughPool() async throws {
        try await self.withClient(maximumConnections: 8) { client, logger in
            let iterations = 1000

            try await withThrowingTaskGroup(of: Int.self) { taskGroup in
                for i in 0..<iterations {
                    taskGroup.addTask {
                        try await client.query("SELECT \(i)", logger: logger) { rows in
                            var last = -1
                            for try await (value) in rows.decode(Int.self) { last = value }
                            return last
                        }
                    }
                }

                var seen = Set<Int>()
                for try await value in taskGroup {
                    seen.insert(value)
                }
                #expect(seen.count == iterations)
            }
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func structuredQueryOnConnectionInsideWithConnection() async throws {
        try await self.withClient { client, logger in
            let names = try await client.withConnection { connection in
                try await connection.query("SELECT unnest(ARRAY['alice', 'bob', 'carol'])", logger: logger) { rows in
                    var names = [String]()
                    for try await (name) in rows.decode(String.self) {
                        names.append(name)
                    }
                    return names
                }
            }
            #expect(names == ["alice", "bob", "carol"])
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func queryWithMetadataReturnsSelectRowCount() async throws {
        try await self.withClient { client, logger in
            let (sum, metadata) = try await client.query("SELECT generate_series(1, 100)", logger: logger) { rows in
                var sum = 0
                for try await (value) in rows.decode(Int.self) {
                    sum += value
                }
                return sum
            }
            #expect(sum == 5050)
            #expect(metadata.command == "SELECT")
            #expect(metadata.rows == 100)
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func queryWithMetadataReportsAffectedRowsForWrites() async throws {
        let tableName = "test_client_query_with_metadata"

        try await self.withClient { client, logger in
            try await client.query("DROP TABLE IF EXISTS \"\(unescaped: tableName)\";", logger: logger)
            try await client.query(
                """
                CREATE TABLE "\(unescaped: tableName)" (
                    id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                    value INT NOT NULL
                );
                """,
                logger: logger
            )

            let (_, insert) = try await client.query(
                #"INSERT INTO "\#(unescaped: tableName)" (value) SELECT generate_series(1, 25);"#,
                logger: logger
            ) { rows in
                for try await _ in rows {}
            }
            #expect(insert.command == "INSERT")
            #expect(insert.oid == 0)
            #expect(insert.rows == 25)

            let (doubled, update) = try await client.query(
                #"UPDATE "\#(unescaped: tableName)" SET value = value * 2 WHERE value > 20 RETURNING value;"#,
                logger: logger
            ) { rows in
                var doubled = [Int]()
                for try await (value) in rows.decode(Int.self) {
                    doubled.append(value)
                }
                return doubled.sorted()
            }
            #expect(doubled == [42, 44, 46, 48, 50])
            #expect(update.command == "UPDATE")
            #expect(update.rows == 5)

            let (_, delete) = try await client.query(
                #"DELETE FROM "\#(unescaped: tableName)";"#,
                logger: logger
            ) { rows in
                for try await _ in rows {}
            }
            #expect(delete.command == "DELETE")
            #expect(delete.rows == 25)

            try await client.query("DROP TABLE \"\(unescaped: tableName)\";", logger: logger)
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func queryWithMetadataReleasesLeaseAfterBodyReturns() async throws {
        try await self.withClient(maximumConnections: 1) { client, logger in
            for i in 1...10 {
                let (count, metadata) = try await client.query("SELECT generate_series(1, \(i))", logger: logger) { rows in
                    var count = 0
                    for try await _ in rows { count += 1 }
                    return count
                }
                #expect(count == i)
                #expect(metadata.rows == i)
            }

            let rows = try await client.query("SELECT 1", logger: logger)
            for try await (value) in rows.decode(Int.self) {
                #expect(value == 1)
            }
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func queryWithMetadataBodyThrowsReleasesLease() async throws {
        struct MyError: Error {}

        try await self.withClient(maximumConnections: 1) { client, logger in
            await #expect(throws: MyError.self) {
                try await client.query("SELECT generate_series(1, 1000000)", logger: logger) { rows in
                    for try await _ in rows {
                        throw MyError()
                    }
                }
            }

            let (_, metadata) = try await client.query("SELECT 3", logger: logger) { rows in
                for try await _ in rows {}
            }
            #expect(metadata.rows == 1)
        }
    }

    // MARK: Helpers

    private func withClient(
        maximumConnections: Int? = nil,
        _ body: (PostgresClient, Logger) async throws -> ()
    ) async throws {
        var logger = Logger(label: "test")
        logger.logLevel = .debug

        var clientConfig = PostgresClient.Configuration.makeTestConfiguration()
        if let maximumConnections {
            clientConfig.options.maximumConnections = maximumConnections
        }
        let client = PostgresClient(configuration: clientConfig, eventLoopGroup: .singletonMultiThreadedEventLoopGroup, backgroundLogger: logger)

        try await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                await client.run()
            }

            do {
                try await body(client, logger)
            } catch {
                taskGroup.cancelAll()
                throw error
            }

            taskGroup.cancelAll()
        }
    }
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
extension PostgresClient.Configuration {
    static func makeTestConfiguration() -> PostgresClient.Configuration {
        var tlsConfiguration = TLSConfiguration.makeClientConfiguration()
        tlsConfiguration.certificateVerification = .none
        var clientConfig = PostgresClient.Configuration(
            host: env("POSTGRES_HOSTNAME") ?? "localhost",
            port: env("POSTGRES_PORT").flatMap({ Int($0) }) ?? 5432,
            username: env("POSTGRES_USER") ?? "test_username",
            password: env("POSTGRES_PASSWORD") ?? "test_password",
            database: env("POSTGRES_DB") ?? "test_database",
            tls: .prefer(tlsConfiguration)
        )
        clientConfig.options.minimumConnections = 0
        clientConfig.options.maximumConnections = 12*4
        clientConfig.options.keepAliveBehavior = .init(frequency: .seconds(5))
        clientConfig.options.connectionIdleTimeout = .seconds(15)

        return clientConfig
    }
}
