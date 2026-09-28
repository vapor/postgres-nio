import Logging
import NIOCore
import NIOPosix
import PostgresNIO
import Testing

#if canImport(Network)
    import NIOTransportServices
#endif

#if canImport(FoundationEssentials)
    import FoundationEssentials
#else
    import Foundation
#endif

@Suite(.serialized)
struct AsyncPostgresConnectionTests {
    @Test func oneThousandRoundTrips() async throws {
        try await withConnection { connection in
            for _ in 0..<1_000 {
                let rows = try await connection.query("SELECT version()", logger: .psqlTest)
                var iterator = rows.makeAsyncIterator()
                let firstRow = try await iterator.next()
                #expect(try firstRow?.decode(String.self, context: .default).contains("PostgreSQL") == true)
                let done = try await iterator.next()
                #expect(done == nil)
            }
        }
    }

    @Test func select10kRows() async throws {
        let start = 1
        let end = 10000

        try await withConnection { connection in
            let rows = try await connection.query("SELECT generate_series(\(start), \(end));", logger: .psqlTest)
            var counter = 0
            for try await row in rows {
                let element = try row.decode(Int.self)
                #expect(element == counter + 1)
                counter += 1
            }

            #expect(counter == end)
        }
    }

    @Test func selectActiveConnection() async throws {

        let query: PostgresQuery = """
            SELECT
                pid
                ,datname
                ,usename
                ,application_name
                ,client_hostname
                ,client_port
                ,backend_start
                ,query_start
                ,query
                ,state
            FROM pg_stat_activity
            WHERE state = 'active';
            """

        try await withConnection { connection in
            let rows = try await connection.query(query, logger: .psqlTest)
            var counter = 0

            for try await element in rows.decode((Int, String, String, String, String?, Int, Date, Date, String, String).self) {
                #expect(element.1 == env("POSTGRES_DB") ?? "test_database")
                #expect(element.2 == env("POSTGRES_USER") ?? "test_username")

                #expect(element.8 == query.sql)
                #expect(element.9 == "active")
                counter += 1
            }

            #expect(counter >= 1)
        }
    }

    @Test func additionalParametersTakeEffect() async throws {
        let query: PostgresQuery = """
            SELECT
                current_setting('application_name');
            """

        let applicationName = "postgres-nio-test"
        var options = PostgresConnection.Configuration.Options()
        options.additionalStartupParameters = [
            ("application_name", applicationName)
        ]

        try await withConnection(options: options) { connection in
            let rows = try await connection.query(query, logger: .psqlTest)
            var counter = 0

            for try await element in rows.decode(String.self) {
                #expect(element == applicationName)

                counter += 1
            }

            #expect(counter >= 1)
        }
    }

    @Test func selectTimeoutWhileLongRunningQuery() async throws {
        let start = 1
        let end = 10_000_000

        try await withConnection { connection -> Void in
            try await connection.query("SET statement_timeout=1000;", logger: .psqlTest)

            let rows = try await connection.query("SELECT generate_series(\(start), \(end));", logger: .psqlTest)
            var counter = 0
            let thrown = await #expect(throws: PSQLError.self, "Expected to get cancelled while reading the query") {
                for try await element in rows.decode(Int.self, context: .default) {
                    #expect(element == counter + 1)
                    counter += 1
                }
            }

            let error = try #require(thrown)
            #expect(error.code == .server)
            #expect(error.serverInfo?[.severity] == "ERROR")

            #expect(!connection.isClosed, "Connection should survive!")

            for num in 0..<10 {
                for try await decoded in try await connection.query("SELECT \(num);", logger: .psqlTest).decode(Int.self) {
                    #expect(decoded == num)
                }
            }
        }
    }

    @Test func connectionSurvives1kQueriesWithATypo() async throws {

        let start = 1
        let end = 10000

        try await withConnection { connection -> Void in
            for _ in 0..<1000 {
                let thrown = await #expect(throws: PSQLError.self, "Expected to throw from the request") {
                    try await connection.query("SELECT generte_series(\(start), \(end));", logger: .psqlTest)
                }

                let error = try #require(thrown)
                #expect(error.code == .server)
                #expect(error.serverInfo?[.severity] == "ERROR")
            }

            // the connection survived all of this, we can still run normal queries:

            for num in 0..<10 {
                for try await decoded in try await connection.query("SELECT \(num);", logger: .psqlTest).decode(Int.self) {
                    #expect(decoded == num)
                }
            }
        }
    }

    @Test func select10times10kRows() async throws {

        let start = 1
        let end = 10000

        try await withConnection { connection in
            await withThrowingTaskGroup(of: Void.self) { taskGroup in
                for _ in 0..<10 {
                    taskGroup.addTask {
                        try await connection.query("SELECT generate_series(\(start), \(end));", logger: .psqlTest)
                    }
                }
            }
        }
    }

    @Test func bindMaximumParameters() async throws {

        try await withConnection { connection in
            // Max binds limit is UInt16.max which is 65535 which is 3 * 5 * 17 * 257
            // Max columns limit is 1664, so we will only make 5 * 257 columns which is less
            // Then we will insert 3 * 17 rows
            // In the insertion, there will be a total of 3 * 17 * 5 * 257 == UInt16.max bindings
            // If the test is successful, it means Postgres supports UInt16.max bindings
            let columnsCount = 5 * 257
            let rowsCount = 3 * 17

            let createQuery = PostgresQuery(
                unsafeSQL: """
                    CREATE TABLE table1 (
                    \((0..<columnsCount).map({ #""int\#($0)" int NOT NULL"# }).joined(separator: ", "))
                    );
                    """
            )
            try await connection.query(createQuery, logger: .psqlTest)

            var binds = PostgresBindings(capacity: Int(UInt16.max))
            for _ in (0..<rowsCount) {
                for num in (0..<columnsCount) {
                    binds.append(num, context: .default)
                }
            }
            #expect(binds.count == Int(UInt16.max))

            let insertionValues = (0..<rowsCount).map { rowIndex in
                let indices = (0..<columnsCount).map { columnIndex -> String in
                    "$\(rowIndex * columnsCount + columnIndex + 1)"
                }
                return "(\(indices.joined(separator: ", ")))"
            }.joined(separator: ", ")
            let insertionQuery = PostgresQuery(
                unsafeSQL: "INSERT INTO table1 VALUES \(insertionValues)",
                binds: binds
            )
            try await connection.query(insertionQuery, logger: .psqlTest)

            let countQuery = PostgresQuery(unsafeSQL: "SELECT COUNT(*) FROM table1")
            let countRows = try await connection.query(countQuery, logger: .psqlTest)
            var countIterator = countRows.makeAsyncIterator()
            let insertedRowsCount = try await countIterator.next()?.decode(Int.self, context: .default)
            #expect(rowsCount == insertedRowsCount)

            let dropQuery = PostgresQuery(unsafeSQL: "DROP TABLE table1")
            try await connection.query(dropQuery, logger: .psqlTest)
        }
    }

    @available(*, deprecated, message: "Deprecated, as it tests a deprecated method.")
    @Test func listenAndNotify() async throws {
        let channelNames = [
            "foo",
            "default",
        ]

        let eventLoop = MultiThreadedEventLoopGroup.singleton.any()

        for channelName in channelNames {
            try await withConnection(on: eventLoop) { connection in
                let stream = try await connection.listen(channelName)
                var iterator = stream.makeAsyncIterator()

                try await withConnection(on: eventLoop) { other in
                    try await other.query(#"NOTIFY "\#(unescaped: channelName)", 'bar';"#, logger: .psqlTest)

                    try await other.query(#"NOTIFY "\#(unescaped: channelName)", 'foo';"#, logger: .psqlTest)
                }

                let first = try await iterator.next()
                #expect(first?.payload == "bar")

                let second = try await iterator.next()
                #expect(second?.payload == "foo")
            }
        }
    }

    @available(*, deprecated, message: "Deprecated, as it tests a deprecated method.")
    @Test func listenTwiceChannel() async throws {
        let eventLoop = MultiThreadedEventLoopGroup.singleton.any()

        try await withConnection(on: eventLoop) { connection in
            // Concurrently listen on a channel that is initially closed
            async let stream1later = connection.listen("same-channel")
            async let stream2later = connection.listen("same-channel")
            let (stream1, stream2) = try await (stream1later, stream2later)

            _ = try await withConnection(on: eventLoop) { other in
                try await other.query(#"NOTIFY "\#(unescaped: "same-channel")";"#, logger: .psqlTest)
            }

            var stream1EventReceived = false
            var stream2EventReceived = false

            for try await _ in stream1 {
                stream1EventReceived = true
                break
            }

            for try await _ in stream2 {
                stream2EventReceived = true
                break
            }

            #expect(stream1EventReceived)
            #expect(stream2EventReceived)
        }
    }

    @available(*, deprecated, message: "Deprecated, as it tests a deprecated method.")
    @Test func listenOnClosedChannel() async throws {

        try await withConnection { connection in
            try await connection.close()
            let error = await #expect(throws: PSQLError.self, "Expected not to get any events") {
                try await connection.listen("futile")
            }
            #expect(error?.code == .listenFailed)
        }
    }

    @available(*, deprecated, message: "Deprecated, as it tests a deprecated method.")
    @Test func listenThenCloseChannel() async throws {

        try await withConnection { connection in
            let stream = try await connection.listen("hopeful")
            try await connection.close()
            await #expect(throws: PSQLError.self, "Expected not to have reached the end of stream") {
                for try await _ in stream {
                    Issue.record("Expected not to get any events")
                }
            }
        }
    }

    @available(*, deprecated, message: "Deprecated, as it tests a deprecated method.")
    @Test func listenThenClosingChannel() async throws {
        try await withConnection { connection in
            _ = try await connection.listen("initial")
            async let asyncClose: () = connection.close()
            let stream: PostgresNotificationSequence
            do {
                stream = try await connection.listen("hopeful")
            } catch let error as PSQLError where error.code == .listenFailed {
                // Expected
                return
            }
            try await asyncClose
            await #expect(throws: PSQLError.self, "Expected not to have reached the end of stream") {
                for try await _ in stream {
                    Issue.record("Expected not to get any events")
                }
            }
        }
    }

    @Test func listenOnChannelWithClosure() async throws {
        let channelNames = [
            "foo",
            "default",
        ]

        let eventLoop = MultiThreadedEventLoopGroup.singleton.any()

        for channelName in channelNames {
            try await withConnection(on: eventLoop) { connection in
                try await connection.listen(on: channelName) { stream in
                    var iterator = stream.makeAsyncIterator()

                    try await withConnection(on: eventLoop) { other in
                        try await other.query(#"NOTIFY "\#(unescaped: channelName)", 'bar';"#, logger: .psqlTest)

                        try await other.query(#"NOTIFY "\#(unescaped: channelName)", 'foo';"#, logger: .psqlTest)
                    }

                    let first = try await iterator.next()
                    #expect(first?.payload == "bar")

                    let second = try await iterator.next()
                    #expect(second?.payload == "foo")
                }
            }
        }
    }

    @Test func leavingTheScopeSecondsAfterCancellationDoesNotCrash() async throws {
        try await withConnection { connection in
            await withThrowingTaskGroup(of: Void.self) { taskGroup in
                let (stream, cont) = AsyncStream.makeStream(of: Void.self)

                taskGroup.addTask {
                    try await connection.listen(on: "foo") { stream in
                        cont.yield()
                        for try await _ in stream {}
                        _ = await Task {
                            try? await Task.sleep(for: .seconds(1))
                        }.result
                        // scope is left long after task is cancelled
                    }
                }
                // wait until listen has started by using an AsyncStream.
                await stream.first { _ in true }
                taskGroup.cancelAll()
            }
        }
    }

    #if canImport(Network)
        @Test func select10kRowsNetworkFramework() async throws {
            let eventLoop = NIOTSEventLoopGroup.singleton.any()

            let start = 1
            let end = 10000

            try await withConnection(on: eventLoop) { connection in
                let rows = try await connection.query("SELECT generate_series(\(start), \(end));", logger: .psqlTest)
                var counter = 1
                for try await row in rows {
                    let element = try row.decode(Int.self, context: .default)
                    #expect(element == counter)
                    counter += 1
                }

                #expect(counter == end + 1)
            }
        }
    #endif

    @Test func cancelTaskThatIsVeryLongRunningWhichAlsoFailsWhileInStreamingMode() async throws {
        // we cancel the query after 400ms.
        // the server times out the query after 1sec.
        try await withConnection { connection -> Void in
            try await connection.query("SET statement_timeout=1000;", logger: .psqlTest)  // 1000 milliseconds

            try await withThrowingTaskGroup(of: Void.self) { group in
                group.addTask {
                    let start = 1
                    let end = 100_000_000

                    let rows = try await connection.query("SELECT generate_series(\(start), \(end));", logger: .psqlTest)
                    var counter = 0
                    await #expect(throws: CancellationError.self, "Expected to get cancelled while reading the query") {
                        for try await element in rows.decode(Int.self, context: .default) {
                            #expect(element == counter + 1)
                            counter += 1
                        }
                    }
                    #expect(counter >= 1)

                    #expect(Task.isCancelled)
                    #expect(!connection.isClosed, "Connection should survive!")
                }

                let delay: UInt64 = 400_000_000  // 400 milliseconds
                try await Task.sleep(nanoseconds: delay)

                group.cancelAll()
            }

            try await connection.query("SELECT 1;", logger: .psqlTest)
        }
    }

    @Test func preparedStatement() async throws {
        struct TestPreparedStatement: PostgresPreparedStatement {
            static let sql = "SELECT pid, datname FROM pg_stat_activity WHERE state = $1"
            typealias Row = (Int, String)

            var state: String

            func makeBindings() -> PostgresBindings {
                var bindings = PostgresBindings()
                bindings.append(self.state)
                return bindings
            }

            func decodeRow(_ row: PostgresNIO.PostgresRow) throws -> Row {
                try row.decode(Row.self)
            }
        }
        let preparedStatement = TestPreparedStatement(state: "active")
        try await withConnection { connection in
            var results = try await connection.execute(preparedStatement, logger: .psqlTest)
            var counter = 0

            for try await element in results {
                #expect(element.1 == env("POSTGRES_DB") ?? "test_database")
                counter += 1
            }

            #expect(counter >= 1)

            // Second execution, which reuses the existing prepared statement
            results = try await connection.execute(preparedStatement, logger: .psqlTest)
            for try await element in results {
                #expect(element.1 == env("POSTGRES_DB") ?? "test_database")
                counter += 1
            }
        }
    }

    static let preparedStatementTestTable = "AsyncTestPreparedStatementTestTable"
    @Test func preparedStatementWithIntegerBinding() async throws {
        struct InsertPreparedStatement: PostgresPreparedStatement {
            static let name = "INSERT-AsyncTestPreparedStatementTestTable"

            static let sql = #"INSERT INTO "\#(AsyncPostgresConnectionTests.preparedStatementTestTable)" (uuid) VALUES ($1);"#
            typealias Row = ()

            var uuid: UUID

            func makeBindings() -> PostgresBindings {
                var bindings = PostgresBindings()
                bindings.append(self.uuid)
                return bindings
            }

            func decodeRow(_ row: PostgresNIO.PostgresRow) throws -> Row {
                ()
            }
        }

        struct SelectPreparedStatement: PostgresPreparedStatement {
            static let name = "SELECT-AsyncTestPreparedStatementTestTable"

            static let sql = #"SELECT id, uuid FROM "\#(AsyncPostgresConnectionTests.preparedStatementTestTable)" WHERE id <= $1;"#
            typealias Row = (Int, UUID)

            var id: Int

            func makeBindings() -> PostgresBindings {
                var bindings = PostgresBindings()
                bindings.append(self.id)
                return bindings
            }

            func decodeRow(_ row: PostgresNIO.PostgresRow) throws -> Row {
                try row.decode((Int, UUID).self)
            }
        }

        try await withConnection { connection in
            try await connection.query(
                """
                CREATE TABLE IF NOT EXISTS "\(unescaped: Self.preparedStatementTestTable)" (
                    id SERIAL PRIMARY KEY,
                    uuid UUID NOT NULL
                )
                """,
                logger: .psqlTest
            )

            _ = try await connection.execute(InsertPreparedStatement(uuid: .init()), logger: .psqlTest)
            _ = try await connection.execute(InsertPreparedStatement(uuid: .init()), logger: .psqlTest)
            _ = try await connection.execute(InsertPreparedStatement(uuid: .init()), logger: .psqlTest)
            _ = try await connection.execute(InsertPreparedStatement(uuid: .init()), logger: .psqlTest)
            _ = try await connection.execute(InsertPreparedStatement(uuid: .init()), logger: .psqlTest)

            let rows = try await connection.execute(SelectPreparedStatement(id: 3), logger: .psqlTest)
            var counter = 0
            for try await (id, uuid) in rows {
                Logger.psqlTest.info(
                    "Received row",
                    metadata: [
                        "id": "\(id)", "uuid": "\(uuid)",
                    ])
                counter += 1
            }

            try await connection.query(
                """
                DROP TABLE "\(unescaped: Self.preparedStatementTestTable)";
                """,
                logger: .psqlTest
            )
        }
    }

    static let preparedStatementWithOptionalTestTable = "AsyncTestPreparedStatementWithOptionalTestTable"
    @Test func preparedStatementWithOptionalBinding() async throws {
        struct InsertPreparedStatement: PostgresPreparedStatement {
            static let name = "INSERT-AsyncTestPreparedStatementWithOptionalTestTable"

            static let sql = #"INSERT INTO "\#(AsyncPostgresConnectionTests.preparedStatementWithOptionalTestTable)" (uuid) VALUES ($1);"#
            typealias Row = ()

            var uuid: UUID?

            func makeBindings() -> PostgresBindings {
                var bindings = PostgresBindings()
                bindings.append(self.uuid)
                return bindings
            }

            func decodeRow(_ row: PostgresNIO.PostgresRow) throws -> Row {
                ()
            }
        }

        struct SelectPreparedStatement: PostgresPreparedStatement {
            static let name = "SELECT-AsyncTestPreparedStatementWithOptionalTestTable"

            static let sql =
                #"SELECT id, uuid FROM "\#(AsyncPostgresConnectionTests.preparedStatementWithOptionalTestTable)" WHERE id <= $1;"#
            typealias Row = (Int, UUID?)

            var id: Int

            func makeBindings() -> PostgresBindings {
                var bindings = PostgresBindings()
                bindings.append(self.id)
                return bindings
            }

            func decodeRow(_ row: PostgresNIO.PostgresRow) throws -> Row {
                try row.decode((Int, UUID?).self)
            }
        }

        try await withConnection { connection in
            try await connection.query(
                """
                CREATE TABLE IF NOT EXISTS "\(unescaped: Self.preparedStatementWithOptionalTestTable)" (
                    id SERIAL PRIMARY KEY,
                    uuid UUID
                )
                """,
                logger: .psqlTest
            )

            _ = try await connection.execute(InsertPreparedStatement(uuid: nil), logger: .psqlTest)
            _ = try await connection.execute(InsertPreparedStatement(uuid: .init()), logger: .psqlTest)
            _ = try await connection.execute(InsertPreparedStatement(uuid: nil), logger: .psqlTest)
            _ = try await connection.execute(InsertPreparedStatement(uuid: .init()), logger: .psqlTest)
            _ = try await connection.execute(InsertPreparedStatement(uuid: nil), logger: .psqlTest)

            let rows = try await connection.execute(SelectPreparedStatement(id: 3), logger: .psqlTest)
            var counter = 0
            for try await (id, uuid) in rows {
                Logger.psqlTest.info(
                    "Received row",
                    metadata: [
                        "id": "\(id)", "uuid": "\(String(describing: uuid))",
                    ])
                counter += 1
            }

            try await connection.query(
                """
                DROP TABLE "\(unescaped: Self.preparedStatementWithOptionalTestTable)";
                """,
                logger: .psqlTest
            )
        }
    }
}
