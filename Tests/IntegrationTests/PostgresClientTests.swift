@_spi(ConnectionPool) import PostgresNIO
import XCTest
import NIOPosix
import NIOSSL
import Logging
import Atomics

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
final class PostgresClientTests: XCTestCase {

    func testGetConnection() async throws {
        var mlogger = Logger(label: "test")
        mlogger.logLevel = .debug
        let logger = mlogger
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 8)
        self.addTeardownBlock {
            try await eventLoopGroup.shutdownGracefully()
        }

        let clientConfig = PostgresClient.Configuration.makeTestConfiguration()
        let client = PostgresClient(configuration: clientConfig, eventLoopGroup: eventLoopGroup, backgroundLogger: logger)

        await withThrowingTaskGroup(of: Void.self) { taskGroup in
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
                _ = await taskGroup.nextResult()!
            }

            taskGroup.cancelAll()
        }
    }
    
    func testTransaction() async throws {
        var mlogger = Logger(label: "test")
        mlogger.logLevel = .debug
        let logger = mlogger
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 8)
        self.addTeardownBlock {
            try await eventLoopGroup.shutdownGracefully()
        }
        
        let tableName = "test_client_transactions"
        
        let clientConfig = PostgresClient.Configuration.makeTestConfiguration()
        let client = PostgresClient(configuration: clientConfig, eventLoopGroup: eventLoopGroup, backgroundLogger: logger)
        
        do {
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
                    _ = await taskGroup.nextResult()!
                }
                
                let rows = try await client.query(#"SELECT COUNT(1)::INT AS table_size FROM "\#(unescaped: tableName)";"#, logger: logger).decode(Int.self)
                for try await (count) in rows {
                    XCTAssertEqual(count, iterations)
                }
                
                /// Test roll back
                taskGroup.addTask {
                    
                    do {
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
                    } catch {
                        XCTAssertNotNil(error)
                        guard let error = error as? PostgresTransactionError else { return XCTFail("Unexpected error type: \(error)") }

                        XCTAssertEqual((error.closureError as? PSQLError)?.code, .server)
                        XCTAssertEqual((error.closureError as? PSQLError)?.serverInfo?[.severity], "ERROR")
                    }
                }
                
                let row = try await client.query(#"SELECT COUNT(1)::INT AS table_size FROM "\#(unescaped: tableName)";"#, logger: logger).decode(Int.self)
                
                for try await (count) in row {
                    XCTAssertEqual(count, iterations)
                }
                
                try await client.query(
                    """
                    DROP TABLE "\(unescaped: tableName)";
                    """,
                    logger: logger
                )
                
                taskGroup.cancelAll()
            }
        } catch {
            XCTFail("Unexpected error: \(String(reflecting: error))")
        }
    }

    func testApplicationNameIsForwardedCorrectly() async throws {
        var mlogger = Logger(label: "test")
        mlogger.logLevel = .debug
        let logger = mlogger
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 8)
        self.addTeardownBlock {
            try await eventLoopGroup.shutdownGracefully()
        }

        var clientConfig = PostgresClient.Configuration.makeTestConfiguration()
        let applicationName = "postgres_nio_test_run"
        clientConfig.options.additionalStartupParameters = [("application_name", applicationName)]
        let client = PostgresClient(configuration: clientConfig, eventLoopGroup: eventLoopGroup, backgroundLogger: logger)

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

            XCTAssertGreaterThanOrEqual(applicationNameFound, 1)

            taskGroup.cancelAll()
        }
    }


    func testQueryDirectly() async throws {
        var mlogger = Logger(label: "test")
        mlogger.logLevel = .debug
        let logger = mlogger
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 8)
        self.addTeardownBlock {
            try await eventLoopGroup.shutdownGracefully()
        }

        let clientConfig = PostgresClient.Configuration.makeTestConfiguration()
        let client = PostgresClient(configuration: clientConfig, eventLoopGroup: eventLoopGroup, backgroundLogger: logger)

        await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                await client.run()
            }

            for _ in 0..<10000 {
                taskGroup.addTask {
                    do {
                        try await client.query("SELECT 1", logger: logger)
                    } catch {
                        XCTFail("Unexpected error: \(error)")
                    }
                }
            }

            for _ in 0..<10000 {
                _ = await taskGroup.nextResult()!
            }

            taskGroup.cancelAll()
        }
    }

    func testQueryTable() async throws {
        let tableName = "test_client_prepared_statement"

        var mlogger = Logger(label: "test")
        mlogger.logLevel = .debug
        let logger = mlogger
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 8)
        self.addTeardownBlock {
            try await eventLoopGroup.shutdownGracefully()
        }

        let clientConfig = PostgresClient.Configuration.makeTestConfiguration()
        let client = PostgresClient(configuration: clientConfig, eventLoopGroup: eventLoopGroup, backgroundLogger: logger)
        do {
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
        } catch {
            XCTFail("Unexpected error: \(String(reflecting: error))")
        }
    }

    func testLTree() async throws {
        let tableName = "test_client_ltree"

        var mlogger = Logger(label: "test")
        mlogger.logLevel = .debug
        let logger = mlogger
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 8)
        self.addTeardownBlock {
            try await eventLoopGroup.shutdownGracefully()
        }

        let clientConfig = PostgresClient.Configuration.makeTestConfiguration()
        let client = PostgresClient(configuration: clientConfig, eventLoopGroup: eventLoopGroup, backgroundLogger: logger)

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
            XCTAssertEqual(count, 1)

            taskGroup.cancelAll()
        }
    }

    /// Ensures the `ltree` extension exists, using a client that does *not* resolve additional data types.
    ///
    /// A client configured with `additionalDataTypeNames = ["ltree"]` cannot be used for this: the OID is
    /// resolved while the connection is being established, so a connection opened before the extension
    /// exists caches an empty lookup table and keeps using it for every later query on that connection.
    private func ensureLTreeExtension(eventLoopGroup: any EventLoopGroup, logger: Logger) async throws {
        let client = PostgresClient(
            configuration: .makeTestConfiguration(),
            eventLoopGroup: eventLoopGroup,
            backgroundLogger: logger
        )

        try await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                await client.run()
            }

            try await client.query("CREATE EXTENSION IF NOT EXISTS ltree;")

            taskGroup.cancelAll()
        }
    }

    /// Removes the `ltree` extension, so a connection can be established while the type does not exist.
    ///
    /// Uses a client that does *not* resolve additional data types, for the same reason as
    /// ``ensureLTreeExtension(eventLoopGroup:logger:)``.
    private func dropLTreeExtension(eventLoopGroup: any EventLoopGroup, logger: Logger) async throws {
        let client = PostgresClient(
            configuration: .makeTestConfiguration(),
            eventLoopGroup: eventLoopGroup,
            backgroundLogger: logger
        )

        try await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                await client.run()
            }

            try await client.query("DROP EXTENSION IF EXISTS ltree CASCADE;")

            taskGroup.cancelAll()
        }
    }

    /// A connection established *before* the extension existed resolves the OID lazily, on first use.
    ///
    /// Without lazy resolution the connection would cache an empty lookup table at establishment and keep
    /// using it forever, so `SELECT $1` would send OID `0`, Postgres would infer `text`, and decoding the
    /// result as an `ltree` would fail on the binary version prefix.
    ///
    /// `maximumConnections = 1` is load bearing: it guarantees the query below runs on the very connection
    /// that was opened while `ltree` did not exist. Without it the pool may hand out a fresh connection
    /// that resolved the OID eagerly, and the test would pass without exercising lazy resolution at all.
    func testLTreeResolvesLazilyAfterExtensionIsInstalled() async throws {
        var mlogger = Logger(label: "test")
        mlogger.logLevel = .debug
        let logger = mlogger
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 8)
        self.addTeardownBlock {
            try await eventLoopGroup.shutdownGracefully()
        }

        try await self.dropLTreeExtension(eventLoopGroup: eventLoopGroup, logger: logger)

        var clientConfig = PostgresClient.Configuration.makeTestConfiguration()
        // Deliberately NOT listed in additionalDataTypeNames: listing it makes the type a requirement of
        // the connection, which would fail establishment here since ltree does not exist yet. Leaving it
        // out is what selects the lenient, resolve-on-demand path this test is about.
        clientConfig.options.minimumConnections = 0
        clientConfig.options.maximumConnections = 1
        let client = PostgresClient(configuration: clientConfig, eventLoopGroup: eventLoopGroup, backgroundLogger: logger)

        try await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                await client.run()
            }

            // Opens the pool's only connection. `ltree` does not exist yet, so eager resolution finds nothing.
            try await client.query("CREATE EXTENSION IF NOT EXISTS ltree;")

            let value = PostgresLTree(labels: ["foo", "bar", "baz"])
            let rows = try await client.query("SELECT \(value)")

            var decoded = [PostgresLTree]()
            for try await label in rows.decode(PostgresLTree.self) {
                decoded.append(label)
            }
            XCTAssertEqual(decoded, [value])

            taskGroup.cancelAll()
        }
    }

    /// The same lazy resolution has to work for prepared statements.
    ///
    /// Prepared statements declare their parameter OIDs once, in `Parse`, and the server caches the result.
    /// If the OID were resolved when the task was created rather than when the message is sent, the statement
    /// would be parsed with OID `0` and stay broken for the life of the connection, even after the lookup
    /// completed.
    func testLTreeResolvesLazilyForPreparedStatements() async throws {
        struct SelectLTree: PostgresPreparedStatement {
            static let name = "SELECT-ltree-lazy-resolution"
            static let sql = "SELECT $1"
            typealias Row = PostgresLTree

            var label: PostgresLTree

            func makeBindings() -> PostgresBindings {
                var bindings = PostgresBindings()
                bindings.append(self.label)
                return bindings
            }

            func decodeRow(_ row: PostgresRow) throws -> Row {
                try row.decode(PostgresLTree.self)
            }
        }

        var mlogger = Logger(label: "test")
        mlogger.logLevel = .debug
        let logger = mlogger
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 8)
        self.addTeardownBlock {
            try await eventLoopGroup.shutdownGracefully()
        }

        try await self.dropLTreeExtension(eventLoopGroup: eventLoopGroup, logger: logger)

        var clientConfig = PostgresClient.Configuration.makeTestConfiguration()
        // Deliberately NOT listed in additionalDataTypeNames: listing it makes the type a requirement of
        // the connection, which would fail establishment here since ltree does not exist yet. Leaving it
        // out is what selects the lenient, resolve-on-demand path this test is about.
        clientConfig.options.minimumConnections = 0
        clientConfig.options.maximumConnections = 1
        let client = PostgresClient(configuration: clientConfig, eventLoopGroup: eventLoopGroup, backgroundLogger: logger)

        try await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                await client.run()
            }

            try await client.query("CREATE EXTENSION IF NOT EXISTS ltree;")

            let value = PostgresLTree(labels: ["foo", "bar", "baz"])

            var decoded = [PostgresLTree]()
            for try await label in try await client.execute(SelectLTree(label: value), logger: logger) {
                decoded.append(label)
            }
            XCTAssertEqual(decoded, [value])

            taskGroup.cancelAll()
        }
    }

    func testLTreeBindRoundTrip() async throws {
        let tableName = "test_client_ltree_bind"

        var mlogger = Logger(label: "test")
        mlogger.logLevel = .debug
        let logger = mlogger
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 8)
        self.addTeardownBlock {
            try await eventLoopGroup.shutdownGracefully()
        }

        try await self.ensureLTreeExtension(eventLoopGroup: eventLoopGroup, logger: logger)

        var clientConfig = PostgresClient.Configuration.makeTestConfiguration()
        clientConfig.options.additionalDataTypeNames = ["ltree"]
        let client = PostgresClient(configuration: clientConfig, eventLoopGroup: eventLoopGroup, backgroundLogger: logger)

        try await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                await client.run()
            }

            try await client.query("DROP TABLE IF EXISTS \"\(unescaped: tableName)\";")

            try await client.query(
                """
                CREATE TABLE IF NOT EXISTS "\(unescaped: tableName)" (
                    id SERIAL PRIMARY KEY,
                    label ltree NOT NULL
                );
                """
            )

            let inserted = PostgresLTree(labels: ["foo", "bar", "baz"])

            try await client.query(
                """
                INSERT INTO "\(unescaped: tableName)" (label) VALUES (\(inserted))
                """
            )

            let rows = try await client.query(
                """
                SELECT label FROM "\(unescaped: tableName)" WHERE label ~ 'foo.*'
                """
            )

            var decoded = [PostgresLTree]()
            for try await label in rows.decode(PostgresLTree.self) {
                decoded.append(label)
            }
            XCTAssertEqual(decoded, [inserted])

            taskGroup.cancelAll()
        }
    }

    func testLTreeBindInTypeAmbiguousContext() async throws {
        var mlogger = Logger(label: "test")
        mlogger.logLevel = .debug
        let logger = mlogger
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 8)
        self.addTeardownBlock {
            try await eventLoopGroup.shutdownGracefully()
        }

        try await self.ensureLTreeExtension(eventLoopGroup: eventLoopGroup, logger: logger)

        var clientConfig = PostgresClient.Configuration.makeTestConfiguration()
        clientConfig.options.additionalDataTypeNames = ["ltree"]
        let client = PostgresClient(configuration: clientConfig, eventLoopGroup: eventLoopGroup, backgroundLogger: logger)

        try await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                await client.run()
            }

            let value = PostgresLTree(labels: ["foo", "bar", "baz"])

            let rows = try await client.query("SELECT \(value)")

            var decoded = [PostgresLTree]()
            for try await label in rows.decode(PostgresLTree.self) {
                decoded.append(label)
            }
            XCTAssertEqual(decoded, [value])

            taskGroup.cancelAll()
        }
    }

    /// Listing a type that does not exist in `additionalDataTypeNames` does not break the connection, but
    /// it does fail any query that binds that type, with an error naming it.
    ///
    /// The connection deliberately still establishes. Failing establishment would make the pool retry the
    /// connection for `circuitBreakerTripAfter` (60s by default) and then fail the caller with
    /// `ConnectionPoolError.connectionCreationCircuitBreakerTripped`, which does not carry the real cause —
    /// a minute of waiting to be told nothing useful.
    ///
    /// Uses a bespoke type rather than `ltree` to show the mechanism is not extension specific: any type
    /// that reports a `psqlTypeName` participates.
    func testUnknownAdditionalDataTypeNameFailsQueriesBindingIt() async throws {
        struct MissingType: PostgresDynamicTypeEncodable {
            var psqlType: PostgresDataType { .null }
            var psqlFormat: PostgresFormat { .text }
            var psqlTypeName: String? { "this_type_does_not_exist" }

            func encode<JSONEncoder: PostgresJSONEncoder>(
                into byteBuffer: inout ByteBuffer,
                context: PostgresEncodingContext<JSONEncoder>
            ) {
                byteBuffer.writeString("irrelevant")
            }
        }

        var mlogger = Logger(label: "test")
        mlogger.logLevel = .debug
        let logger = mlogger
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 8)
        self.addTeardownBlock {
            try await eventLoopGroup.shutdownGracefully()
        }

        var clientConfig = PostgresClient.Configuration.makeTestConfiguration()
        clientConfig.options.additionalDataTypeNames = ["this_type_does_not_exist"]
        let client = PostgresClient(configuration: clientConfig, eventLoopGroup: eventLoopGroup, backgroundLogger: logger)

        await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                await client.run()
            }

            do {
                // A type the server does not have does not make the connection itself unusable.
                _ = try await client.query("SELECT 1")
            } catch {
                XCTFail("The connection should still work for queries that don't bind the type: \(error)")
            }

            do {
                _ = try await client.query("SELECT \(MissingType())")
                XCTFail("Expected the query to fail because the bound type does not exist")
            } catch let error as PSQLError {
                XCTAssertEqual(error.code, .unresolvedDataTypeNames)
                XCTAssertTrue(
                    String(reflecting: error).contains("this_type_does_not_exist"),
                    "The error should name the type that could not be resolved: \(String(reflecting: error))"
                )
            } catch {
                XCTFail("Unexpected error type: \(String(reflecting: error))")
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
