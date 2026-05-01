import InMemoryTracing
import Testing
import Tracing
@testable import PostgresNIO

@Suite struct PostgresTracingSupportTests {
    @Test func testUserQuerySpanDefaultsToNamespaceTargetSpanName() {
        var tracing = PostgresTracingConfiguration(isEnabled: true)
        tracing.queryTextPolicy = .safe

        let config = PostgresConnection.Configuration(
            host: "db.example.com",
            port: 5433,
            username: "trace_user",
            password: nil,
            database: "trace_db",
            tls: .disable
        )

        let metadata = PostgresTraceMetadata(
            operation: .userQuery(Self.parameterizedQuery("SELECT * FROM public.users WHERE id = $1")),
            configuration: tracing,
            connectionInfo: .init(configuration: .init(config))
        )

        #expect(metadata.spanName == "trace_db")
        #expect(metadata.spanKind == SpanKind.client)
        #expect(metadata.attributes.stringValue(for: "db.system.name") == "postgresql")
        #expect(metadata.attributes.stringValue(for: "db.system") == "postgresql")
        #expect(metadata.attributes.stringValue(for: "db.namespace") == "trace_db")
        #expect(metadata.attributes.stringValue(for: "server.address") == "db.example.com")
        #expect(metadata.attributes.intValue(for: "server.port") == 5433)
        #expect(metadata.attributes["db.operation.name"] == nil)
        #expect(metadata.attributes["db.collection.name"] == nil)
        #expect(metadata.attributes["db.query.summary"] == nil)
        #expect(metadata.attributes.stringValue(for: "db.query.text") == "SELECT * FROM public.users WHERE id = $1")
    }

    @Test func testUserQuerySpanFallsBackToNamespaceTargetWhenNoStatementMetadataExists() {
        var tracing = PostgresTracingConfiguration(isEnabled: true)
        tracing.queryTextPolicy = .recordAll

        let info = PostgresTracingConnectionInfo(configuration: .init(
            PostgresConnection.Configuration(
                host: "localhost",
                port: 5432,
                username: "postgres",
                password: nil,
                database: "postgres",
                tls: .disable
            )
        ))

        let metadata = PostgresTraceMetadata(
            operation: .userQuery(PostgresQuery(unsafeSQL: "", binds: PostgresBindings())),
            configuration: tracing,
            connectionInfo: info
        )

        #expect(metadata.spanName == "postgres")
        #expect(metadata.attributes["db.operation.name"] == nil)
        #expect(metadata.attributes["db.query.summary"] == nil)
    }

    @Test func testInferredStatementMetadataPolicyUsesSQLKeywordAndTarget() {
        let tracing = PostgresTracingConfiguration(
            isEnabled: true,
            statementMetadataPolicy: .inferred
        )
        let info = PostgresTracingConnectionInfo(configuration: .init(
            PostgresConnection.Configuration(
                host: "localhost",
                port: 5432,
                username: "postgres",
                password: nil,
                database: "postgres",
                tls: .disable
            )
        ))

        func spanName(for sql: String) -> String? {
            PostgresTraceMetadata(
                operation: .userQuery(Self.parameterizedQuery(sql)),
                configuration: tracing,
                connectionInfo: info
            ).spanName
        }

        #expect(spanName(for: #"INSERT INTO "perf_uploads" ("id") VALUES ($1)"#) == "INSERT postgres")
        #expect(spanName(for: "SELECT * FROM users WHERE id = $1") == "SELECT postgres")
        #expect(spanName(for: "UPDATE users SET name = $1") == "UPDATE postgres")
        #expect(spanName(for: "DELETE FROM orders WHERE id = $1") == "DELETE postgres")
        #expect(spanName(for: "BEGIN") == "BEGIN postgres")
        #expect(spanName(for: "") == "postgres")

        let metadata = PostgresTraceMetadata(
            operation: .userQuery(Self.parameterizedQuery("INSERT INTO t VALUES ($1)")),
            configuration: tracing,
            connectionInfo: info
        )
        #expect(metadata.attributes.stringValue(for: "db.operation.name") == "INSERT")
        #expect(metadata.attributes["db.query.summary"] == nil)
    }

    @Test func testPreparedExecutionSpanUsesInferredOperationNameAndTarget() {
        let tracing = PostgresTracingConfiguration(
            isEnabled: true,
            statementMetadataPolicy: .inferred
        )
        let info = PostgresTracingConnectionInfo(configuration: .init(
            PostgresConnection.Configuration(
                host: "localhost",
                port: 5432,
                username: "postgres",
                password: nil,
                database: "postgres",
                tls: .disable
            )
        ))

        let metadata = PostgresTraceMetadata(
            operation: .preparedExecution(
                sql: #"INSERT INTO "perf_uploads" ("id", "user_id") VALUES ($1, $2)"#,
                bindCount: 2
            ),
            configuration: tracing,
            connectionInfo: info
        )

        #expect(metadata.spanName == "INSERT postgres")
        #expect(metadata.attributes.stringValue(for: "db.operation.name") == "INSERT")
        #expect(metadata.attributes["db.query.summary"] == nil)
    }

    @Test func testCopyFromUsesExactMetadataWithoutSQLInference() {
        let tracing = PostgresTracingConfiguration(isEnabled: true)
        let info = PostgresTracingConnectionInfo(configuration: .init(
            PostgresConnection.Configuration(
                host: "localhost",
                port: 5432,
                username: "postgres",
                password: nil,
                database: "postgres",
                tls: .disable
            )
        ))

        let metadata = PostgresTraceMetadata(
            operation: .libraryQuery(
                PostgresQuery(
                    unsafeSQL: #"COPY "perf_uploads" FROM STDIN WITH (FORMAT text)"#,
                    binds: PostgresBindings()
                ),
                safeQueryText: #"COPY "perf_uploads" FROM STDIN WITH (FORMAT text)"#,
                exactSummary: .init(
                    operationName: "COPY",
                    querySummary: nil,
                    collectionName: "perf_uploads",
                    storedProcedureName: nil
                )
            ),
            configuration: tracing,
            connectionInfo: info
        )

        #expect(metadata.spanName == "COPY perf_uploads")
        #expect(metadata.attributes.stringValue(for: "db.operation.name") == "COPY")
        #expect(metadata.attributes.stringValue(for: "db.collection.name") == "perf_uploads")
    }

    @Test func testSafeQueryTextPolicyOnlyCapturesSafeSQL() {
        let safe = PostgresTracingConfiguration(isEnabled: true, queryTextPolicy: .safe)
        let info = PostgresTracingConnectionInfo(configuration: .init(
            PostgresConnection.Configuration(
                host: "localhost",
                port: 5432,
                username: "postgres",
                password: nil,
                database: "postgres",
                tls: .disable
            )
        ))

        let rawLiteral = PostgresTraceMetadata(
            operation: .userQuery(PostgresQuery(unsafeSQL: "SELECT 1", binds: PostgresBindings())),
            configuration: safe,
            connectionInfo: info
        )
        let libraryGenerated = PostgresTraceMetadata(
            operation: .libraryQuery(
                PostgresQuery(
                    unsafeSQL: #"COPY "users" FROM STDIN WITH (FORMAT text,DELIMITER U&'\0009')"#,
                    binds: PostgresBindings()
                ),
                safeQueryText: #"COPY "users" FROM STDIN WITH (FORMAT text,DELIMITER ?)"#
            ),
            configuration: safe,
            connectionInfo: info
        )
        let parameterized = PostgresTraceMetadata(
            operation: .userQuery(Self.parameterizedQuery("SELECT * FROM users WHERE id = $1")),
            configuration: safe,
            connectionInfo: info
        )
        let placeholderInLiteral = PostgresTraceMetadata(
            operation: .userQuery(PostgresQuery(unsafeSQL: #"SELECT '$1'"#, binds: PostgresBindings())),
            configuration: safe,
            connectionInfo: info
        )
        let recordAll = PostgresTraceMetadata(
            operation: .userQuery(PostgresQuery(unsafeSQL: "SELECT 1", binds: PostgresBindings())),
            configuration: .init(isEnabled: true, queryTextPolicy: .recordAll),
            connectionInfo: info
        )

        #expect(rawLiteral.attributes["db.query.text"] == nil)
        #expect(
            libraryGenerated.attributes.stringValue(for: "db.query.text")
                == #"COPY "users" FROM STDIN WITH (FORMAT text,DELIMITER ?)"#
        )
        #expect(parameterized.attributes.stringValue(for: "db.query.text") == "SELECT * FROM users WHERE id = $1")
        #expect(placeholderInLiteral.attributes["db.query.text"] == nil)
        #expect(recordAll.attributes.stringValue(for: "db.query.text") == "SELECT 1")
    }

    @Test func testDisabledStatementMetadataPolicySkipsDerivedSQLMetadata() {
        let tracing = PostgresTracingConfiguration(
            isEnabled: true,
            queryTextPolicy: .safe,
            statementMetadataPolicy: .disabled
        )
        let info = PostgresTracingConnectionInfo(configuration: .init(
            PostgresConnection.Configuration(
                host: "localhost",
                port: 5432,
                username: "postgres",
                password: nil,
                database: "postgres",
                tls: .disable
            )
        ))

        let metadata = PostgresTraceMetadata(
            operation: .userQuery(Self.parameterizedQuery("SELECT * FROM public.users WHERE id = $1")),
            configuration: tracing,
            connectionInfo: info
        )

        #expect(metadata.spanName == "postgres")
        #expect(metadata.attributes["db.operation.name"] == nil)
        #expect(metadata.attributes["db.query.summary"] == nil)
        #expect(metadata.attributes["db.collection.name"] == nil)
        #expect(metadata.attributes.stringValue(for: "db.query.text") == "SELECT * FROM public.users WHERE id = $1")
    }

    @Test func testPrepareMetadataUsesExactOperationOnlyWhenEnabled() {
        let info = PostgresTracingConnectionInfo(configuration: .init(
            PostgresConnection.Configuration(
                host: "localhost",
                port: 5432,
                username: "postgres",
                password: nil,
                database: "postgres",
                tls: .disable
            )
        ))

        let exact = PostgresTraceMetadata(
            operation: .prepare(sql: "SELECT 1"),
            configuration: .init(isEnabled: true, queryTextPolicy: .safe, statementMetadataPolicy: .exact),
            connectionInfo: info
        )
        let disabled = PostgresTraceMetadata(
            operation: .prepare(sql: "SELECT 1"),
            configuration: .init(isEnabled: true, queryTextPolicy: .safe, statementMetadataPolicy: .disabled),
            connectionInfo: info
        )

        #expect(exact.spanName == "PREPARE")
        #expect(exact.attributes.stringValue(for: "db.operation.name") == "PREPARE")
        #expect(exact.attributes.stringValue(for: "db.query.summary") == "PREPARE")
        #expect(disabled.spanName == "postgres")
        #expect(disabled.attributes["db.operation.name"] == nil)
        #expect(disabled.attributes["db.query.summary"] == nil)
    }

    @Test func testFailureRecordsSQLStateOnSpan() throws {
        let tracer = InMemoryTracer()
        var tracing = PostgresTracingConfiguration(isEnabled: true)
        tracing.tracer = tracer

        let info = PostgresTracingConnectionInfo(configuration: .init(
            PostgresConnection.Configuration(
                host: "localhost",
                port: 5432,
                username: "postgres",
                password: nil,
                database: "postgres",
                tls: .disable
            )
        ))

        let span = try #require(
            PostgresTraceOperation
                .userQuery(Self.parameterizedQuery("SELECT * FROM users WHERE id = $1"))
                .makeSpan(configuration: tracing, connectionInfo: info, parentContext: .topLevel)
        )

        span.fail(PSQLError.server(.init(fields: [.sqlState: "08P01", .message: "protocol violation"])))

        let finished = try #require(tracer.finishedSpans.first)
        #expect(finished.operationName == "postgres")
        #expect(finished.status?.code == .error)
        #expect(finished.attributes.stringValue(for: "db.response.status_code") == "08P01")
        #expect(finished.attributes.stringValue(for: "error.type") == "08P01")
        #expect(finished.errors.count == 1)
        #expect((finished.errors.first?.error as? PSQLError)?.serverInfo?[.sqlState] == "08P01")
    }

    @Test func testFailureErrorDetailsPolicyCanAttachPrimaryServerMessage() throws {
        let tracer = InMemoryTracer()
        var tracing = PostgresTracingConfiguration(isEnabled: true)
        tracing.tracer = tracer
        tracing.errorDetailsPolicy = .message

        let info = PostgresTracingConnectionInfo(configuration: .init(
            PostgresConnection.Configuration(
                host: "localhost",
                port: 5432,
                username: "postgres",
                password: nil,
                database: "postgres",
                tls: .disable
            )
        ))

        let span = try #require(
            PostgresTraceOperation
                .userQuery(Self.parameterizedQuery("SELECT * FROM users WHERE id = $1"))
                .makeSpan(configuration: tracing, connectionInfo: info, parentContext: .topLevel)
        )

        span.fail(PSQLError.server(.init(fields: [
            .sqlState: "40P01",
            .message: "deadlock detected",
            .detail: "query details",
        ])))

        let finished = try #require(tracer.finishedSpans.first)
        let recordedError = try #require(finished.errors.first)
        #expect(recordedError.attributes.stringValue(for: "exception.message") == "deadlock detected")
    }

    @Test func testFailureErrorDetailsPolicyCanAttachDebugDescription() throws {
        let tracer = InMemoryTracer()
        var tracing = PostgresTracingConfiguration(isEnabled: true)
        tracing.tracer = tracer
        tracing.errorDetailsPolicy = .debugDescription

        let info = PostgresTracingConnectionInfo(configuration: .init(
            PostgresConnection.Configuration(
                host: "localhost",
                port: 5432,
                username: "postgres",
                password: nil,
                database: "postgres",
                tls: .disable
            )
        ))

        let span = try #require(
            PostgresTraceOperation
                .userQuery(Self.parameterizedQuery("SELECT * FROM users WHERE id = $1"))
                .makeSpan(configuration: tracing, connectionInfo: info, parentContext: .topLevel)
        )

        var error = PSQLError.server(.init(fields: [
            .sqlState: "40P01",
            .message: "deadlock detected",
        ]))
        error.file = "TracingFile.swift"
        error.line = 4242
        error.query = Self.parameterizedQuery("SELECT broken WHERE id = $1")

        span.fail(error)

        let finished = try #require(tracer.finishedSpans.first)
        let recordedError = try #require(finished.errors.first)
        let exceptionMessage = try #require(recordedError.attributes.stringValue(for: "exception.message"))
        #expect(exceptionMessage.contains("PSQLError(code: server"))
        #expect(exceptionMessage.contains("triggeredFromRequestInFile: TracingFile.swift"))
        #expect(exceptionMessage.contains("query: PostgresQuery(sql: SELECT broken WHERE id = $1"))
    }

    private static func parameterizedQuery(_ sql: String) -> PostgresQuery {
        var bindings = PostgresBindings()
        bindings.appendNull()
        return PostgresQuery(unsafeSQL: sql, binds: bindings)
    }
}

private extension SpanAttributes {
    func stringValue(for key: String) -> String? {
        switch self[key]?.toSpanAttribute() {
        case .string(let value):
            return value
        case .stringConvertible(let value):
            return String(describing: value)
        default:
            return nil
        }
    }

    func intValue(for key: String) -> Int? {
        switch self[key]?.toSpanAttribute() {
        case .int32(let value):
            return Int(value)
        case .int64(let value):
            return Int(value)
        default:
            return nil
        }
    }
}
