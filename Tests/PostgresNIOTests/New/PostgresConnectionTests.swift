import NIOCore
import NIOPosix
import NIOEmbedded
import XCTest
import Logging
@testable import PostgresNIO

class PostgresConnectionTests: XCTestCase {

    let logger = Logger(label: "PostgresConnectionTests")

    func testConnectionFailure() {
        // We start a local server and close it immediately to ensure that the port
        // number we try to connect to is not used by any other process.
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        
        var tempChannel: Channel?
        XCTAssertNoThrow(tempChannel = try ServerBootstrap(group: eventLoopGroup)
                            .bind(to: .init(ipAddress: "127.0.0.1", port: 0)).wait())
        let maybePort = tempChannel?.localAddress?.port
        XCTAssertNoThrow(try tempChannel?.close().wait())
        guard let port = maybePort else {
            return XCTFail("Could not get port number from temp started server")
        }
        
        let config = PostgresConnection.Configuration(
            host: "127.0.0.1", port: port,
            username: "postgres", password: "abc123", database: "postgres",
            tls: .disable
        )
        
        var logger = Logger.psqlTest
        logger.logLevel = .trace
        
        XCTAssertThrowsError(try PostgresConnection.connect(on: eventLoopGroup.next(), configuration: config, id: 1, logger: logger).wait()) {
            XCTAssertTrue($0 is PSQLError)
        }
    }

    func testOptionsAreSentOnTheWire() async throws {
        let eventLoop = NIOAsyncTestingEventLoop()
        let channel = try await NIOAsyncTestingChannel(loop: eventLoop) { channel in
            try channel.pipeline.syncOperations.addHandlers(ReverseByteToMessageHandler(PSQLFrontendMessageDecoder()))
            try channel.pipeline.syncOperations.addHandlers(ReverseMessageToByteHandler(PSQLBackendMessageEncoder()))
        }
        try await channel.connect(to: .makeAddressResolvingHost("localhost", port: 5432))

        let configuration = {
            var config = PostgresConnection.Configuration(
                establishedChannel: channel,
                username: "username",
                password: "postgres",
                database: "database"
            )
            config.options.additionalStartupParameters = [
                ("DateStyle", "ISO, MDY"),
                ("application_name", "postgres-nio-test"),
                ("server_encoding", "UTF8"),
                ("integer_datetimes", "on"),
                ("client_encoding", "UTF8"),
                ("TimeZone", "Etc/UTC"),
                ("is_superuser", "on"),
                ("server_version", "13.1 (Debian 13.1-1.pgdg100+1)"),
                ("session_authorization", "postgres"),
                ("IntervalStyle", "postgres"),
                ("standard_conforming_strings", "on")
            ]
            return config
        }()

        async let connectionPromise = PostgresConnection.connect(on: eventLoop, configuration: configuration, id: 1, logger: .psqlTest)
        let message = try await channel.waitForOutboundWrite(as: PostgresFrontendMessage.self)
        XCTAssertEqual(message, .startup(.versionThree(parameters: .init(user: "username", database: "database", options: configuration.options.additionalStartupParameters, replication: .false))))
        try await channel.writeInbound(PostgresBackendMessage.authentication(.ok))
        try await channel.writeInbound(PostgresBackendMessage.backendKeyData(.init(processID: 1234, secretKey: 5678)))
        try await channel.writeInbound(PostgresBackendMessage.readyForQuery(.idle))

        let connection = try await connectionPromise
        try await connection.close()
    }

    func testSimpleListen() async throws {
        let (connection, channel) = try await self.makeTestConnectionWithAsyncTestingChannel()

        try await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                let events = try await connection.listen("foo")
                for try await event in events {
                    XCTAssertEqual(event.payload, "wooohooo")
                    break
                }
            }

            let listenMessage = try await channel.waitForUnpreparedRequest()
            XCTAssertEqual(listenMessage.parse.query, #"LISTEN "foo";"#)

            try await channel.sendUnpreparedRequestWithNoParametersBindResponse()
            try await channel.writeInbound(PostgresBackendMessage.commandComplete("LISTEN"))
            try await channel.writeInbound(PostgresBackendMessage.readyForQuery(.idle))

            try await channel.writeInbound(PostgresBackendMessage.notification(.init(backendPID: 12, channel: "foo", payload: "wooohooo")))

            let unlistenMessage = try await channel.waitForUnpreparedRequest()
            XCTAssertEqual(unlistenMessage.parse.query, #"UNLISTEN "foo";"#)

            try await channel.sendUnpreparedRequestWithNoParametersBindResponse()
            try await channel.writeInbound(PostgresBackendMessage.commandComplete("UNLISTEN"))
            try await channel.writeInbound(PostgresBackendMessage.readyForQuery(.idle))

            switch await taskGroup.nextResult()! {
            case .success:
                break
            case .failure(let failure):
                XCTFail("Unexpected error: \(failure)")
            }
        }
    }

    func testSimpleListenDoesNotUnlistenIfThereIsAnotherSubscriber() async throws {
        let (connection, channel) = try await self.makeTestConnectionWithAsyncTestingChannel()

        try await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                let events = try await connection.listen("foo")
                for try await event in events {
                    XCTAssertEqual(event.payload, "wooohooo")
                    break
                }
            }

            taskGroup.addTask {
                let events = try await connection.listen("foo")
                var counter = 0
                loop: for try await event in events {
                    defer { counter += 1 }
                    switch counter {
                    case 0:
                        XCTAssertEqual(event.payload, "wooohooo")
                    case 1:
                        XCTAssertEqual(event.payload, "wooohooo2")
                        break loop
                    default:
                        XCTFail("Unexpected message: \(event)")
                    }
                }
            }

            let listenMessage = try await channel.waitForUnpreparedRequest()
            XCTAssertEqual(listenMessage.parse.query, #"LISTEN "foo";"#)

            try await channel.sendUnpreparedRequestWithNoParametersBindResponse()
            try await channel.writeInbound(PostgresBackendMessage.commandComplete("LISTEN"))
            try await channel.writeInbound(PostgresBackendMessage.readyForQuery(.idle))

            try await channel.writeInbound(PostgresBackendMessage.notification(.init(backendPID: 12, channel: "foo", payload: "wooohooo")))
            try await channel.writeInbound(PostgresBackendMessage.notification(.init(backendPID: 12, channel: "foo", payload: "wooohooo2")))

            let unlistenMessage = try await channel.waitForUnpreparedRequest()
            XCTAssertEqual(unlistenMessage.parse.query, #"UNLISTEN "foo";"#)

            try await channel.sendUnpreparedRequestWithNoParametersBindResponse()
            try await channel.writeInbound(PostgresBackendMessage.commandComplete("UNLISTEN"))
            try await channel.writeInbound(PostgresBackendMessage.readyForQuery(.idle))

            switch await taskGroup.nextResult()! {
            case .success:
                break
            case .failure(let failure):
                XCTFail("Unexpected error: \(failure)")
            }
        }
    }

    func testSimpleListenConnectionDrops() async throws {
        let (connection, channel) = try await self.makeTestConnectionWithAsyncTestingChannel()

        try await withThrowingTaskGroup(of: Void.self) { [logger] taskGroup in
            taskGroup.addTask {
                let events = try await connection.listen("foo")
                var iterator = events.makeAsyncIterator()
                let first = try await iterator.next()
                XCTAssertEqual(first?.payload, "wooohooo")
                do {
                    _ = try await iterator.next()
                    XCTFail("Did not expect to not throw")
                } catch {
                    logger.error("error", metadata: ["error": "\(error)"])
                }
            }

            let listenMessage = try await channel.waitForUnpreparedRequest()
            XCTAssertEqual(listenMessage.parse.query, #"LISTEN "foo";"#)

            try await channel.sendUnpreparedRequestWithNoParametersBindResponse()
            try await channel.writeInbound(PostgresBackendMessage.commandComplete("LISTEN"))
            try await channel.writeInbound(PostgresBackendMessage.readyForQuery(.idle))

            try await channel.writeInbound(PostgresBackendMessage.notification(.init(backendPID: 12, channel: "foo", payload: "wooohooo")))
            struct MyWeirdError: Error {}
            channel.pipeline.fireErrorCaught(MyWeirdError())

            switch await taskGroup.nextResult()! {
            case .success:
                break
            case .failure(let failure):
                XCTFail("Unexpected error: \(failure)")
            }
        }
    }

    func testCloseGracefullyClosesWhenInternalQueueIsEmpty() async throws {
        let (connection, channel) = try await self.makeTestConnectionWithAsyncTestingChannel()
        try await withThrowingTaskGroup(of: Void.self) { [logger] taskGroup async throws -> () in
            for _ in 1...2 {
                taskGroup.addTask {
                    let rows = try await connection.query("SELECT 1;", logger: logger)
                    var iterator = rows.decode(Int.self).makeAsyncIterator()
                    let first = try await iterator.next()
                    XCTAssertEqual(first, 1)
                    let second = try await iterator.next()
                    XCTAssertNil(second)
                }
            }

            for i in 0...1 {
                let listenMessage = try await channel.waitForUnpreparedRequest()
                XCTAssertEqual(listenMessage.parse.query, "SELECT 1;")

                if i == 0 {
                    taskGroup.addTask {
                        try await connection.closeGracefully()
                    }
                }

                try await channel.writeInbound(PostgresBackendMessage.parseComplete)
                try await channel.writeInbound(PostgresBackendMessage.parameterDescription(.init(dataTypes: [])))
                let intDescription = RowDescription.Column(
                    name: "",
                    tableOID: 0,
                    columnAttributeNumber: 0,
                    dataType: .int8, dataTypeSize: 8, dataTypeModifier: 0, format: .binary
                )
                try await channel.writeInbound(PostgresBackendMessage.rowDescription(.init(columns: [intDescription])))
                try await channel.testingEventLoop.executeInContext { channel.read() }
                try await channel.writeInbound(PostgresBackendMessage.bindComplete)
                try await channel.testingEventLoop.executeInContext { channel.read() }
                try await channel.writeInbound(PostgresBackendMessage.dataRow([Int(1)]))
                try await channel.testingEventLoop.executeInContext { channel.read() }
                try await channel.writeInbound(PostgresBackendMessage.commandComplete("SELECT 1 1"))
                try await channel.testingEventLoop.executeInContext { channel.read() }
                try await channel.writeInbound(PostgresBackendMessage.readyForQuery(.idle))
            }

            let terminate = try await channel.waitForOutboundWrite(as: PostgresFrontendMessage.self)
            XCTAssertEqual(terminate, .terminate)
            try await channel.closeFuture.get()
            XCTAssertEqual(channel.isActive, false)

            while let taskResult = await taskGroup.nextResult() {
                switch taskResult {
                case .success:
                    break
                case .failure(let failure):
                    XCTFail("Unexpected error: \(failure)")
                }
            }
        }
    }

    func testCloseClosesImmediately() async throws {
        let (connection, channel) = try await self.makeTestConnectionWithAsyncTestingChannel()

        try await withThrowingTaskGroup(of: Void.self) { [logger] taskGroup async throws -> () in
            for _ in 1...2 {
                taskGroup.addTask {
                    try await connection.query("SELECT 1;", logger: logger)
                }
            }

            let listenMessage = try await channel.waitForUnpreparedRequest()
            XCTAssertEqual(listenMessage.parse.query, "SELECT 1;")

            async let close: () = connection.close()

            try await channel.closeFuture.get()
            XCTAssertEqual(channel.isActive, false)

            try await close

            while let taskResult = await taskGroup.nextResult() {
                switch taskResult {
                case .success:
                    XCTFail("Expected queries to fail")
                case .failure(let failure):
                    guard let error = failure as? PSQLError else {
                        return XCTFail("Unexpected error type: \(failure)")
                    }
                    XCTAssertEqual(error.code, .clientClosedConnection)
                }
            }
        }
    }

    func testIfServerJustClosesTheErrorReflectsThat() async throws {
        let (connection, channel) = try await self.makeTestConnectionWithAsyncTestingChannel()
        let logger = self.logger

        async let response = try await connection.query("SELECT 1;", logger: logger)

        let listenMessage = try await channel.waitForUnpreparedRequest()
        XCTAssertEqual(listenMessage.parse.query, "SELECT 1;")

        try await channel.testingEventLoop.executeInContext { channel.pipeline.fireChannelInactive() }
        try await channel.testingEventLoop.executeInContext { channel.pipeline.fireChannelUnregistered() }

        do {
            _ = try await response
            XCTFail("Expected to throw")
        } catch {
            XCTAssertEqual((error as? PSQLError)?.code, .serverClosedConnection)
        }

        // retry on same connection

        do {
            _ = try await connection.query("SELECT 1;", logger: self.logger)
            XCTFail("Expected to throw")
        } catch {
            XCTAssertEqual((error as? PSQLError)?.code, .serverClosedConnection)
        }
    }

    struct TestPrepareStatement: PostgresPreparedStatement {
        static let sql = "SELECT datname FROM pg_stat_activity WHERE state = $1"
        typealias Row = String

        var state: String

        func makeBindings() -> PostgresBindings {
            var bindings = PostgresBindings()
            bindings.append(.init(string: self.state))
            return bindings
        }

        func decodeRow(_ row: PostgresNIO.PostgresRow) throws -> Row {
            try row.decode(Row.self)
        }
    }

    func testPreparedStatement() async throws {
        let (connection, channel) = try await self.makeTestConnectionWithAsyncTestingChannel()

        try await withThrowingTaskGroup(of: Void.self) { taskGroup async throws -> () in
            taskGroup.addTask {
                let preparedStatement = TestPrepareStatement(state: "active")
                let result = try await connection.execute(preparedStatement, logger: .psqlTest)
                var rows = 0
                for try await database in result {
                    rows += 1
                    XCTAssertEqual("test_database", database)
                }
                XCTAssertEqual(rows, 1)
            }

            let prepareRequest = try await channel.waitForPrepareRequest()
            XCTAssertEqual(prepareRequest.parse.query, "SELECT datname FROM pg_stat_activity WHERE state = $1")
            XCTAssertEqual(prepareRequest.parse.parameters.first, .text)
            guard case .preparedStatement(let name) = prepareRequest.describe else {
                fatalError("Describe should contain a prepared statement")
            }
            XCTAssertEqual(name, String(reflecting: TestPrepareStatement.self))

            try await channel.sendPrepareResponse(
                parameterDescription: .init(dataTypes: [
                    PostgresDataType.text
                ]),
                rowDescription: .init(columns: [
                    .init(
                        name: "datname",
                        tableOID: 12222,
                        columnAttributeNumber: 2,
                        dataType: .name,
                        dataTypeSize: 64,
                        dataTypeModifier: -1,
                        format: .text
                    )
                ])
            )

            let preparedRequest = try await channel.waitForPreparedRequest()
            XCTAssertEqual(preparedRequest.bind.preparedStatementName, String(reflecting: TestPrepareStatement.self))
            XCTAssertEqual(preparedRequest.bind.parameters.count, 1)
            XCTAssertEqual(preparedRequest.bind.resultColumnFormats, [.binary])

            try await channel.sendPreparedResponse(
                dataRows: [
                    ["test_database"]
                ],
                commandTag: TestPrepareStatement.sql
            )
        }
    }

    func testWeDontCrashOnUnexpectedChannelEvents() async throws {
        let (connection, channel) = try await self.makeTestConnectionWithAsyncTestingChannel()

        enum MyEvent {
            case pleaseDontCrash
        }
        channel.pipeline.fireUserInboundEventTriggered(MyEvent.pleaseDontCrash)
        try await connection.close()
    }

    func testSerialExecutionOfSamePreparedStatement() async throws {
        let (connection, channel) = try await self.makeTestConnectionWithAsyncTestingChannel()

        try await withThrowingTaskGroup(of: Void.self) { taskGroup async throws -> () in
            // Send the same prepared statement twice, but with different parameters.
            // Send one first and wait to send the other request until preparation is complete
            taskGroup.addTask {
                let preparedStatement = TestPrepareStatement(state: "active")
                let result = try await connection.execute(preparedStatement, logger: .psqlTest)
                var rows = 0
                for try await database in result {
                    rows += 1
                    XCTAssertEqual("test_database", database)
                }
                XCTAssertEqual(rows, 1)
            }

            let prepareRequest = try await channel.waitForPrepareRequest()
            XCTAssertEqual(prepareRequest.parse.query, "SELECT datname FROM pg_stat_activity WHERE state = $1")
            XCTAssertEqual(prepareRequest.parse.parameters.first, .text)
            guard case .preparedStatement(let name) = prepareRequest.describe else {
                fatalError("Describe should contain a prepared statement")
            }
            XCTAssertEqual(name, String(reflecting: TestPrepareStatement.self))

            try await channel.sendPrepareResponse(
                parameterDescription: .init(dataTypes: [
                    PostgresDataType.text
                ]),
                rowDescription: .init(columns: [
                    .init(
                        name: "datname",
                        tableOID: 12222,
                        columnAttributeNumber: 2,
                        dataType: .name,
                        dataTypeSize: 64,
                        dataTypeModifier: -1,
                        format: .text
                    )
                ])
            )

            let preparedRequest1 = try await channel.waitForPreparedRequest()
            var buffer = preparedRequest1.bind.parameters[0]!
            let parameter1 = buffer.readString(length: buffer.readableBytes)!
            XCTAssertEqual(parameter1, "active")
            try await channel.sendPreparedResponse(
                dataRows: [
                    ["test_database"]
                ],
                commandTag: TestPrepareStatement.sql
            )

            // Now that the statement has been prepared and executed, send another request that will only get executed
            // without preparation
            taskGroup.addTask {
                let preparedStatement = TestPrepareStatement(state: "idle")
                let result = try await connection.execute(preparedStatement, logger: .psqlTest)
                var rows = 0
                for try await database in result {
                    rows += 1
                    XCTAssertEqual("test_database", database)
                }
                XCTAssertEqual(rows, 1)
            }

            let preparedRequest2 = try await channel.waitForPreparedRequest()
            buffer = preparedRequest2.bind.parameters[0]!
            let parameter2 = buffer.readString(length: buffer.readableBytes)!
            XCTAssertEqual(parameter2, "idle")
            try await channel.sendPreparedResponse(
                dataRows: [
                    ["test_database"]
                ],
                commandTag: TestPrepareStatement.sql
            )
            // Ensure we received and responded to both the requests
            let parameters = [parameter1, parameter2]
            XCTAssert(parameters.contains("active"))
            XCTAssert(parameters.contains("idle"))
        }
    }

    func testStatementPreparationOnlyHappensOnceWithConcurrentRequests() async throws {
        let (connection, channel) = try await self.makeTestConnectionWithAsyncTestingChannel()

        try await withThrowingTaskGroup(of: Void.self) { taskGroup async throws -> () in
            // Send the same prepared statement twice, but with different parameters.
            // Let them race to tests that requests and responses aren't mixed up
            taskGroup.addTask {
                let preparedStatement = TestPrepareStatement(state: "active")
                let result = try await connection.execute(preparedStatement, logger: .psqlTest)
                var rows = 0
                for try await database in result {
                    rows += 1
                    XCTAssertEqual("test_database_active", database)
                }
                XCTAssertEqual(rows, 1)
            }
            taskGroup.addTask {
                let preparedStatement = TestPrepareStatement(state: "idle")
                let result = try await connection.execute(preparedStatement, logger: .psqlTest)
                var rows = 0
                for try await database in result {
                    rows += 1
                    XCTAssertEqual("test_database_idle", database)
                }
                XCTAssertEqual(rows, 1)
            }

            // The channel deduplicates prepare requests, we're going to see only one of them
            let prepareRequest = try await channel.waitForPrepareRequest()
            XCTAssertEqual(prepareRequest.parse.query, "SELECT datname FROM pg_stat_activity WHERE state = $1")
            XCTAssertEqual(prepareRequest.parse.parameters.first, .text)
            guard case .preparedStatement(let name) = prepareRequest.describe else {
                fatalError("Describe should contain a prepared statement")
            }
            XCTAssertEqual(name, String(reflecting: TestPrepareStatement.self))

            try await channel.sendPrepareResponse(
                parameterDescription: .init(dataTypes: [
                    PostgresDataType.text
                ]),
                rowDescription: .init(columns: [
                    .init(
                        name: "datname",
                        tableOID: 12222,
                        columnAttributeNumber: 2,
                        dataType: .name,
                        dataTypeSize: 64,
                        dataTypeModifier: -1,
                        format: .text
                    )
                ])
            )

            // Now both the tasks have their statements prepared.
            // We should see both of their execute requests coming in, the order is nondeterministic
            let preparedRequest1 = try await channel.waitForPreparedRequest()
            var buffer = preparedRequest1.bind.parameters[0]!
            let parameter1 = buffer.readString(length: buffer.readableBytes)!
            try await channel.sendPreparedResponse(
                dataRows: [
                    ["test_database_\(parameter1)"]
                ],
                commandTag: TestPrepareStatement.sql
            )
            let preparedRequest2 = try await channel.waitForPreparedRequest()
            buffer = preparedRequest2.bind.parameters[0]!
            let parameter2 = buffer.readString(length: buffer.readableBytes)!
            try await channel.sendPreparedResponse(
                dataRows: [
                    ["test_database_\(parameter2)"]
                ],
                commandTag: TestPrepareStatement.sql
            )
            // Ensure we received and responded to both the requests
            let parameters = [parameter1, parameter2]
            XCTAssert(parameters.contains("active"))
            XCTAssert(parameters.contains("idle"))
        }
    }

    func testStatementPreparationFailure() async throws {
        let (connection, channel) = try await self.makeTestConnectionWithAsyncTestingChannel()

        try await withThrowingTaskGroup(of: Void.self) { taskGroup async throws -> () in
            // Send the same prepared statement twice, but with different parameters.
            // Send one first and wait to send the other request until preparation is complete
            taskGroup.addTask {
                let preparedStatement = TestPrepareStatement(state: "active")
                do {
                    _ = try await connection.execute(preparedStatement, logger: .psqlTest)
                    XCTFail("Was supposed to fail")
                } catch {
                    XCTAssert(error is PSQLError)
                }
            }

            let prepareRequest = try await channel.waitForPrepareRequest()
            XCTAssertEqual(prepareRequest.parse.query, "SELECT datname FROM pg_stat_activity WHERE state = $1")
            XCTAssertEqual(prepareRequest.parse.parameters.first, .text)
            guard case .preparedStatement(let name) = prepareRequest.describe else {
                fatalError("Describe should contain a prepared statement")
            }
            XCTAssertEqual(name, String(reflecting: TestPrepareStatement.self))
            
            // Respond with an error taking care to return a SQLSTATE that isn't
            // going to get the connection closed.
            try await channel.writeInbound(PostgresBackendMessage.error(.init(fields: [
                .sqlState : "26000" // invalid_sql_statement_name
            ])))
            try await channel.testingEventLoop.executeInContext { channel.read() }
            try await channel.writeInbound(PostgresBackendMessage.readyForQuery(.idle))
            try await channel.testingEventLoop.executeInContext { channel.read() }


            // Send another requests with the same prepared statement, which should fail straight
            // away without any interaction with the server
            taskGroup.addTask {
                let preparedStatement = TestPrepareStatement(state: "idle")
                do {
                    _ = try await connection.execute(preparedStatement, logger: .psqlTest)
                    XCTFail("Was supposed to fail")
                } catch {
                    XCTAssert(error is PSQLError)
                }
            }
        }
    }

    func testCopyDataSucceeds() async throws {
        let (connection, channel) = try await self.makeTestConnectionWithAsyncTestingChannel()

        try await withThrowingTaskGroup(of: Void.self) { taskGroup async throws -> () in
            taskGroup.addTask {
                try await connection.copyFrom(table: "copy_table", logger: .psqlTest) { writer in
                    try await writer.write(ByteBuffer(staticString: "1\tAlice\n"))
                }
            }

            let copyMessage = try await channel.waitForUnpreparedRequest()
            XCTAssertEqual(copyMessage.parse.query, "COPY copy_table FROM STDIN")
            XCTAssertEqual(copyMessage.bind.parameters, [])
            try await channel.sendUnpreparedRequestWithNoParametersBindResponse()
            try await channel.sendCopyInResponseForTwoTextualColumns()
            let data = try await channel.waitForCopyData()
            XCTAssertEqual(String(buffer: data.data), "1\tAlice\n")
            XCTAssertEqual(data.result, .done)
            try await channel.writeInbound(PostgresBackendMessage.commandComplete("COPY 1"))
            try await channel.writeInbound(PostgresBackendMessage.readyForQuery(.idle))
        }
    }

    func testCopyDataWriterFails() async throws {
        struct MyError: Error, CustomStringConvertible {
            var description: String { "My error" }
        }
        
        let (connection, channel) = try await self.makeTestConnectionWithAsyncTestingChannel()

        try await withThrowingTaskGroup(of: Void.self) { taskGroup async throws -> () in
            taskGroup.addTask {
                await assertThrowsError(
                    try await connection.copyFrom(table: "copy_table", logger: .psqlTest) { writer in
                        throw MyError()
                    }
                ) { error in 
                    XCTAssert(error is MyError, "Expected error of type MyError, got \(error)")
                }
            }

            _ = try await channel.waitForUnpreparedRequest()
            try await channel.sendUnpreparedRequestWithNoParametersBindResponse()
            try await channel.sendCopyInResponseForTwoTextualColumns()
            let data = try await channel.waitForCopyData()
            XCTAssertEqual(data.result, .failed(message: "My error"))
            try await channel.writeInbound(PostgresBackendMessage.error(.init(fields: [
                .message: "COPY from stdin failed: My error",
                .sqlState : "57014" // query_canceled
            ])))

            // Ensure we get a `sync` message so that the backend transitions out of copy mode.
            let syncMessage = try await channel.waitForOutboundWrite(as: PostgresFrontendMessage.self)
            XCTAssertEqual(syncMessage, .sync)
            try await channel.writeInbound(PostgresBackendMessage.readyForQuery(.idle))
        }
    }

    func testBackendSendsErrorDuringCopy() async throws {
        struct MyError: Error, CustomStringConvertible {
            var description: String { "My error" }
        }
        
        let (connection, channel) = try await self.makeTestConnectionWithAsyncTestingChannel()

        try await withThrowingTaskGroup(of: Void.self) { taskGroup async throws -> () in
            taskGroup.addTask {
                await assertThrowsError(
                    try await connection.copyFrom(table: "copy_table", logger: .psqlTest) { writer in
                        try await writer.write(ByteBuffer(staticString: "1Alice\n"))
                    }
                ) { error in
                    XCTAssertEqual((error as? PSQLError)?.serverInfo?.underlying.fields[.sqlState], "22P02")
                }
            }

            _ = try await channel.waitForUnpreparedRequest()
            try await channel.sendUnpreparedRequestWithNoParametersBindResponse()
            try await channel.sendCopyInResponseForTwoTextualColumns()
            _ = try await channel.waitForCopyData()
            try await channel.writeInbound(PostgresBackendMessage.error(.init(fields: [
                .message: #"invalid input syntax for type integer: "1Alice""#,
                .sqlState : "22P02" // invalid_text_representation
            ])))
            
            // Ensure we get a `sync` message so that the backend transitions out of copy mode.
            let syncMessage = try await channel.waitForOutboundWrite(as: PostgresFrontendMessage.self)
            XCTAssertEqual(syncMessage, .sync)
            try await channel.writeInbound(PostgresBackendMessage.readyForQuery(.idle))
        }
    }


    func testBackendSendsErrorDuringCopyBeforeCopyDone() async throws {
        let (connection, channel) = try await self.makeTestConnectionWithAsyncTestingChannel()

        let backendDidSendErrorExpectation = self.expectation(description: "Backend did send error")

        try await withThrowingTaskGroup(of: Void.self) { taskGroup async throws -> () in
            taskGroup.addTask {
                await assertThrowsError(
                    try await connection.copyFrom(table: "copy_table", logger: .psqlTest) { writer in
                        try await writer.write(ByteBuffer(staticString: "1Alice\n"))
                        channel.flush()
                        _ = await XCTWaiter.fulfillment(of: [backendDidSendErrorExpectation])
                    }
                ) { error in
                    XCTAssertEqual((error as? PSQLError)?.serverInfo?.underlying.fields[.sqlState], "22P02")
                }
            }

            _ = try await channel.waitForUnpreparedRequest()
            try await channel.sendUnpreparedRequestWithNoParametersBindResponse()
            try await channel.sendCopyInResponseForTwoTextualColumns()
            
            let copyDataMessage = try await channel.waitForOutboundWrite(as: PostgresFrontendMessage.self)
            XCTAssertEqual(copyDataMessage, .copyData(PostgresFrontendMessage.CopyData(data: ByteBuffer(staticString: "1Alice\n"))))
            
            try await channel.writeInbound(PostgresBackendMessage.error(.init(fields: [
                .message: #"invalid input syntax for type integer: "1Alice""#,
                .sqlState : "22P02" // invalid_text_representation
            ])))
            backendDidSendErrorExpectation.fulfill()
            
            // We don't expect to receive a CopyDone or CopyFail message if the server sent us an error.

            // Ensure we get a `sync` message so that the backend transitions out of copy mode.
            let syncMessage = try await channel.waitForOutboundWrite(as: PostgresFrontendMessage.self)
            XCTAssertEqual(syncMessage, .sync)
            try await channel.writeInbound(PostgresBackendMessage.readyForQuery(.idle))
        }
    }

    func testWriteDataClosureTerminatesWhenServerThrowsError() async throws {
        let (connection, channel) = try await self.makeTestConnectionWithAsyncTestingChannel()

        let expectation = self.expectation(description: "Backend sent error")

        try await withThrowingTaskGroup(of: Void.self) { taskGroup async throws -> () in
            taskGroup.addTask {
                await assertThrowsError(
                    try await connection.copyFrom(table: "copy_table", logger: .psqlTest) { writer in
                        try await writer.write(ByteBuffer(staticString: "1Alice\n"))
                        channel.flush()
                        _ = await XCTWaiter.fulfillment(of: [expectation])
                        do {
                            try await writer.write(ByteBuffer(staticString: "2\tBob\n"))
                            XCTFail("Expected error to be thrown")
                        } catch {
                            XCTAssert(error is PostgresCopyFromWriter.CopyCancellationError, "Received unexpected error: \(error)")
                            throw error
                        }
                    }
                ) { error in
                    XCTAssertEqual((error as? PSQLError)?.serverInfo?.underlying.fields[.sqlState], "22P02")
                }
            }

            _ = try await channel.waitForUnpreparedRequest()
            try await channel.sendUnpreparedRequestWithNoParametersBindResponse()
            try await channel.sendCopyInResponseForTwoTextualColumns()
            
            let dataMessage = try await channel.waitForOutboundWrite(as: PostgresFrontendMessage.self)
            XCTAssertEqual(dataMessage, .copyData(PostgresFrontendMessage.CopyData(data: ByteBuffer(staticString: "1Alice\n"))))
            
            try await channel.writeInbound(PostgresBackendMessage.error(.init(fields: [
                .message: #"invalid input syntax for type integer: "1Alice""#,
                .sqlState : "22P02" // invalid_text_representation
            ])))
            expectation.fulfill()
            
            // We don't expect to receive a CopyDone or CopyFail message if the server sent us an error.

            // Ensure we get a `sync` message so that the backend transitions out of copy mode.
            let syncMessage = try await channel.waitForOutboundWrite(as: PostgresFrontendMessage.self)
            XCTAssertEqual(syncMessage, .sync)
            try await channel.writeInbound(PostgresBackendMessage.readyForQuery(.idle))
        }
    }

    func testCopyDataWithOptions() async throws {
        let (connection, channel) = try await self.makeTestConnectionWithAsyncTestingChannel()

        try await withThrowingTaskGroup(of: Void.self) { taskGroup async throws -> () in
            taskGroup.addTask {
                try await connection.copyFrom(table: "copy_table", columns: ["id", "name"], options: CopyFromOptions(delimiter: ","), logger: .psqlTest) { writer in
                    try await writer.write(ByteBuffer(staticString: "1,Alice\n"))
                }
            }

            let copyMessage = try await channel.waitForUnpreparedRequest()
            XCTAssertEqual(copyMessage.parse.query, "COPY copy_table(id,name) FROM STDIN WITH (DELIMITER ',')")
            XCTAssertEqual(copyMessage.bind.parameters, [])
            try await channel.sendUnpreparedRequestWithNoParametersBindResponse()
            try await channel.sendCopyInResponseForTwoTextualColumns()
            let data = try await channel.waitForCopyData()
            XCTAssertEqual(String(buffer: data.data), "1,Alice\n")
            XCTAssertEqual(data.result, .done)
            try await channel.writeInbound(PostgresBackendMessage.commandComplete("COPY 1"))
            try await channel.writeInbound(PostgresBackendMessage.readyForQuery(.idle))
        }
    }

    func makeTestConnectionWithAsyncTestingChannel() async throws -> (PostgresConnection, NIOAsyncTestingChannel) {
        let eventLoop = NIOAsyncTestingEventLoop()
        let channel = try await NIOAsyncTestingChannel(loop: eventLoop) { channel in
            try channel.pipeline.syncOperations.addHandlers(ReverseByteToMessageHandler(PSQLFrontendMessageDecoder()))
            try channel.pipeline.syncOperations.addHandlers(ReverseMessageToByteHandler(PSQLBackendMessageEncoder()))
        }
        try await channel.connect(to: .makeAddressResolvingHost("localhost", port: 5432))

        let configuration = PostgresConnection.Configuration(
            establishedChannel: channel,
            username: "username",
            password: "postgres",
            database: "database"
        )

        let logger = self.logger
        async let connectionPromise = PostgresConnection.connect(on: eventLoop, configuration: configuration, id: 1, logger: logger)
        let message = try await channel.waitForOutboundWrite(as: PostgresFrontendMessage.self)
        XCTAssertEqual(message, .startup(.versionThree(parameters: .init(user: "username", database: "database", options: [], replication: .false))))
        try await channel.writeInbound(PostgresBackendMessage.authentication(.ok))
        try await channel.writeInbound(PostgresBackendMessage.backendKeyData(.init(processID: 1234, secretKey: 5678)))
        try await channel.writeInbound(PostgresBackendMessage.readyForQuery(.idle))

        let connection = try await connectionPromise

        self.addTeardownBlock {
            try await connection.close()
        }

        return (connection, channel)
    }
}

extension NIOAsyncTestingChannel {

    func waitForUnpreparedRequest() async throws -> UnpreparedRequest {
        let parse = try await self.waitForOutboundWrite(as: PostgresFrontendMessage.self)
        let describe = try await self.waitForOutboundWrite(as: PostgresFrontendMessage.self)
        let bind = try await self.waitForOutboundWrite(as: PostgresFrontendMessage.self)
        let execute = try await self.waitForOutboundWrite(as: PostgresFrontendMessage.self)
        let sync = try await self.waitForOutboundWrite(as: PostgresFrontendMessage.self)

        guard case .parse(let parse) = parse,
              case .describe(let describe) = describe,
              case .bind(let bind) = bind,
              case .execute(let execute) = execute,
              case .sync = sync
        else {
            fatalError()
        }

        return UnpreparedRequest(parse: parse, describe: describe, bind: bind, execute: execute)
    }

    struct CopyDataRequest {
        enum Result: Equatable {
            /// The data copy finished successfully with a `CopyDone` message.
            case done
            /// The data copy finished with a `CopyFail` message containing the following error message.
            case failed(message: String)
        }

        /// The data that was transferred.
        var data: ByteBuffer

        /// The `CopyDone` or `CopyFail` message that finalized the data transfer.
        var result: Result
    }

    func waitForCopyData() async throws -> CopyDataRequest {
        var copiedData = ByteBuffer()
        while true {
            let message = try await self.waitForOutboundWrite(as: PostgresFrontendMessage.self)
            switch message {
            case .copyData(let data): 
                copiedData.writeImmutableBuffer(data.data)
            case .copyDone: 
                return CopyDataRequest(data: copiedData, result: .done)
            case .copyFail(let message):
                return CopyDataRequest(data: copiedData, result: .failed(message: message.message))
            default: 
                fatalError("Unexpected message")
            }
        }
    }

    func waitForPrepareRequest() async throws -> PrepareRequest {
        let parse = try await self.waitForOutboundWrite(as: PostgresFrontendMessage.self)
        let describe = try await self.waitForOutboundWrite(as: PostgresFrontendMessage.self)
        let sync = try await self.waitForOutboundWrite(as: PostgresFrontendMessage.self)

        guard case .parse(let parse) = parse,
              case .describe(let describe) = describe,
              case .sync = sync
        else {
            fatalError("Unexpected message")
        }

        return PrepareRequest(parse: parse, describe: describe)
    }

    func sendPrepareResponse(
        parameterDescription: PostgresBackendMessage.ParameterDescription,
        rowDescription: RowDescription
    ) async throws {
        try await self.writeInbound(PostgresBackendMessage.parseComplete)
        try await self.testingEventLoop.executeInContext { self.read() }
        try await self.writeInbound(PostgresBackendMessage.parameterDescription(parameterDescription))
        try await self.testingEventLoop.executeInContext { self.read() }
        try await self.writeInbound(PostgresBackendMessage.rowDescription(rowDescription))
        try await self.testingEventLoop.executeInContext { self.read() }
        try await self.writeInbound(PostgresBackendMessage.readyForQuery(.idle))
        try await self.testingEventLoop.executeInContext { self.read() }
    }

    func waitForPreparedRequest() async throws -> PreparedRequest {
        let bind = try await self.waitForOutboundWrite(as: PostgresFrontendMessage.self)
        let execute = try await self.waitForOutboundWrite(as: PostgresFrontendMessage.self)
        let sync = try await self.waitForOutboundWrite(as: PostgresFrontendMessage.self)

        guard case .bind(let bind) = bind,
              case .execute(let execute) = execute,
              case .sync = sync
        else {
            fatalError()
        }

        return PreparedRequest(bind: bind, execute: execute)
    }

    func sendPreparedResponse(
        dataRows: [DataRow],
        commandTag: String
    ) async throws {
        try await self.writeInbound(PostgresBackendMessage.bindComplete)
        try await self.testingEventLoop.executeInContext { self.read() }
        for dataRow in dataRows {
            try await self.writeInbound(PostgresBackendMessage.dataRow(dataRow))
        }
        try await self.testingEventLoop.executeInContext { self.read() }
        try await self.writeInbound(PostgresBackendMessage.commandComplete(commandTag))
        try await self.testingEventLoop.executeInContext { self.read() }
        try await self.writeInbound(PostgresBackendMessage.readyForQuery(.idle))
        try await self.testingEventLoop.executeInContext { self.read() }
    }

    /// Send the messages up to `BindComplete` for an unnamed query that does not bind any parameters.
    func sendUnpreparedRequestWithNoParametersBindResponse() async throws {
        try await writeInbound(PostgresBackendMessage.parseComplete)
        try await writeInbound(PostgresBackendMessage.parameterDescription(.init(dataTypes: [])))
        try await writeInbound(PostgresBackendMessage.noData)
        try await writeInbound(PostgresBackendMessage.bindComplete)
    }

    func sendCopyInResponseForTwoTextualColumns() async throws {
        try await writeInbound(PostgresBackendMessage.copyInResponse(.init(format: .textual, columnFormats: [.textual, .textual])))
    }
}

struct UnpreparedRequest {
    var parse: PostgresFrontendMessage.Parse
    var describe: PostgresFrontendMessage.Describe
    var bind: PostgresFrontendMessage.Bind
    var execute: PostgresFrontendMessage.Execute
}

struct PrepareRequest {
    var parse: PostgresFrontendMessage.Parse
    var describe: PostgresFrontendMessage.Describe
}

struct PreparedRequest {
    var bind: PostgresFrontendMessage.Bind
    var execute: PostgresFrontendMessage.Execute
}
