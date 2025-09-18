import NIOCore
import NIOPosix
import NIOEmbedded
import Testing
import Logging
@testable import PostgresNIO

@Suite struct PostgresConnectionTests {

    let logger = Logger(label: "PostgresConnectionTests")

    @Test func testConnectionFailure() {
        // We start a local server and close it immediately to ensure that the port
        // number we try to connect to is not used by any other process.
        let eventLoopGroup = NIOSingletons.posixEventLoopGroup

        var tempChannel: Channel?
        #expect(throws: Never.self) {
            tempChannel = try ServerBootstrap(group: eventLoopGroup)
                .bind(to: .init(ipAddress: "127.0.0.1", port: 0)).wait()
        }
        let maybePort = tempChannel?.localAddress?.port
        #expect(throws: Never.self) { try tempChannel?.close().wait() }
        guard let port = maybePort else {
            Issue.record("Could not get port number from temp started server")
            return
        }
        
        let config = PostgresConnection.Configuration(
            host: "127.0.0.1", port: port,
            username: "postgres", password: "abc123", database: "postgres",
            tls: .disable
        )
        
        var logger = Logger.psqlTest
        logger.logLevel = .trace
        
        #expect(throws: PSQLError.self) {
            try PostgresConnection
                .connect(on: eventLoopGroup.next(), configuration: config, id: 1, logger: logger)
                .wait()
        }
    }

    @Test func testOptionsAreSentOnTheWire() async throws {
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
        #expect(message == .startup(.versionThree(parameters: .init(user: "username", database: "database", options: configuration.options.additionalStartupParameters, replication: .false))))
        try await channel.writeInbound(PostgresBackendMessage.authentication(.ok))
        try await channel.writeInbound(PostgresBackendMessage.backendKeyData(.init(processID: 1234, secretKey: 5678)))
        try await channel.writeInbound(PostgresBackendMessage.readyForQuery(.idle))

        let connection = try await connectionPromise
        try await connection.close()
    }

    @Test func testSimpleListen() async throws {
        try await self.withAsyncTestingChannel { connection, channel in
            try await withThrowingTaskGroup(of: Void.self) { taskGroup in
                taskGroup.addTask {
                    let events = try await connection.listen("foo")
                    for try await event in events {
                        #expect(event.payload == "wooohooo")
                        break
                    }
                }

                let listenMessage = try await channel.waitForUnpreparedRequest()
                #expect(listenMessage.parse.query == #"LISTEN "foo";"#)

                try await channel.writeInbound(PostgresBackendMessage.parseComplete)
                try await channel.writeInbound(PostgresBackendMessage.parameterDescription(.init(dataTypes: [])))
                try await channel.writeInbound(PostgresBackendMessage.noData)
                try await channel.writeInbound(PostgresBackendMessage.bindComplete)
                try await channel.writeInbound(PostgresBackendMessage.commandComplete("LISTEN"))
                try await channel.writeInbound(PostgresBackendMessage.readyForQuery(.idle))

                try await channel.writeInbound(PostgresBackendMessage.notification(.init(backendPID: 12, channel: "foo", payload: "wooohooo")))

                let unlistenMessage = try await channel.waitForUnpreparedRequest()
                #expect(unlistenMessage.parse.query == #"UNLISTEN "foo";"#)

                try await channel.writeInbound(PostgresBackendMessage.parseComplete)
                try await channel.writeInbound(PostgresBackendMessage.parameterDescription(.init(dataTypes: [])))
                try await channel.writeInbound(PostgresBackendMessage.noData)
                try await channel.writeInbound(PostgresBackendMessage.bindComplete)
                try await channel.writeInbound(PostgresBackendMessage.commandComplete("UNLISTEN"))
                try await channel.writeInbound(PostgresBackendMessage.readyForQuery(.idle))

                switch await taskGroup.nextResult()! {
                case .success:
                    break
                case .failure(let failure):
                    Issue.record("Unexpected error: \(failure)")
                }
            }
        }
    }

    @Test func testSimpleListenDoesNotUnlistenIfThereIsAnotherSubscriber() async throws {
        try await self.withAsyncTestingChannel { connection, channel in

            try await withThrowingTaskGroup(of: Void.self) { taskGroup in
                taskGroup.addTask {
                    let events = try await connection.listen("foo")
                    for try await event in events {
                        #expect(event.payload == "wooohooo")
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
                            #expect(event.payload == "wooohooo")
                        case 1:
                            #expect(event.payload == "wooohooo2")
                            break loop
                        default:
                            Issue.record("Unexpected message: \(event)")
                        }
                    }
                }

                let listenMessage = try await channel.waitForUnpreparedRequest()
                #expect(listenMessage.parse.query == #"LISTEN "foo";"#)

                try await channel.writeInbound(PostgresBackendMessage.parseComplete)
                try await channel.writeInbound(PostgresBackendMessage.parameterDescription(.init(dataTypes: [])))
                try await channel.writeInbound(PostgresBackendMessage.noData)
                try await channel.writeInbound(PostgresBackendMessage.bindComplete)
                try await channel.writeInbound(PostgresBackendMessage.commandComplete("LISTEN"))
                try await channel.writeInbound(PostgresBackendMessage.readyForQuery(.idle))

                try await channel.writeInbound(PostgresBackendMessage.notification(.init(backendPID: 12, channel: "foo", payload: "wooohooo")))
                try await channel.writeInbound(PostgresBackendMessage.notification(.init(backendPID: 12, channel: "foo", payload: "wooohooo2")))

                let unlistenMessage = try await channel.waitForUnpreparedRequest()
                #expect(unlistenMessage.parse.query == #"UNLISTEN "foo";"#)

                try await channel.writeInbound(PostgresBackendMessage.parseComplete)
                try await channel.writeInbound(PostgresBackendMessage.parameterDescription(.init(dataTypes: [])))
                try await channel.writeInbound(PostgresBackendMessage.noData)
                try await channel.writeInbound(PostgresBackendMessage.bindComplete)
                try await channel.writeInbound(PostgresBackendMessage.commandComplete("UNLISTEN"))
                try await channel.writeInbound(PostgresBackendMessage.readyForQuery(.idle))

                switch await taskGroup.nextResult()! {
                case .success:
                    break
                case .failure(let failure):
                    Issue.record("Unexpected error: \(failure)")
                }
            }
        }
    }

    @Test func testSimpleListenConnectionDrops() async throws {
        try await self.withAsyncTestingChannel { connection, channel in

            try await withThrowingTaskGroup(of: Void.self) { [logger] taskGroup in
                taskGroup.addTask {
                    let events = try await connection.listen("foo")
                    var iterator = events.makeAsyncIterator()
                    let first = try await iterator.next()
                    #expect(first?.payload == "wooohooo")
                    do {
                        _ = try await iterator.next()
                        Issue.record("Did not expect to not throw")
                    } catch {
                        logger.error("error", metadata: ["error": "\(error)"])
                    }
                }

                let listenMessage = try await channel.waitForUnpreparedRequest()
                #expect(listenMessage.parse.query == #"LISTEN "foo";"#)

                try await channel.writeInbound(PostgresBackendMessage.parseComplete)
                try await channel.writeInbound(PostgresBackendMessage.parameterDescription(.init(dataTypes: [])))
                try await channel.writeInbound(PostgresBackendMessage.noData)
                try await channel.writeInbound(PostgresBackendMessage.bindComplete)
                try await channel.writeInbound(PostgresBackendMessage.commandComplete("LISTEN"))
                try await channel.writeInbound(PostgresBackendMessage.readyForQuery(.idle))

                try await channel.writeInbound(PostgresBackendMessage.notification(.init(backendPID: 12, channel: "foo", payload: "wooohooo")))
                struct MyWeirdError: Error {}
                channel.pipeline.fireErrorCaught(MyWeirdError())

                switch await taskGroup.nextResult()! {
                case .success:
                    break
                case .failure(let failure):
                    Issue.record("Unexpected error: \(failure)")
                }
            }
        }
    }

    @Test func testCloseGracefullyClosesWhenInternalQueueIsEmpty() async throws {
        try await self.withAsyncTestingChannel { connection, channel in
            try await withThrowingTaskGroup(of: Void.self) { [logger] taskGroup async throws -> () in
                for _ in 1...2 {
                    taskGroup.addTask {
                        let rows = try await connection.query("SELECT 1;", logger: logger)
                        var iterator = rows.decode(Int.self).makeAsyncIterator()
                        let first = try await iterator.next()
                        #expect(first == 1)
                        let second = try await iterator.next()
                        #expect(second == nil)
                    }
                }

                for i in 0...1 {
                    let listenMessage = try await channel.waitForUnpreparedRequest()
                    #expect(listenMessage.parse.query == "SELECT 1;")

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
                #expect(terminate == .terminate)
                try await channel.closeFuture.get()
                #expect(!channel.isActive)

                while let taskResult = await taskGroup.nextResult() {
                    switch taskResult {
                    case .success:
                        break
                    case .failure(let failure):
                        Issue.record("Unexpected error: \(failure)")
                    }
                }
            }
        }
    }

    @Test func testCloseClosesImmediatly() async throws {
        try await self.withAsyncTestingChannel { connection, channel in

            try await withThrowingTaskGroup(of: Void.self) { [logger] taskGroup async throws -> () in
                for _ in 1...2 {
                    taskGroup.addTask {
                        try await connection.query("SELECT 1;", logger: logger)
                    }
                }

                let listenMessage = try await channel.waitForUnpreparedRequest()
                #expect(listenMessage.parse.query == "SELECT 1;")

                async let close: () = connection.close()

                try await channel.closeFuture.get()
                #expect(!channel.isActive)

                try await close

                while let taskResult = await taskGroup.nextResult() {
                    switch taskResult {
                    case .success:
                        Issue.record("Expected queries to fail")
                    case .failure(let failure):
                        guard let error = failure as? PSQLError else {
                            Issue.record("Unexpected error type: \(failure)")
                            return
                        }
                        #expect(error.code == .clientClosedConnection)
                    }
                }
            }
        }
    }

    @Test func testIfServerJustClosesTheErrorReflectsThat() async throws {
        try await self.withAsyncTestingChannel { connection, channel in
            let logger = self.logger

            async let response = try await connection.query("SELECT 1;", logger: logger)

            let listenMessage = try await channel.waitForUnpreparedRequest()
            #expect(listenMessage.parse.query == "SELECT 1;")

            try await channel.testingEventLoop.executeInContext { channel.pipeline.fireChannelInactive() }
            try await channel.testingEventLoop.executeInContext { channel.pipeline.fireChannelUnregistered() }

            do {
                _ = try await response
                Issue.record("Expected to throw")
            } catch {
                #expect((error as? PSQLError)?.code == .serverClosedConnection)
            }

            // retry on same connection

            do {
                _ = try await connection.query("SELECT 1;", logger: self.logger)
                Issue.record("Expected to throw")
            } catch {
                #expect((error as? PSQLError)?.code == .serverClosedConnection)
            }
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

    @Test func testPreparedStatement() async throws {
        try await self.withAsyncTestingChannel { connection, channel in

            try await withThrowingTaskGroup(of: Void.self) { taskGroup async throws -> () in
                taskGroup.addTask {
                    let preparedStatement = TestPrepareStatement(state: "active")
                    let result = try await connection.execute(preparedStatement, logger: .psqlTest)
                    var rows = 0
                    for try await database in result {
                        rows += 1
                        #expect("test_database" == database)
                    }
                    #expect(rows == 1)
                }

                let prepareRequest = try await channel.waitForPrepareRequest()
                #expect(prepareRequest.parse.query == "SELECT datname FROM pg_stat_activity WHERE state = $1")
                #expect(prepareRequest.parse.parameters.first == .text)
                guard case .preparedStatement(let name) = prepareRequest.describe else {
                    fatalError("Describe should contain a prepared statement")
                }
                #expect(name == String(reflecting: TestPrepareStatement.self))

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
                #expect(preparedRequest.bind.preparedStatementName == String(reflecting: TestPrepareStatement.self))
                #expect(preparedRequest.bind.parameters.count == 1)
                #expect(preparedRequest.bind.resultColumnFormats == [.binary])

                try await channel.sendPreparedResponse(
                    dataRows: [
                        ["test_database"]
                    ],
                    commandTag: TestPrepareStatement.sql
                )
            }
        }
    }

    @Test func testWeDontCrashOnUnexpectedChannelEvents() async throws {
        try await self.withAsyncTestingChannel { connection, channel in

            enum MyEvent {
                case pleaseDontCrash
            }
            channel.pipeline.fireUserInboundEventTriggered(MyEvent.pleaseDontCrash)
            try await connection.close()
        }
    }

    @Test func testSerialExecutionOfSamePreparedStatement() async throws {
        try await self.withAsyncTestingChannel { connection, channel in

            try await withThrowingTaskGroup(of: Void.self) { taskGroup async throws -> () in
                // Send the same prepared statement twice, but with different parameters.
                // Send one first and wait to send the other request until preparation is complete
                taskGroup.addTask {
                    let preparedStatement = TestPrepareStatement(state: "active")
                    let result = try await connection.execute(preparedStatement, logger: .psqlTest)
                    var rows = 0
                    for try await database in result {
                        rows += 1
                        #expect("test_database" == database)
                    }
                    #expect(rows == 1)
                }

                let prepareRequest = try await channel.waitForPrepareRequest()
                #expect(prepareRequest.parse.query == "SELECT datname FROM pg_stat_activity WHERE state = $1")
                #expect(prepareRequest.parse.parameters.first == .text)
                guard case .preparedStatement(let name) = prepareRequest.describe else {
                    fatalError("Describe should contain a prepared statement")
                }
                #expect(name == String(reflecting: TestPrepareStatement.self))

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
                #expect(parameter1 == "active")
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
                        #expect("test_database" == database)
                    }
                    #expect(rows == 1)
                }

                let preparedRequest2 = try await channel.waitForPreparedRequest()
                buffer = preparedRequest2.bind.parameters[0]!
                let parameter2 = buffer.readString(length: buffer.readableBytes)!
                #expect(parameter2 == "idle")
                try await channel.sendPreparedResponse(
                    dataRows: [
                        ["test_database"]
                    ],
                    commandTag: TestPrepareStatement.sql
                )
                // Ensure we received and responded to both the requests
                let parameters = [parameter1, parameter2]
                #expect(parameters.contains("active"))
                #expect(parameters.contains("idle"))
            }
        }
    }

    @Test func testStatementPreparationOnlyHappensOnceWithConcurrentRequests() async throws {
        try await self.withAsyncTestingChannel { connection, channel in

            try await withThrowingTaskGroup(of: Void.self) { taskGroup async throws -> () in
                // Send the same prepared statement twice, but with different parameters.
                // Let them race to tests that requests and responses aren't mixed up
                taskGroup.addTask {
                    let preparedStatement = TestPrepareStatement(state: "active")
                    let result = try await connection.execute(preparedStatement, logger: .psqlTest)
                    var rows = 0
                    for try await database in result {
                        rows += 1
                        #expect("test_database_active" == database)
                    }
                    #expect(rows == 1)
                }
                taskGroup.addTask {
                    let preparedStatement = TestPrepareStatement(state: "idle")
                    let result = try await connection.execute(preparedStatement, logger: .psqlTest)
                    var rows = 0
                    for try await database in result {
                        rows += 1
                        #expect("test_database_idle" == database)
                    }
                    #expect(rows == 1)
                }

                // The channel deduplicates prepare requests, we're going to see only one of them
                let prepareRequest = try await channel.waitForPrepareRequest()
                #expect(prepareRequest.parse.query == "SELECT datname FROM pg_stat_activity WHERE state = $1")
                #expect(prepareRequest.parse.parameters.first == .text)
                guard case .preparedStatement(let name) = prepareRequest.describe else {
                    fatalError("Describe should contain a prepared statement")
                }
                #expect(name == String(reflecting: TestPrepareStatement.self))

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
                #expect(parameters.contains("active"))
                #expect(parameters.contains("idle"))
            }
        }
    }

    @Test func testStatementPreparationFailure() async throws {
        try await self.withAsyncTestingChannel { connection, channel in

            try await withThrowingTaskGroup(of: Void.self) { taskGroup async throws -> () in
                // Send the same prepared statement twice, but with different parameters.
                // Send one first and wait to send the other request until preparation is complete
                taskGroup.addTask {
                    let preparedStatement = TestPrepareStatement(state: "active")
                    do {
                        _ = try await connection.execute(preparedStatement, logger: .psqlTest)
                        Issue.record("Was supposed to fail")
                    } catch {
                        #expect(error is PSQLError)
                    }
                }

                let prepareRequest = try await channel.waitForPrepareRequest()
                #expect(prepareRequest.parse.query == "SELECT datname FROM pg_stat_activity WHERE state = $1")
                #expect(prepareRequest.parse.parameters.first == .text)
                guard case .preparedStatement(let name) = prepareRequest.describe else {
                    fatalError("Describe should contain a prepared statement")
                }
                #expect(name == String(reflecting: TestPrepareStatement.self))

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
                        Issue.record("Was supposed to fail")
                    } catch {
                        #expect(error is PSQLError)
                    }
                }
            }
        }
    }

    func withAsyncTestingChannel(_ body: (PostgresConnection, NIOAsyncTestingChannel) async throws -> ()) async throws {
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
        #expect(message == .startup(.versionThree(parameters: .init(user: "username", database: "database", options: [], replication: .false))))
        try await channel.writeInbound(PostgresBackendMessage.authentication(.ok))
        try await channel.writeInbound(PostgresBackendMessage.backendKeyData(.init(processID: 1234, secretKey: 5678)))
        try await channel.writeInbound(PostgresBackendMessage.readyForQuery(.idle))

        let connection = try await connectionPromise

        do {
            try await body(connection, channel)
        } catch {

        }

        try await connection.close()
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
