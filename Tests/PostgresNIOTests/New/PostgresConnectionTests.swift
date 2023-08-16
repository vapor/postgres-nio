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
            XCTAssertEqual(listenMessage.parse.query, "LISTEN foo;")

            try await channel.writeInbound(PostgresBackendMessage.parseComplete)
            try await channel.writeInbound(PostgresBackendMessage.parameterDescription(.init(dataTypes: [])))
            try await channel.writeInbound(PostgresBackendMessage.noData)
            try await channel.writeInbound(PostgresBackendMessage.bindComplete)
            try await channel.writeInbound(PostgresBackendMessage.commandComplete("LISTEN"))
            try await channel.writeInbound(PostgresBackendMessage.readyForQuery(.idle))

            try await channel.writeInbound(PostgresBackendMessage.notification(.init(backendPID: 12, channel: "foo", payload: "wooohooo")))

            let unlistenMessage = try await channel.waitForUnpreparedRequest()
            XCTAssertEqual(unlistenMessage.parse.query, "UNLISTEN foo;")

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
            XCTAssertEqual(listenMessage.parse.query, "LISTEN foo;")

            try await channel.writeInbound(PostgresBackendMessage.parseComplete)
            try await channel.writeInbound(PostgresBackendMessage.parameterDescription(.init(dataTypes: [])))
            try await channel.writeInbound(PostgresBackendMessage.noData)
            try await channel.writeInbound(PostgresBackendMessage.bindComplete)
            try await channel.writeInbound(PostgresBackendMessage.commandComplete("LISTEN"))
            try await channel.writeInbound(PostgresBackendMessage.readyForQuery(.idle))

            try await channel.writeInbound(PostgresBackendMessage.notification(.init(backendPID: 12, channel: "foo", payload: "wooohooo")))
            try await channel.writeInbound(PostgresBackendMessage.notification(.init(backendPID: 12, channel: "foo", payload: "wooohooo2")))

            let unlistenMessage = try await channel.waitForUnpreparedRequest()
            XCTAssertEqual(unlistenMessage.parse.query, "UNLISTEN foo;")

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
                XCTFail("Unexpected error: \(failure)")
            }
        }
    }

    func testSimpleListenConnectionDrops() async throws {
        let (connection, channel) = try await self.makeTestConnectionWithAsyncTestingChannel()

        try await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                let events = try await connection.listen("foo")
                var iterator = events.makeAsyncIterator()
                let first = try await iterator.next()
                XCTAssertEqual(first?.payload, "wooohooo")
                do {
                    _ = try await iterator.next()
                    XCTFail("Did not expect to not throw")
                } catch {
                    print(error)
                }
            }

            let listenMessage = try await channel.waitForUnpreparedRequest()
            XCTAssertEqual(listenMessage.parse.query, "LISTEN foo;")

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
                XCTFail("Unexpected error: \(failure)")
            }
        }
    }

    func testCloseGracefullyClosesWhenInternalQueueIsEmpty() async throws {
        let (connection, channel) = try await self.makeTestConnectionWithAsyncTestingChannel()
        try await withThrowingTaskGroup(of: Void.self) { taskGroup async throws -> () in
            for _ in 1...2 {
                taskGroup.addTask {
                    let rows = try await connection.query("SELECT 1;", logger: self.logger)
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

    func testCloseClosesImmediatly() async throws {
        let (connection, channel) = try await self.makeTestConnectionWithAsyncTestingChannel()

        try await withThrowingTaskGroup(of: Void.self) { taskGroup async throws -> () in
            for _ in 1...2 {
                taskGroup.addTask {
                    try await connection.query("SELECT 1;", logger: self.logger)
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

    struct TestPrepareStatement: PostgresPreparedStatement {
        static var sql = "SELECT datname FROM pg_stat_activity WHERE state = $1"
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
            // Wait for the PREPARE request from the client
            guard case .parse(let parse) = try await channel.waitForOutboundWrite(as: PostgresFrontendMessage.self) else {
                fatalError("Unexpected message")
            }
            XCTAssertEqual(parse.query, "SELECT datname FROM pg_stat_activity WHERE state = $1")
            XCTAssertEqual(parse.parameters.count, 0)
            guard case .describe(.preparedStatement(let name)) = try await channel.waitForOutboundWrite(as: PostgresFrontendMessage.self) else {
                fatalError("Unexpected message")
            }
            XCTAssertEqual(name, String(reflecting: TestPrepareStatement.self))
            guard case .sync = try await channel.waitForOutboundWrite(as: PostgresFrontendMessage.self) else {
                fatalError("Unexpected message")
            }

            // Respond to the PREPARE request
            try await channel.writeInbound(PostgresBackendMessage.parseComplete)
            try await channel.testingEventLoop.executeInContext { channel.read() }
            try await channel.writeInbound(PostgresBackendMessage.parameterDescription(.init(dataTypes: [
                PostgresDataType.text
            ])))
            try await channel.testingEventLoop.executeInContext { channel.read() }
            let rowDescription = RowDescription(columns: [
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
            try await channel.writeInbound(PostgresBackendMessage.rowDescription(rowDescription))
            try await channel.testingEventLoop.executeInContext { channel.read() }
            try await channel.writeInbound(PostgresBackendMessage.readyForQuery(.idle))
            try await channel.testingEventLoop.executeInContext { channel.read() }

            // Wait for the EXECUTE request
            guard case .bind(let bind) = try await channel.waitForOutboundWrite(as: PostgresFrontendMessage.self) else {
                fatalError("Unexpected message")
            }
            XCTAssertEqual(bind.preparedStatementName, String(reflecting: TestPrepareStatement.self))
            XCTAssertEqual(bind.parameters.count, 1)
            XCTAssertEqual(bind.resultColumnFormats, [.binary])
            guard case .execute = try await channel.waitForOutboundWrite(as: PostgresFrontendMessage.self) else {
                fatalError("Unexpected message")
            }
            guard case .sync = try await channel.waitForOutboundWrite(as: PostgresFrontendMessage.self) else {
                fatalError("Unexpected message")
            }
            // Respond to the EXECUTE request
            try await channel.writeInbound(PostgresBackendMessage.bindComplete)
            try await channel.testingEventLoop.executeInContext { channel.read() }
            let dataRow = DataRow(arrayLiteral: "test_database")
            try await channel.writeInbound(PostgresBackendMessage.dataRow(dataRow))
            try await channel.testingEventLoop.executeInContext { channel.read() }
            try await channel.writeInbound(PostgresBackendMessage.commandComplete(TestPrepareStatement.sql))
            try await channel.testingEventLoop.executeInContext { channel.read() }
            try await channel.writeInbound(PostgresBackendMessage.readyForQuery(.idle))
            try await channel.testingEventLoop.executeInContext { channel.read() }
        }
    }

    func makeTestConnectionWithAsyncTestingChannel() async throws -> (PostgresConnection, NIOAsyncTestingChannel) {
        let eventLoop = NIOAsyncTestingEventLoop()
        let channel = await NIOAsyncTestingChannel(handlers: [
            ReverseByteToMessageHandler(PSQLFrontendMessageDecoder()),
            ReverseMessageToByteHandler(PSQLBackendMessageEncoder()),
        ], loop: eventLoop)
        try await channel.connect(to: .makeAddressResolvingHost("localhost", port: 5432))

        let configuration = PostgresConnection.Configuration(
            establishedChannel: channel,
            username: "username",
            password: "postgres",
            database: "database"
        )

        async let connectionPromise = PostgresConnection.connect(on: eventLoop, configuration: configuration, id: 1, logger: self.logger)
        let message = try await channel.waitForOutboundWrite(as: PostgresFrontendMessage.self)
        XCTAssertEqual(message, .startup(.versionThree(parameters: .init(user: "username", database: "database", replication: .false))))
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
}

struct UnpreparedRequest {
    var parse: PostgresFrontendMessage.Parse
    var describe: PostgresFrontendMessage.Describe
    var bind: PostgresFrontendMessage.Bind
    var execute: PostgresFrontendMessage.Execute
}
