import NIOCore
import NIOPosix
import XCTest
import Logging
@testable import PostgresNIO

class PSQLConnectionTests: XCTestCase {
    
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
            host: "127.0.0.1",
            port: port,
            username: "postgres",
            database: "postgres",
            password: "abc123",
            tls: .disable
        )
        
        var logger = Logger.psqlTest
        logger.logLevel = .trace
        
        XCTAssertThrowsError(try PostgresConnection.connect(connectionID: 1, configuration: config, logger: logger, on: eventLoopGroup.next()).wait()) {
            XCTAssertTrue($0 is PSQLError)
        }
    }
}
