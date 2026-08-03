import NIOCore
import NIOPosix
import Logging

extension Logger {
    static var psqlTest: Logger {
        var logger = Logger(label: "psql.test")
        logger.logLevel = .info
        return logger
    }
}

func withSilentServer<Success: ~Copyable>(_ body: (_ port: Int) async throws -> Success) async throws -> Success{
    let server = try await ServerBootstrap(group: NIOSingletons.posixEventLoopGroup)
        .bind(to: .init(ipAddress: "127.0.0.1", port: 0)).get()
    let result: Result<Success, any Error>
    do {
        result = .success(try await body(server.localAddress!.port!))
    } catch {
        result = .failure(error)
    }
    try? await server.close().get()
    return try result.get()
}
