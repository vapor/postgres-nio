#if swift(>=5.5.2) && canImport(_Concurrency)
extension PSQLConnection {

    public static func connect(
        configuration: PSQLConnection.Configuration,
        logger: Logger,
        on eventLoop: EventLoop
    ) async throws -> PSQLConnection {
        try await Self.connect(configuration: configuration, logger: logger, on: eventLoop).get()
    }

    public func close() async throws {
        try await self.close().get()
    }

    public func query(_ query: PSQLQuery, logger: Logger) async throws -> PSQLRowSequence {
        var logger = logger
        logger[postgresMetadataKey: .connectionID] = "\(self.connectionID)"
        guard query.binds.count <= Int(Int16.max) else {
            throw PSQLError(.tooManyParameters)
        }
        let promise = self.channel.eventLoop.makePromise(of: PSQLRowStream.self)
        let context = ExtendedQueryContext(
            query: query,
            logger: logger,
            promise: promise)

        self.channel.write(PSQLTask.extendedQuery(context), promise: nil)

        return try await promise.futureResult.map({ $0.asyncSequence() }).get()
    }
}
#endif
