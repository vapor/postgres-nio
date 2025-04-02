import _ConnectionPoolModule
import _ConnectionPoolTestUtils
import Benchmark

let benchmarks: @Sendable () -> Void = {
    Benchmark("Minimal benchmark", configuration: .init(scalingFactor: .kilo)) { benchmark in
        let clock = MockClock()
        let factory = MockConnectionFactory<MockClock>(autoMaxStreams: 1)
        var configuration = ConnectionPoolConfiguration()
        configuration.maximumConnectionSoftLimit = 50
        configuration.maximumConnectionHardLimit = 50

        let pool = ConnectionPool(
            configuration: configuration,
            idGenerator: ConnectionIDGenerator(),
            keepAliveBehavior: MockPingPongBehavior(keepAliveFrequency: nil, connectionType: MockConnection.self),
            observabilityDelegate: NoOpConnectionPoolMetrics(connectionIDType: MockConnection.ID.self),
            clock: clock
        ) {
            try await factory.makeConnection(id: $0, for: $1)
        }

        await withTaskGroup { taskGroup in

            taskGroup.addTask {
                await pool.run()
            }

            let sequential = benchmark.scaledIterations.upperBound / configuration.maximumConnectionSoftLimit

            for parallel in 0..<configuration.maximumConnectionSoftLimit {
                taskGroup.addTask {
                    for _ in 0..<sequential {
                        do {
                            try await pool.withConnection { connection in
                                blackHole(connection)
                            }
                        } catch {
                            fatalError()
                        }
                    }
                }
            }

            for i in 0..<configuration.maximumConnectionSoftLimit {
                await taskGroup.next()
            }
            taskGroup.cancelAll()
        }
    }
}
