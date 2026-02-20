import _ConnectionPoolModule
import _ConnectionPoolTestUtils
import Benchmark
import NIOCore
import NIOPosix

let benchmarks: @Sendable () -> Void = {
    Benchmark("Pool: Lease/Release 1k requests: 50 parallel", configuration: .init(scalingFactor: .kilo)) { benchmark in
        let clock = MockClock()
        let factory = MockConnectionFactory<MockClock, MockExecutor>(autoMaxStreams: 1)
        var configuration = ConnectionPoolConfiguration()
        configuration.maximumConnectionSoftLimit = 50
        configuration.maximumConnectionHardLimit = 50

        let pool = ConnectionPool(
            configuration: configuration,
            connectionConfiguration: MockConnectionConfiguration(username: "username", password: "password"),
            idGenerator: ConnectionIDGenerator(),
            keepAliveBehavior: MockPingPongBehavior(keepAliveFrequency: nil, connectionType: MockConnection.self),
            executor: MockExecutor(),
            observabilityDelegate: NoOpConnectionPoolMetrics(connectionIDType: MockConnection<MockExecutor>.ID.self),
            clock: clock
        ) {
            try await factory.makeConnection(id: $0, configuration: $1, for: $2)
        }

        await withTaskGroup { taskGroup in

            taskGroup.addTask {
                await pool.run()
            }

            let sequential = benchmark.scaledIterations.upperBound / configuration.maximumConnectionSoftLimit

            benchmark.startMeasurement()

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

            benchmark.stopMeasurement()

            taskGroup.cancelAll()
        }
    }

    Benchmark("Pool: Lease/Release 1k requests: sequential", configuration: .init(scalingFactor: .kilo)) { benchmark in
        let clock = MockClock()
        let factory = MockConnectionFactory<MockClock, MockExecutor>(autoMaxStreams: 1)
        var configuration = ConnectionPoolConfiguration()
        configuration.maximumConnectionSoftLimit = 50
        configuration.maximumConnectionHardLimit = 50

        let pool = ConnectionPool(
            configuration: configuration,
            connectionConfiguration: MockConnectionConfiguration(username: "username", password: "password"),
            idGenerator: ConnectionIDGenerator(),
            keepAliveBehavior: MockPingPongBehavior(keepAliveFrequency: nil, connectionType: MockConnection.self),
            executor: MockExecutor(),
            observabilityDelegate: NoOpConnectionPoolMetrics(connectionIDType: MockConnection<MockExecutor>.ID.self),
            clock: clock
        ) {
            try await factory.makeConnection(id: $0, configuration: $1, for: $2)
        }

        await withTaskGroup { taskGroup in

            taskGroup.addTask {
                await pool.run()
            }

            let sequential = benchmark.scaledIterations.upperBound / configuration.maximumConnectionSoftLimit

            benchmark.startMeasurement()

            for _ in benchmark.scaledIterations {
                do {
                    try await pool.withConnection { connection in
                        blackHole(connection)
                    }
                } catch {
                    fatalError()
                }
            }

            benchmark.stopMeasurement()

            taskGroup.cancelAll()
        }
    }

    Benchmark("PoolManager/TaskExecutor: Lease/Release 1k requests: 50 parallel – 10 MockExecutors", configuration: .init(scalingFactor: .kilo)) { benchmark in
        let clock = MockClock()
        let factory = MockConnectionFactory<MockClock, MockExecutor>(autoMaxStreams: 1)
        var configuration = ConnectionPoolManagerConfiguration()
        let executorCount = 10
        let executors = (0..<executorCount).map { _ in MockExecutor() }

        let concurrency = 50

        configuration.maximumConnectionPerExecutorSoftLimit = concurrency / executorCount
        configuration.maximumConnectionPerExecutorHardLimit = concurrency / executorCount

        let pool = ConnectionPoolManager(
            configuration: configuration,
            connectionConfiguration: MockConnectionConfiguration(username: "username", password: "password"),
            idGenerator: ConnectionIDGenerator(),
            requestType: ConnectionRequest<MockConnection>.self,
            keepAliveBehavior: MockPingPongBehavior(keepAliveFrequency: nil, connectionType: MockConnection.self),
            executors: executors,
            observabilityDelegate: NoOpConnectionPoolMetrics(connectionIDType: MockConnection<MockExecutor>.ID.self),
            clock: clock
        ) {
            try await factory.makeConnection(id: $0, configuration: $1, for: $2)
        }

        await withTaskGroup { taskGroup in
            taskGroup.addTask {
                await pool.run()
            }

            let sequential = benchmark.scaledIterations.upperBound / concurrency

            benchmark.startMeasurement()

            for parallel in 0..<concurrency {
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

            for i in 0..<concurrency {
                await taskGroup.next()
            }

            benchmark.stopMeasurement()

            taskGroup.cancelAll()
        }
    }

    if #available(macOS 15.0, iOS 18.0, tvOS 18.0, watchOS 11.0, *) {
        let eventLoops = NIOSingletons.posixEventLoopGroup
        let count = eventLoops.makeIterator().reduce(into: 0, { (result, _) in result += 1 })
        Benchmark("PoolManager/TaskExecutor: Lease/Release 1k requests: 10 parallel – \(count) NIO executors", configuration: .init(scalingFactor: .kilo)) { benchmark in
            let clock = MockClock()
            let factory = MockConnectionFactory<MockClock, NIOTaskExecutor>(autoMaxStreams: 1)
            var configuration = ConnectionPoolManagerConfiguration()
            try await NIOTaskExecutor.withExecutors(eventLoops) { executors in
                let concurrency = 50

                configuration.maximumConnectionPerExecutorSoftLimit = concurrency / executors.count
                configuration.maximumConnectionPerExecutorHardLimit = concurrency / executors.count

                let pool = ConnectionPoolManager(
                    configuration: configuration,
                    connectionConfiguration: MockConnectionConfiguration(username: "username", password: "password"),
                    idGenerator: ConnectionIDGenerator(),
                    requestType: ConnectionRequest<MockConnection>.self,
                    keepAliveBehavior: MockPingPongBehavior(keepAliveFrequency: nil, connectionType: MockConnection.self),
                    executors: executors,
                    observabilityDelegate: NoOpConnectionPoolMetrics(connectionIDType: MockConnection<NIOTaskExecutor>.ID.self),
                    clock: clock
                ) {
                    try await factory.makeConnection(id: $0, configuration: $1, for: $2)
                }

                await withTaskGroup { taskGroup in
                    taskGroup.addTask {
                        await pool.run()
                    }

                    let sequential = benchmark.scaledIterations.upperBound / executors.count

                    benchmark.startMeasurement()

                    for executor in executors {
                        taskGroup.addTask(executorPreference: executor) {
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

                    for _ in executors {
                        await taskGroup.next()
                    }

                    benchmark.stopMeasurement()

                    taskGroup.cancelAll()
                }
            }
        }
    }
}
