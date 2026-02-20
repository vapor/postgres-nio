//
//  NIOTaskExecutor.swift
//  benchmarks
//
//  Created by Fabian Fett on 09.05.25.
//

import NIOCore
import NIOPosix
import _ConnectionPoolModule

@available(macOS 15.0, iOS 18.0, tvOS 18.0, watchOS 11.0, *)
final class NIOTaskExecutor {

    private static let threadSpecificEventLoop = ThreadSpecificVariable<NIOTaskExecutor>()

    let eventLoop: any EventLoop

    private init(eventLoop: any EventLoop) {
        self.eventLoop = eventLoop
    }

    static func withExecutors(_ eventLoops: MultiThreadedEventLoopGroup, _ body: ([NIOTaskExecutor]) async throws -> ()) async throws {
        var executors = [NIOTaskExecutor]()
        for eventLoop in eventLoops.makeIterator() {
            let executor = NIOTaskExecutor(eventLoop: eventLoop)
            try await eventLoop.submit {
                NIOTaskExecutor.threadSpecificEventLoop.currentValue = executor
            }.get()
            executors.append(executor)
        }
        do {
            try await body(executors)
        } catch {

        }
        for eventLoop in eventLoops.makeIterator() {
            try await eventLoop.submit {
                NIOTaskExecutor.threadSpecificEventLoop.currentValue = nil
            }.get()
        }
    }
}

@available(macOS 15.0, iOS 18.0, tvOS 18.0, watchOS 11.0, *)
extension NIOTaskExecutor: TaskExecutor {

    func enqueue(_ job: consuming ExecutorJob) {
        // By default we are just going to use execute to run the job
        // this is quite heavy since it allocates the closure for
        // every single job.
        let unownedJob = UnownedJob(job)
        self.eventLoop.execute {
            unownedJob.runSynchronously(on: self.asUnownedTaskExecutor())
        }
    }

    func asUnownedTaskExecutor() -> UnownedTaskExecutor {
        UnownedTaskExecutor(ordinary: self)
    }
}

@available(macOS 15.0, iOS 18.0, tvOS 18.0, watchOS 11.0, *)
extension NIOTaskExecutor: ConnectionPoolExecutor {
    typealias ID = ObjectIdentifier

    var id: ObjectIdentifier {
        ObjectIdentifier(self)
    }

    static func getExecutorID() -> ObjectIdentifier? {
        self.threadSpecificEventLoop.currentValue?.id
    }
}
