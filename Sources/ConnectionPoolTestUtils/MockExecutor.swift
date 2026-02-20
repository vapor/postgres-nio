//
//  MockExecutor.swift
//  postgres-nio
//
//  Created by Fabian Fett on 07.05.25.
//

import _ConnectionPoolModule

public final class MockExecutor: ConnectionPoolExecutor, Sendable {
    public typealias ID = ObjectIdentifier

    public var id: ID { ObjectIdentifier(self) }

    static public func getExecutorID() -> ObjectIdentifier? {
        MockExecutor.executorID
    }

    public init() {}

    @TaskLocal
    static var executorID: MockExecutor.ID?
}
