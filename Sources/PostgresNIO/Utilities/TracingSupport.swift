import NIOCore
import Tracing

/// Tracing configuration and the span attributes that are the same for every span a connection emits.
///
/// Created once per connection, after the channel has connected, so the peer address is known.
struct TracingSupport: Sendable {
    let options: TracingConfiguration
    /// The tracer spans are created with. A no-op tracer if tracing is disabled.
    let tracer: any Tracer
    /// Attributes that only depend on the connection: `db.system.name`, `db.namespace`,
    /// `server.address`, `server.port`, `network.peer.address` and `network.peer.port`.
    let baseAttributes: SpanAttributes
    /// The `{target}` of the span naming fallback chain, when no collection name is available:
    /// `db.namespace` if set, `server.address:server.port` otherwise.
    let target: String

    init(configuration: PostgresConnection.InternalConfiguration, remoteAddress: SocketAddress?) {
        self.options = configuration.options.tracing
        self.tracer = self.options.isEnabled ? self.options.tracer : NoOpTracer()

        var attributes = SpanAttributes()
        attributes.db.system.name = .postgresql
        attributes.db.namespace = configuration.database

        // `server.*` describes what the user asked to connect to, `network.peer.*` what we ended up
        // talking to. For connections made on a preexisting channel only the latter is known.
        var address: String?
        switch configuration.connection {
        case .unresolvedTCP(let host, let port):
            attributes.server.address = host
            // Only emit this if it's not the default port.
            if port != 5432 {
                attributes.server.port = port
            }
            address = "\(host):\(port)"
        case .unresolvedUDS(let path):
            attributes.server.address = path
            address = path
        case .resolved(let socketAddress):
            address = Self.recordServer(socketAddress, in: &attributes)
        case .bootstrapped:
            if let remoteAddress {
                address = Self.recordServer(remoteAddress, in: &attributes)
            }
        }

        if let remoteAddress {
            if let ipAddress = remoteAddress.ipAddress {
                attributes.network.peer.address = ipAddress
                attributes.network.peer.port = remoteAddress.port
            } else if let pathname = remoteAddress.pathname {
                attributes.network.peer.address = pathname
            }
        }

        self.baseAttributes = attributes
        // Falls back to `db.system.name` only if neither an address nor a database is known, which can
        // only happen on a preexisting channel without a remote address.
        self.target = configuration.database ?? address ?? "postgresql"
    }

    /// Records `server.address` and `server.port` from a socket address and returns the
    /// `server.address:server.port` form used as span name target.
    private static func recordServer(_ socketAddress: SocketAddress, in attributes: inout SpanAttributes) -> String? {
        if let ipAddress = socketAddress.ipAddress {
            attributes.server.address = ipAddress
            guard let port = socketAddress.port else { return ipAddress }
            if port != 5432 {
                attributes.server.port = port
            }
            return "\(ipAddress):\(port)"
        } else if let pathname = socketAddress.pathname {
            attributes.server.address = pathname
            return pathname
        }
        return nil
    }

    /// Computes the span name following the OTel database span naming fallback chain:
    ///
    /// 1. `{db.query.summary}`, if the caller supplied one
    /// 2. `{db.operation.name} {target}`, if a low-cardinality operation name is known
    /// 3. `{target}`, being the first available of `db.collection.name`, `db.namespace`
    ///    or `server.address:server.port`
    /// 4. `{db.system.name}` (`postgresql`)
    ///
    /// The query text is never inspected, as mandated by the spec.
    func spanName(summary: String? = nil, operation: String? = nil, collection: String? = nil) -> String {
        if let summary {
            return summary
        }
        let target = collection ?? self.target
        if let operation {
            return "\(operation) \(target)"
        }
        return target
    }

    /// Runs `operation` inside a new span, ending the span when the operation completes.
    ///
    /// Unlike `Tracer.withSpan`, this does not record errors thrown by `operation` on the span: server
    /// errors carry fields such as `DETAIL` and `HINT` that may contain user data. The operation is
    /// expected to record a sanitized form via ``recordError(_:in:)`` before throwing.
    func withSpan<T>(
        _ operationName: String,
        at startedAt: DefaultTracerClock.Instant = DefaultTracerClock.now,
        _ operation: (any Span) async throws -> T
    ) async rethrows -> T {
        let span = self.tracer.startSpan(operationName, ofKind: .client, at: startedAt)
        defer { span.end() }

        return try await ServiceContext.$current.withValue(span.context) {
            try await operation(span)
        }
    }

    /// Records `error.type`, `db.response.status_code` and the span status for a failed query.
    ///
    /// `error.type` is the SQLSTATE for server errors, the ``PSQLError/Code-swift.struct`` for other
    /// client errors, the ``PostgresDecodingError/Code-swift.struct`` for decoding errors and the
    /// error's type name otherwise.
    ///
    /// The error itself is deliberately not recorded on the span: server errors carry fields such as
    /// `DETAIL` and `HINT` that may contain user data.
    func recordError(_ error: any Error, in span: any Span) {
        let errorType: String
        switch error {
        case let error as PSQLError:
            if error.code == .server, let sqlState = error.serverInfo?[.sqlState] {
                span.attributes.db.response.statusCode = sqlState
                errorType = sqlState
            } else {
                errorType = "PSQLError.Code.\(error.code)"
            }
        case let error as PostgresDecodingError:
            errorType = "PostgresDecodingError.Code.\(error.code)"
        default:
            errorType = String(reflecting: type(of: error))
        }
        span.attributes.error.type = .init(rawValue: errorType)
        span.setStatus(.init(code: .error))
    }
}
