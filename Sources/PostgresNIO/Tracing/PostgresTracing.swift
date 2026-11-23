import NIOCore
import Tracing

struct PostgresTracing: Sendable {
    private let recordsQueryText: Bool
    private let recordsReturnedRows: Bool
    /// `db.system.name`, `db.namespace`, `server.address` and `server.port`, which the transaction span reports.
    private let scopeAttributes: SpanAttributes
    /// ``scopeAttributes`` plus `network.peer.address` and `network.peer.port`, which every other span reports.
    private let baseAttributes: SpanAttributes
    private let target: String

    init(configuration: PostgresConnection.InternalConfiguration, remoteAddress: SocketAddress?) {
        let tracing = configuration.options.tracing
        self.recordsQueryText = tracing.recordsQueryText
        self.recordsReturnedRows = tracing.recordsReturnedRows

        var attributes = SpanAttributes()
        attributes["db.system.name"] = tracing.databaseSystemName
        attributes["db.namespace"] = configuration.database

        // `server.*` describes what the user asked to connect to, `network.peer.*` what we ended up
        // talking to
        var endpoint: String?
        func appendServer(address: String, port: Int?) {
            attributes["server.address"] = address
            // The conventions only ask for the port when it isn't the default one.
            if let port, port != 5432 {
                attributes["server.port"] = port
            }
            endpoint = port.map { "\(address):\($0)" } ?? address
        }
        switch configuration.connection {
        case .unresolvedTCP(let host, let port):
            appendServer(address: host, port: port)
        case .unresolvedUDS(let path):
            appendServer(address: path, port: nil)
        case .resolved(let socketAddress):
            if let address = socketAddress.ipAddress ?? socketAddress.pathname {
                appendServer(address: address, port: socketAddress.port)
            }
        case .bootstrapped:
            if let remoteAddress, let address = remoteAddress.ipAddress ?? remoteAddress.pathname {
                appendServer(address: address, port: remoteAddress.port)
            }
        }
        self.scopeAttributes = attributes

        if let remoteAddress, let address = remoteAddress.ipAddress ?? remoteAddress.pathname {
            attributes["network.peer.address"] = address
            attributes["network.peer.port"] = remoteAddress.port
        }
        self.baseAttributes = attributes
        self.target = configuration.database ?? endpoint ?? tracing.databaseSystemName
    }

    /// Computes the span name following the OTel database span naming fallback chain:
    ///
    /// 1. `{db.query.summary}`, if the caller supplied one
    /// 2. `{db.operation.name} {target}`, if a low-cardinality operation name is known
    /// 3. `{target}`, being the first available of `db.collection.name`, `db.namespace`
    ///    or `server.address:server.port`
    /// 4. `{db.system.name}`
    ///
    /// The query text is never inspected, as mandated by the spec.
    private func spanName(summary: String?, operation: String?, collection: String?) -> String {
        if let summary {
            return summary
        }
        let target = collection ?? self.target
        if let operation {
            return "\(operation) \(target)"
        }
        return target
    }

    private var activeTracer: (any Tracer)? {
        let tracer = InstrumentationSystem.tracer
        return tracer is NoOpTracer ? nil : tracer
    }

    func startSpan(
        queryText: String,
        summary: String? = nil,
        operation: String? = nil,
        collection: String? = nil
    ) -> (any Span)? {
        guard let tracer = self.activeTracer else { return nil }
        let span = tracer.startSpan(
            self.spanName(summary: summary, operation: operation, collection: collection),
            ofKind: .client
        )
        span.updateAttributes { attributes in
            attributes.merge(self.baseAttributes)
            attributes["db.query.summary"] = summary
            attributes["db.operation.name"] = operation
            attributes["db.collection.name"] = collection
            if self.recordsQueryText {
                attributes["db.query.text"] = queryText
            }
        }
        return span
    }

    func startTransactionSpan() -> (any Span)? {
        guard let tracer = self.activeTracer else { return nil }
        let span = tracer.startSpan("TRANSACTION", ofKind: .internal)
        span.attributes.merge(self.scopeAttributes)
        return span
    }

    /// Records `error.type`, `db.response.status_code` and the span status for a failed operation.
    func recordError(_ error: any Error, in span: (any Span)?) {
        guard let span else { return }
        let psqlError = error as? PSQLError
        span.updateAttributes { attributes in
            attributes["error.type"] = psqlError?.telemetryErrorType ?? "_OTHER"
            attributes["db.response.status_code"] = psqlError?.telemetryStatusCode
        }
        span.setStatus(.init(code: .error))
    }

    func recordReturnedRows(of rows: PostgresRowSequence, in span: (any Span)?) {
        guard self.recordsReturnedRows else { return }
        span?.attributes["db.response.returned_rows"] = rows.stream.receivedRowCount.load(ordering: .relaxed)
    }
}
