import Tracing

/// Configuration for PostgresNIO's distributed tracing support.
public struct PostgresTracingConfiguration: Sendable {
    /// Policy for attaching `db.query.text` to database spans.
    public enum QueryTextPolicy: Sendable {
        /// Record query text only when PostgresNIO can treat it as parameterized or library-generated.
        case safe

        /// Record all query text, including raw SQL that may contain literal values.
        case recordAll
    }

    /// Policy for attaching optional low-cardinality statement metadata to spans.
    public enum StatementMetadataPolicy: Sendable {
        /// Emit exact statement metadata only for operations whose semantics are already known to PostgresNIO.
        case exact

        /// Heuristically infer statement metadata from SQL text when exact metadata is unavailable.
        ///
        /// This keeps compatibility with backends that benefit from SQL verb grouping, but the
        /// metadata is derived from the query text rather than the higher-level API surface.
        case inferred

        /// Do not attach optional statement metadata.
        case disabled
    }

    /// Policy for attaching potentially sensitive error details to recorded tracing exceptions.
    public enum ErrorDetailsPolicy: Sendable {
        /// Preserve the default `Error` description.
        ///
        /// For `PSQLError`, this keeps the generic privacy-preserving description.
        case safe

        /// Attach only the primary server message when PostgresNIO receives a server-side `PSQLError`.
        ///
        /// This is often enough to surface useful failures such as `deadlock detected`
        /// without attaching the full debug representation.
        case message

        /// Attach `String(reflecting: error)` as the exception message.
        ///
        /// This may include sensitive information such as server detail strings, query context,
        /// and source locations. Use only when that additional visibility is acceptable.
        case debugDescription
    }

    @usableFromInline
    var _tracer: Optional<any Sendable>

    /// Whether tracing is enabled for this PostgresNIO instance.
    public var isEnabled: Bool

    /// Query text recording policy for database spans.
    public var queryTextPolicy: QueryTextPolicy

    /// How PostgresNIO should attach optional low-cardinality statement metadata to spans.
    public var statementMetadataPolicy: StatementMetadataPolicy

    /// How much detail PostgresNIO should attach to recorded tracing exceptions.
    public var errorDetailsPolicy: ErrorDetailsPolicy

    /// Create a tracing configuration.
    ///
    /// - Parameters:
    ///   - isEnabled: Whether tracing is enabled. Defaults to `false`.
    ///   - queryTextPolicy: Query text policy. Defaults to `.safe`.
    ///   - statementMetadataPolicy: Optional statement metadata policy. Defaults to `.exact`.
    ///   - errorDetailsPolicy: Error details policy. Defaults to `.safe`.
    public init(
        isEnabled: Bool = false,
        queryTextPolicy: QueryTextPolicy = .safe,
        statementMetadataPolicy: StatementMetadataPolicy = .exact,
        errorDetailsPolicy: ErrorDetailsPolicy = .safe
    ) {
        self._tracer = nil
        self.isEnabled = isEnabled
        self.queryTextPolicy = queryTextPolicy
        self.statementMetadataPolicy = statementMetadataPolicy
        self.errorDetailsPolicy = errorDetailsPolicy
    }

    /// Tracer that should be used by PostgresNIO.
    ///
    /// If `nil`, PostgresNIO uses ``InstrumentationSystem/tracer`` when tracing is enabled.
    public var tracer: (any Tracer)? {
        get {
            guard let _tracer else {
                return nil
            }
            return _tracer as! (any Tracer)?
        }
        set {
            self._tracer = newValue
        }
    }
}

extension PostgresTracingConfiguration {
    @usableFromInline
    var resolvedTracer: (any Tracer)? {
        guard self.isEnabled else {
            return nil
        }
        if let _tracer {
            return _tracer as! (any Tracer)?
        }
        return InstrumentationSystem.tracer
    }
}
