# Metrics in PostgresNIO

## Summary

We want to collect metrics about the client. This includes both operations from the DB client, such as query times, and metrics from the connection pool, such as connection counts. Instrumenting the connection pool is the trickier part as we want to preserve performance and produce abstract statistics instead of database-specific data.

## Motivation

Collecting metrics is crucial for understanding the behavior and performance of the PostgresNIO client. 
A user currently cannot tell whether a slow query is due to the database itself or whether it is waiting on a connection from the pool. By monitoring operations, responses, and connection pool usage, we can identify bottlenecks, optimise resource usage, and ensure the client is reliable in production environments.

## Metrics

The metrics listed here are taken from the [OTel Semantic Conventions for database clients](https://opentelemetry.io/docs/specs/semconv/db/database-metrics/).

### Connection Pool

As for the connection pool, this is the list of metrics we want to collect:

`db.client.connection.count`: 
- description: The current number of connections in the pool, categorized by their state (`idle`/`used`);
- type: Gauge

`db.client.connection.max`:
- description: The maximum number of connections allowed in the pool;
- type: Gauge

`db.client.connection.idle.max`:
- description: The maximum number of idle connections allowed in the pool;
- type: Gauge

`db.client.connection.idle.min`:
- description: The minimum number of idle connections maintained in the pool;
- type: Gauge

`db.client.connection.pending_requests`:
- description: The number of pending requests waiting for a connection;
- type: Gauge

`db.client.connection.timeouts`:
- description: The number of connection timeouts that have occurred trying to obtain a connection;
- type: Counter
- not currently reported: see in open questions

`db.client.connection.create_time`:
- description: The time taken to create a new connection;
- type: Histogram

`db.client.connection.wait_time`:
- description: The time spent waiting to obtain a connection from the pool;
- type: Histogram

`db.client.connection.use_time`:
- description: The time a connection is used before being released back to the pool;
- type: Histogram

Other than the `state` label on `db.client.connection.count`, all connection pool metrics are described by the `db.client.connection.pool.name` label.

> Note: All of these metrics are still in the `development` stage and may change in future releases.

### DB Client Metrics

The DB client metrics focus on the operations performed by the client and the responses received from the database.
Following is the list of metrics:

- `db.client.operation.duration`: The duration of operations performed by the client;
- `db.client.response.returned_rows`: The number of records returned by the database operation.

Following are the attributes associated with the DB client metrics:
- `db.system.name`: `postgresql`;
- `db.namespace`: The namespace of the database involved in the operation;
- `db.response.status_code`: The status code returned by the database in response to the operation;
- `error.type`: Class of error encountered during the operation;
- `server.port`
- `db.query.summary`: A summary of the database query executed;
- `network.peer.address`: The address of the network peer (database server) involved in the operation;
- `network.peer.port`: The port of the network peer (database server) involved in the operation;
- `server.address`

> Note: `db.operation.name` and `db.collection.name` won't be produced as PostgresNIO does not parse SQL.

These attributes can be taken from the existing work at https://github.com/vapor/postgres-nio/pull/687, if refined a bit.

## Design

### Connection Pool

The connection pool holds statistics about the number of connections, their states, request count, and more. We want to collect these metrics as they're vital for understanding the performance and behavior of the connection pool. However, we also want to ensure that collecting these metrics does not introduce significant (if any) performance overhead.

In order to achieve this, we will not have the ConnectionPool rely directly on Swift Metrics. Instead, it will expose statistics about its operation after each step, allowing an external metrics collector to gather and report these metrics efficiently.
This also allows us to produce metrics that are abstract from the implementation sitting on top of the connection pool, allowing agnostic statistics to be collected for a number of different users such as database clients as well as HTTP clients.

In detail, the way this will work is that after each operation within the connection pool, statistics will be returned from the operation. These statistics will be gathered by an external protocol which will then name them, depending on the context, and report them to the appropriate metrics backend, creating Swift Metrics-compatible metrics (counters, gauges, etc.).
We will have:

```swift
struct ConnectionPoolStatistics {
    let id: Int

    // Already tracked in ConnectionGroup.Stats
    let idleConnectionCount: Int
    let usedConnectionCount: Int
    let maxConnections: Int
    let maxIdleConnections: Int
    let minIdleConnections: Int
    let pendingRequests: Int
}

extension PoolStateMachine {
    // ...

    struct Action {
        // ...
        let stats: ConnectionPoolStatistics? // nil if no new stats to report
    }
}
```

Every operation that returns an action in the state machine will also return the updated statistics; `.none()` returns `nil`.
Updating statistics happens through a `makeStats` method that looks roughly like this:

```swift
@inlinable
mutating func makeStats() -> ConnectionPoolStatistics {
    self.lastStatsID &+= 1
    return ConnectionPoolStatistics(
        id: self.lastStatsID,
        idleConnectionCount: Int(self.connections.stats.idle),
        usedConnectionCount: Int(self.connections.stats.leased),
        maxConnections: self.configuration.maximumConnectionHardLimit,
        maxIdleConnections: self.configuration.maximumConnectionSoftLimit,
        minIdleConnections: self.configuration.minimumConnectionCount,
        pendingRequests: self.requestQueue.count
    )
}
```

This method makes use of the existing `ConnectionGroup.Stats` to gather the current state of the connections.

The state machine will build stats inside the lock, ensuring that the statistics accurately reflect the state of the connection pool at the time of the operation. However, statistics will be sent to the reporter after leaving the lock, because reporting metrics may involve busy operations that could slow down the performance of the connection pool if done under the lock. This means that they could arrive out of order relative to the actual operation. To mitigate this, we'll also include a sequence number (the `id` property), allowing the reporter to drop stale statistics.

The pool will also be provided with a protocol for reporting statistics:
```swift
protocol ConnectionPoolStatisticsReporter {
    func report(statistics: ConnectionPoolStatistics)
}
```

The `report` method is called in `runStateMachineActions`, which is the method that's used to run the actions that the state machine returned after processing an operation. Reporting is run after the actual actions.

There are some extra statistics that we would like to collect:
- `createTime`
- `waitTime`
- `useTime`

These metrics don't fit into the `ConnectionPoolStatistics` structure because they are event-based and do not represent the state of the connection pool. Therefore they will be reported inside of the database client, alongside the other metrics collected by `PostgresClientMetrics`.

### Database Client

The client will be the one actually producing the metrics, using the statistics provided by the connection pool to generate the appropriate metrics for reporting, and adding custom database and PostgreSQL-specific metrics as needed.

Inside of the Postgres layer, we'll re-use the current `PostgresClientMetrics` implementation for reporting database client metrics. This class will be a `ConnectionPoolStatisticsReporter`, allowing it to receive metrics from the connection pool, merge them with the PostgresClient specific metrics and transform everything in the appropriate Swift Metrics format. This allows the client to: 
- name the metrics according to the OTel Semantic Conventions for database/PSQL clients;
- hold the burden of the performance impact of reporting metrics, ensuring that the connection pool itself remains efficient.

The `PostgresConnection` will also hold a reference to the metrics factory, allowing it to report connection-specific metrics such as query execution times, which the client cannot capture, since queries can be executed on standalone connections as well.

## Configuration

To configure metrics reporting the `PostgresClient.Configuration` and `PostgresConnection.Configuration` will also include a new `metrics: PostgresMetricsConfiguration` parameter:

```swift
public struct PostgresMetricsConfiguration: Sendable {
    public let factory: (any MetricsFactory)?

    public init(factory: (any MetricsFactory)? = MetricsSystem.factory) {
        self.factory = factory is NOOPMetricsHandler ? nil : factory
    }
}
```

This parameter will be set at configuration creation time and will be threaded down to the connection as well. We need the factory on the connection as well because metrics such as query execution times are reported from the connection layer. Setting the factory to `nil` or `NOOPMetricsHandler` will effectively disable metrics reporting.

## API and compatibility impact

The `_ConnectionPoolModule` is underscored, this means that it is considered internal and therefore provides no stability guarantee. This will allow us to conform the current `ConnectionPoolObservabilityDelegate` to `ConnectionPoolStatisticsReporter`.

On the Postgres client side, we're adding a new `PostgresMetricsConfiguration` to handle the configuration of metrics reporting, plus `options.metrics` on the `PostgresClient.Configuration` to reference it.

## Performance

The performance impact of enabling metrics is expected to be minimal, as the metrics collection is designed to be lightweight.
- Connection Pool Metrics: The overhead of collecting connection pool metrics is minimal, as it primarily involves updating integers in memory and does not require expensive operations. Building the snapshot of the current state is performed under locks however this does not show any visible performance regression. In terms of numbers, per 1000 lease/release operations measurements showed 2.65–2.69 ms without statistics vs 2.54–2.58 ms with statistics, which is just noise.
- DB Client Metrics: The overhead is higher than the connection pool metrics as we have to interact with the metrics API. However in the grand scheme of things it won't have a significant impact on overall performance, compared to blocking operations like the actual database queries themselves.

## Alternatives Considered

### AsyncStream statistics

The main discussion was around how to produce and report metrics from the connection pool in a way that's both performant and minimally intrusive to the normal operation of the client. The main alternative to the current mode of operations (returning stats along with state machine actions) was keeping an AsyncStream of connection pool statistics that could be consumed asynchronously by interested parties. This resulted in worse performance (about 2x slower) due to the overhead of task switching.

### Not collecting metrics

The obvious other choice was not emitting metrics at all. Seeing how metrics provide valuable insights into the behavior and performance of the connection pool and database client, and the connection pool performance does not regress collecting statistics, collecting metrics is the preferred approach.

## Open questions

### Timeouts 

The main missing piece right now is the timeouts metric, i.e the number of timeouts that have occurred trying to obtain a connection from the pool. Currently the pool does not track this information and we therefore cannot report it. We can either start tracking this information in the pool, or leave it up to the DB client, which could maintain its own count of timeouts and report it as part of its metrics.

### Connection Pool Name

Connection pool level metrics all include the connection pool name as a label, allowing differentiation between multiple pools within the same application. Currently the connection pool does not have a name and probably shouldn't need one, as it's the library using the pool that should provide one (e.g PostgresNIO, Valkey). We have a couple of possibilities:
- choosing a default name in the library (such as `postgresnio-connection-pool`): this has the limitation of not being able to differentiate between multiple postgresnio pools, e.g if the user connects to multiple databases;
- allowing the user to provide a custom name for the connection pool: this allows differentiation between multiple pools, but requires the user to explicitly set the name.
- derive the name as the spec suggests, using `server.address:server.port/db.namespace`. This would still not differentiate two clients connecting to the same DB.
