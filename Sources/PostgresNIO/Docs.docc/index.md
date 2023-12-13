# ``PostgresNIO``

@Metadata {
    @TitleHeading(Package)
}

🐘 Non-blocking, event-driven Swift client for PostgreSQL built on SwiftNIO.

## Overview

Features:

- A ``PostgresConnection`` which allows you to connect to, authorize with, query, and retrieve results from a PostgreSQL server using [SwiftNIO].
- An async/await interface that supports backpressure 
- Automatic conversions between Swift primitive types and the Postgres wire format
- Integrated with the Swift server ecosystem, including use of [SwiftLog].
- Designed to run efficiently on all supported platforms (tested extensively on Linux and Darwin systems)
- Support for `Network.framework` when available (e.g. on Apple platforms)
 
## Topics

### Articles

- <doc:migrations>

### Connections

- ``PostgresConnection``

### Querying

- ``PostgresQuery``
- ``PostgresBindings``
- ``PostgresRow``
- ``PostgresRowSequence``
- ``PostgresRandomAccessRow``
- ``PostgresCell``
- ``PreparedQuery``
- ``PostgresQueryMetadata``

### Encoding and Decoding

- ``PostgresEncodable``
- ``PostgresEncodingContext``
- ``PostgresDecodable``
- ``PostgresDecodingContext``
- ``PostgresArrayEncodable``
- ``PostgresArrayDecodable``
- ``PostgresJSONEncoder``
- ``PostgresJSONDecoder``
- ``PostgresDataType``
- ``PostgresFormat``
- ``PostgresNumeric``

### Notifications

- ``PostgresListenContext``

### Errors

- ``PostgresError``
- ``PostgresDecodingError``

### Deprecated

These types are already deprecated or will be deprecated in the near future. All of them will be 
removed from the public API with the next major release. 

- ``PostgresDatabase``
- ``PostgresData``
- ``PostgresDataConvertible``
- ``PostgresQueryResult``
- ``PostgresJSONCodable``
- ``PostgresJSONBCodable``
- ``PostgresMessageDecoder``
- ``PostgresMessage``
- ``PostgresMessageType``
- ``PostgresFormatCode``
- ``SASLAuthenticationManager``
- ``SASLAuthenticationMechanism``
- ``SASLAuthenticationError``
- ``SASLAuthenticationStepResult``

[SwiftNIO]: https://github.com/apple/swift-nio
[SwiftLog]: https://github.com/apple/swift-log
