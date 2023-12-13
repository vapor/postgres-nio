// swift-tools-version:5.7
import PackageDescription

let package = Package(
    name: "postgres-nio",
    platforms: [
        .macOS(.v13),
        .iOS(.v16),
        .watchOS(.v9),
        .tvOS(.v16),
    ],
    products: [
        .library(name: "PostgresNIO", targets: ["PostgresNIO"]),
        .library(name: "_ConnectionPoolModule", targets: ["_ConnectionPoolModule"]),
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-atomics.git", from: "1.2.0"),
        .package(url: "https://github.com/apple/swift-collections.git", from: "1.0.4"),
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.59.0"),
        .package(url: "https://github.com/apple/swift-nio-transport-services.git", from: "1.19.0"),
        .package(url: "https://github.com/apple/swift-nio-ssl.git", from: "2.25.0"),
        .package(url: "https://github.com/apple/swift-crypto.git", "2.0.0" ..< "4.0.0"),
        .package(url: "https://github.com/apple/swift-metrics.git", from: "2.4.1"),
        .package(url: "https://github.com/apple/swift-log.git", from: "1.5.3"),
    ],
    targets: [
        .target(
            name: "PostgresNIO",
            dependencies: [
                .target(name: "_ConnectionPoolModule"),
                .product(name: "Atomics", package: "swift-atomics"),
                .product(name: "Crypto", package: "swift-crypto"),
                .product(name: "Logging", package: "swift-log"),
                .product(name: "Metrics", package: "swift-metrics"),
                .product(name: "NIO", package: "swift-nio"),
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOPosix", package: "swift-nio"),
                .product(name: "NIOTransportServices", package: "swift-nio-transport-services"),
                .product(name: "NIOTLS", package: "swift-nio"),
                .product(name: "NIOSSL", package: "swift-nio-ssl"),
                .product(name: "NIOFoundationCompat", package: "swift-nio"),
            ]
        ),
        .target(
            name: "_ConnectionPoolModule",
            dependencies: [
                .product(name: "Atomics", package: "swift-atomics"),
                .product(name: "DequeModule", package: "swift-collections"),
            ],
            path: "Sources/ConnectionPoolModule"
        ),
        .testTarget(
            name: "PostgresNIOTests",
            dependencies: [
                .target(name: "PostgresNIO"),
                .product(name: "NIOEmbedded", package: "swift-nio"),
                .product(name: "NIOTestUtils", package: "swift-nio"),
            ]
        ),
        .testTarget(
            name: "ConnectionPoolModuleTests",
            dependencies: [
                .target(name: "_ConnectionPoolModule"),
                .product(name: "DequeModule", package: "swift-collections"),
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOConcurrencyHelpers", package: "swift-nio"),
                .product(name: "NIOEmbedded", package: "swift-nio"),
            ]
        ),
        .testTarget(
            name: "IntegrationTests",
            dependencies: [
                .target(name: "PostgresNIO"),
                .product(name: "NIOTestUtils", package: "swift-nio"),
            ]
        ),
    ]
)
