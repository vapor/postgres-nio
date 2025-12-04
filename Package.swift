// swift-tools-version:6.0
import PackageDescription

#if compiler(>=6.1)
let swiftSettings: [SwiftSetting] = [
    .enableUpcomingFeature("NonisolatedNonsendingByDefault")
]
#else
let swiftSettings: [SwiftSetting] = [
    // Sadly the 6.0 compiler concurrency checker finds false positives.
    // To be able to compile, lets reduce the language version down to 5 for 6.0 only.
    .swiftLanguageMode(.v5)
]
#endif

let package = Package(
    name: "postgres-nio",
    platforms: [
        .macOS(.v10_15),
        .iOS(.v13),
        .watchOS(.v6),
        .tvOS(.v13),
    ],
    products: [
        .library(name: "PostgresNIO", targets: ["PostgresNIO"]),
        .library(name: "_ConnectionPoolModule", targets: ["_ConnectionPoolModule"]),
        .library(name: "_ConnectionPoolTestUtils", targets: ["_ConnectionPoolTestUtils"]),
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-atomics.git", from: "1.2.0"),
        .package(url: "https://github.com/apple/swift-collections.git", from: "1.0.4"),
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.81.0"),
        .package(url: "https://github.com/apple/swift-nio-transport-services.git", from: "1.19.0"),
        .package(url: "https://github.com/apple/swift-nio-ssl.git", from: "2.25.0"),
        .package(url: "https://github.com/apple/swift-crypto.git", "3.9.0" ..< "5.0.0"),
        .package(url: "https://github.com/apple/swift-metrics.git", from: "2.4.1"),
        .package(url: "https://github.com/apple/swift-log.git", from: "1.5.3"),
        .package(url: "https://github.com/swift-server/swift-service-lifecycle.git", from: "2.5.0"),
    ],
    targets: [
        .target(
            name: "PostgresNIO",
            dependencies: [
                .target(name: "_ConnectionPoolModule"),
                .product(name: "Atomics", package: "swift-atomics"),
                .product(name: "Crypto", package: "swift-crypto"),
                .product(name: "_CryptoExtras", package: "swift-crypto"),
                .product(name: "Logging", package: "swift-log"),
                .product(name: "Metrics", package: "swift-metrics"),
                .product(name: "NIO", package: "swift-nio"),
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOPosix", package: "swift-nio"),
                .product(name: "NIOTransportServices", package: "swift-nio-transport-services"),
                .product(name: "NIOTLS", package: "swift-nio"),
                .product(name: "NIOSSL", package: "swift-nio-ssl"),
                .product(name: "NIOFoundationCompat", package: "swift-nio"),
                .product(name: "ServiceLifecycle", package: "swift-service-lifecycle"),
            ],
            swiftSettings: swiftSettings + [.enableExperimentalFeature("Lifetimes")]
        ),
        .target(
            name: "_ConnectionPoolModule",
            dependencies: [
                .product(name: "Atomics", package: "swift-atomics"),
                .product(name: "DequeModule", package: "swift-collections"),
            ],
            path: "Sources/ConnectionPoolModule",
            swiftSettings: swiftSettings
        ),
        .target(
            name: "_ConnectionPoolTestUtils",
            dependencies: [
                "_ConnectionPoolModule",
                .product(name: "NIOConcurrencyHelpers", package: "swift-nio"),
            ],
            path: "Sources/ConnectionPoolTestUtils",
            swiftSettings: swiftSettings
        ),
        .testTarget(
            name: "PostgresNIOTests",
            dependencies: [
                .target(name: "PostgresNIO"),
                .product(name: "NIOEmbedded", package: "swift-nio"),
                .product(name: "NIOTestUtils", package: "swift-nio"),
            ],
            swiftSettings: swiftSettings
        ),
        .testTarget(
            name: "ConnectionPoolModuleTests",
            dependencies: [
                .target(name: "_ConnectionPoolModule"),
                .target(name: "_ConnectionPoolTestUtils"),
                .product(name: "DequeModule", package: "swift-collections"),
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOConcurrencyHelpers", package: "swift-nio"),
                .product(name: "NIOEmbedded", package: "swift-nio"),
            ],
            swiftSettings: swiftSettings
        ),
        .testTarget(
            name: "IntegrationTests",
            dependencies: [
                .target(name: "PostgresNIO"),
                .product(name: "NIOTestUtils", package: "swift-nio"),
            ],
            swiftSettings: swiftSettings
        ),
    ]
)
