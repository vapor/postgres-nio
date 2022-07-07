// swift-tools-version:5.4
import PackageDescription

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
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-atomics.git", from: "1.0.2"),
        .package(url: "https://github.com/apple/swift-nio.git", .branch("main")),
        .package(url: "https://github.com/apple/swift-nio-transport-services.git", from: "1.11.4"),
        .package(url: "https://github.com/apple/swift-nio-ssl.git", from: "2.14.1"),
        .package(url: "https://github.com/apple/swift-crypto.git", "1.0.0" ..< "3.0.0"),
        .package(url: "https://github.com/apple/swift-metrics.git", from: "2.0.0"),
        .package(url: "https://github.com/apple/swift-log.git", from: "1.4.0"),
    ],
    targets: [
        .target(name: "PostgresNIO", dependencies: [
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
        ]),
        .testTarget(name: "PostgresNIOTests", dependencies: [
            .target(name: "PostgresNIO"),
            .product(name: "NIOEmbedded", package: "swift-nio"),
            .product(name: "NIOTestUtils", package: "swift-nio"),
        ]),
        .testTarget(name: "IntegrationTests", dependencies: [
            .target(name: "PostgresNIO"),
            .product(name: "NIOTestUtils", package: "swift-nio"),
        ]),
    ]
)
