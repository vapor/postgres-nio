// swift-tools-version:5.1
import PackageDescription

let package = Package(
    name: "postgres-nio",
    platforms: [
       .macOS(.v10_14)
    ],
    products: [
        .library(name: "PostgresNIO", targets: ["PostgresNIO"]),
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.0.0"),
        .package(url: "https://github.com/apple/swift-nio-ssl.git", from: "2.0.0"),
        .package(url: "https://github.com/apple/swift-metrics.git", from: "1.0.0"),
        .package(url: "https://github.com/apple/swift-log.git", from: "1.0.0"),
    ],
    targets: [
        .target(name: "CMD5", dependencies: []),
        .target(name: "PostgresNIO", dependencies: [
            "CMD5", "Logging", "Metrics", "NIO", "NIOSSL"
        ]),
        .testTarget(name: "PostgresNIOTests", dependencies: [
            "PostgresNIO", "NIOTestUtils"
        ]),
    ]
)
