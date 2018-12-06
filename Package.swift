// swift-tools-version:4.2
import PackageDescription

let package = Package(
    name: "nio-postgres",
    products: [
        .library(name: "NIOPostgres", targets: ["NIOPostgres"]),
        .executable(name: "NIOPostgresBenchmark", targets: ["NIOPostgresBenchmark"]),
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-nio.git", from: "1.0.0"),
        .package(url: "https://github.com/apple/swift-nio-ssl.git", from: "1.0.0"),
    ],
    targets: [
        .target(name: "CMD5", dependencies: []),
        .target(name: "NIOPostgres", dependencies: ["CMD5", "NIO", "NIOOpenSSL"]),
        .target(name: "NIOPostgresBenchmark", dependencies: ["NIOPostgres"]),
        .testTarget(name: "NIOPostgresTests", dependencies: ["NIOPostgres"]),
    ]
)
