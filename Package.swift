// swift-tools-version:5.0
import PackageDescription

let package = Package(
    name: "nio-postgres",
    products: [
        .library(name: "NIOPostgres", targets: ["NIOPostgres"]),
        .executable(name: "NIOPostgresBenchmark", targets: ["NIOPostgresBenchmark"]),
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-nio.git", .branch("master")),
        .package(url: "https://github.com/apple/swift-nio-ssl.git", .branch("master")),
    ],
    targets: [
        .target(name: "CMD5", dependencies: []),
        .target(name: "NIOPostgres", dependencies: ["CMD5", "NIO", "NIOSSL"]),
        .target(name: "NIOPostgresBenchmark", dependencies: ["NIOPostgres"]),
        .testTarget(name: "NIOPostgresTests", dependencies: ["NIOPostgres"]),
    ]
)
