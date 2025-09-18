// swift-tools-version: 6.0

import PackageDescription

let package = Package(
    name: "benchmarks",
    platforms: [
        .macOS("14")
    ],
    dependencies: [
        .package(path: "../"),
        .package(url: "https://github.com/ordo-one/package-benchmark.git", from: "1.29.0"),
        .package(url: "https://github.com/vapor/postgres-kit.git", from: "2.14.0"),
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.82.0"),
    ],
    targets: [
        .executableTarget(
            name: "ConnectionPoolBenchmarks",
            dependencies: [
                .product(name: "_ConnectionPoolModule", package: "postgres-nio"),
                .product(name: "_ConnectionPoolTestUtils", package: "postgres-nio"),
                .product(name: "Benchmark", package: "package-benchmark"),
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOPosix", package: "swift-nio"),
            ],
            path: "Benchmarks/ConnectionPoolBenchmarks",
            plugins: [
                .plugin(name: "BenchmarkPlugin", package: "package-benchmark")
            ]
        ),
        .executableTarget(
            name: "PostgresPerf",
            dependencies: [
                .product(name: "PostgresNIO", package: "postgres-nio"),
                .product(name: "PostgresKit", package: "postgres-kit"),
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOPosix", package: "swift-nio"),
            ],
        )
    ]
)
