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
    ],
    targets: [
        .executableTarget(
            name: "ConnectionPoolBenchmarks",
            dependencies: [
                .product(name: "_ConnectionPoolModule", package: "postgres-nio"),
                .product(name: "_ConnectionPoolTestUtils", package: "postgres-nio"),
                .product(name: "Benchmark", package: "package-benchmark"),
            ],
            path: "Benchmarks/ConnectionPoolBenchmarks",
            plugins: [
                .plugin(name: "BenchmarkPlugin", package: "package-benchmark")
            ]
        ),
    ]
)
