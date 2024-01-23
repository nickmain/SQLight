// swift-tools-version: 5.9
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "SQLight",
    platforms: [.iOS(.v17), .tvOS(.v17), .macOS(.v14)],
    products: [
        // Products define the executables and libraries a package produces, making them visible to other packages.
        .library(
            name: "SQLight",
            targets: ["SQLight"]),
        .library(
            name: "SQLParser",
            targets: ["SQLParser"]),
    ],
    targets: [
        // Targets are the basic building blocks of a package, defining a module or a test suite.
        // Targets can depend on other targets in this package and products from dependencies.
        .target(
            name: "SQLight",
            dependencies: ["SQLParser"]),
        .testTarget(
            name: "SQLightTests",
            dependencies: ["SQLight"],
            resources: [.copy("Resources/databases")]),

        .target(
            name: "SQLParser"),
        .testTarget(
            name: "SQLParserTests",
            dependencies: ["SQLParser"]),
    ]
)
