// Copyright (c) 2024 David N Main

import SQLite3
import OSLog

/// The namespace for the SQLite3 wrappers.
///
/// Use ``Connection/createInMemoryDatabase()`` or ``Connection/open(file:option:)`` to start
/// using this library.
/// 
public struct SQLight {

    /// Common logger for SQLite operations
    public static let logger = Logger(subsystem: "Epistem", category: "SQLite")

    /// The SQLite version number as a string
    public static var versionString: String { SQLite3.SQLITE_VERSION }

    /// The SQLite version number
    public static var versionNumber: Int { Int(SQLite3.SQLITE_VERSION_NUMBER) }

    /// The source id and time of the SQLite build
    public static var sourceId: String { SQLite3.SQLITE_SOURCE_ID }

    /// Whether the SQLite3 library was compiled with thread-safety mechanisms (mutexes)
    public static var isThreadsafe: Bool { SQLite3.sqlite3_threadsafe() != 0 }

    private init() {}
}
