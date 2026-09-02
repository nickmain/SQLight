// Copyright (c) 2026 David N Main

import SQLiteCore
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
    public static var versionString: String { SQLITE_VERSION }

    /// The SQLite version number
    public static var versionNumber: Int { Int(SQLITE_VERSION_NUMBER) }

    /// The source id and time of the SQLite build
    public static var sourceId: String { SQLITE_SOURCE_ID }

    /// Whether the SQLite3 library was compiled with thread-safety mechanisms (mutexes)
    public static var isThreadsafe: Bool { sqlite3_threadsafe() != 0 }

    private init() {}
}
