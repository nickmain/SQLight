// Copyright (c) 2024 David N Main

import Foundation
import SQLite3

public extension SQLight {

    /// A SQLite3 database connection.
    ///
    /// Once opened, a database connection will be closed when the instance is deinitialized.
    ///
    class Connection {

        /// The options that can be used when opening a database file
        public enum OpenOption {
            /// Open as read-only, the file must already exist or an error is thrown
            case readOnly

            /// Open as read and write, the file must already exist or an error is thrown
            case readWrite

            /// Open as read and write, the file will be created if it does not exist
            case create
        }

        /// The special file name for an in-memory database.
        ///
        /// Use ``createInMemoryDatabase()`` to create an in-memory database.
        public static let inMemoryDatabase = ":memory:"

        /// The pointer to the ["sqlite3"](https://www.sqlite.org/c3ref/sqlite3.html) structure for the connection.
        public let sqlite3ptr: OpaquePointer

        /// The database connection name - either the file path or ``inMemoryDatabase``
        public let name: String

        private init(sqlite3ptr: OpaquePointer, dbName: String) {
            SQLight.logger.debug("Opening database: \(dbName, privacy: .public)")
            self.sqlite3ptr = sqlite3ptr
            self.name = dbName
        }

        /// Perform an immediate flush to disk.
        ///
        /// See ["sqlite3_db_cacheflush"](https://www.sqlite.org/c3ref/db_cacheflush.html) for details.
        public func flushCache() {
            SQLite3.sqlite3_db_cacheflush(sqlite3ptr)
        }

        /// Open or create a database with a file path.
        ///
        /// - Parameters:
        ///   - path: the file to open or create
        ///   - option: the read-write-create option to use, default is ``OpenOption/create``
        ///
        public static func open(file path: String, option: OpenOption = .create) throws -> Connection {

            var ptr: OpaquePointer? = nil

            let resultCode: Int32

            switch option {
            case .readOnly:  resultCode = SQLite3.sqlite3_open_v2(path, &ptr, SQLite3.SQLITE_OPEN_READONLY, nil)
            case .readWrite: resultCode = SQLite3.sqlite3_open_v2(path, &ptr, SQLite3.SQLITE_OPEN_READWRITE, nil)
            case .create:    resultCode = SQLite3.sqlite3_open(path, &ptr)
            }

            guard resultCode == SQLite3.SQLITE_OK else {
                throw Error.result(.fromSQLite(code: resultCode))
            }

            guard let ptr else {
                throw Error.message("while opening database connection for \(path)")
            }

            return .init(sqlite3ptr: ptr, dbName: path)
        }

        /// Create a purely in-memory database
        public static func createInMemoryDatabase() throws -> Connection {
            var ptr: OpaquePointer? = nil

            let resultCode = SQLite3.sqlite3_open(Self.inMemoryDatabase, &ptr)
            guard resultCode == SQLite3.SQLITE_OK else {
                throw Error.result(.fromSQLite(code: resultCode))
            }

            guard let ptr else {
                throw Error.message("while opening memory database connection")
            }

            return .init(sqlite3ptr: ptr, dbName: "in_memory:\(ptr.debugDescription)")
        }

        deinit {
            let name = self.name
            SQLight.logger.debug("Closing database @ \(name, privacy: .public)")
            SQLite3.sqlite3_close(sqlite3ptr)
        }
    }
}
