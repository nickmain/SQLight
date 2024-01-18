// Copyright (c) 2024 David N Main

import Foundation
import SQLite3

public extension SQLight {

    /// Base class for virtual table cursors
    class Cursor {
     
        internal var sqliteCursor = SQLiteCursor(sqlite3_vtab_cursor: .init())

        internal weak var tableWeakRef: Table?

        /// Get the associated Table
        public var table: Table? { tableWeakRef }

        public init(table: Table) {
            self.tableWeakRef = table
            sqliteCursor.cursor = self // strong ref that will be released when SQLite closes the cursor
        }

        /// Override this to handle a SQLite close-cursor request.
        ///
        /// After this method returns the strong reference to this instance, held by SQLite,
        /// will be released.
        open func close() {
            // nothing
        }

        /// Override to handle a request to advance to the next result row
        ///
        /// - Returns: false if the operation encountered a problem
        open func next() -> Bool {
            true
        }

        /// Whether there is a current result row available. Override in the subclass.
        open var hasCurrentRow: Bool { false }

        /// Get the row id of the current result row. Override in the subclass.
        open var currentRowId: Int { 0 }

        /// Override to return column values for the current result row.
        ///
        /// - Parameter index: the zero-based column index
        ///
        /// - Returns: the column value or nil if there is none
        open func columnValue(at index: Int) -> SQLight.Value? {
            nil
        }
    }
}

internal struct SQLiteCursor {
    var sqlite3_vtab_cursor: SQLite3.sqlite3_vtab_cursor
    var cursor: SQLight.Cursor?
}
