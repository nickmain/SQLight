// Copyright (c) 2024 David N Main

import Foundation
import SQLite3

public extension SQLight {

    /// Base class for virtual table cursors
    class Cursor {
     
        /// A filter used to restrict the results.
        ///
        public struct Filter {
            /// The index that was selected for the query
            public let index: SQLight.Table.Index

            /// The constraint arguments corresponding to the ``SQLight/Table/Index/constraints`` of the Index.
            ///
            /// Some constraint operators, such as ``SQLight/Table/Index/Constraint/Operator/isNotNull``, do not require 
            /// an argument and the corresponding value in this array will be ``SQLight/Value/null``
            public let arguments: [Value]

            public init(index: SQLight.Table.Index, arguments: [Value]) {
                self.index = index
                self.arguments = arguments
            }
        }

        internal var sqliteCursor = SQLiteCursor(sqlite3_vtab_cursor: .init())

        internal weak var tableWeakRef: Table?

        /// Get the associated Table
        public var table: Table? { tableWeakRef }

        /// The query filter, set by a call from SQLite to the ``filter(index:arguments:)`` method.
        public var filter: Filter?

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

        /// Call from SQLite to set the query filter before fetching any results.
        /// This default implementation sets the ``filter-swift.property`` property.
        open func filter(index: Table.Index, arguments: [SQLight.Value]) {
            filter = Filter(index: index, arguments: arguments)
        }
    }
}

public extension SQLight.Cursor.Filter {

    /// Apply the filter to a row to determine if the constraints allow the result.
    ///
    /// Note this is a partial implementation only useful for testing at the moment.
    func allows(row: [SQLight.Value]) -> Bool {
        for (index, constraint) in self.index.constraints.enumerated() {
            let rhs = self.arguments[index]
            let lhs = row[index]

            // return false if any constraint fails
            if !lhs.testConstraint(op: constraint.operation, arg: rhs) {
                return false
            }
        }

        return true
    }
}

internal struct SQLiteCursor {
    var sqlite3_vtab_cursor: SQLite3.sqlite3_vtab_cursor
    var cursor: SQLight.Cursor?
}
