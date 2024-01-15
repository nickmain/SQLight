// Copyright (c) 2024 David N Main

import Foundation
import SQLite3

public extension SQLight {

    /// A SQLite3 prepared statement.
    ///
    /// Create a PreparedStatement using the ``Connection/prepare(statement:)`` method.
    ///
    /// Parameter indexes are 1-based.
    ///
    /// The prepared statement is finalized when the instance is deinitialized.
    ///
    /// ``step()`` must be called to actually execute the statement.
    ///
    class PreparedStatement {

        internal let statementPtr: OpaquePointer

        fileprivate init(statementPtr: OpaquePointer) {
            self.statementPtr = statementPtr
        }

        /// The number of columns that the statement will return in each result row.
        public var columnCount: Int { Int(SQLite3.sqlite3_column_count(statementPtr)) }

        /// Reset the prepared statement
        ///
        /// See [the sqlite3_reset() function](https://www.sqlite.org/c3ref/reset.html)
        public func reset() throws {
            let rc = SQLite3.sqlite3_reset(statementPtr)
            guard rc == SQLite3.SQLITE_OK else {
                throw Error.result(.fromSQLite(code: rc))
            }
        }

        /// The result of a ``step()`` operation
        public enum StepResult {
            /// A new row was fetched and its column values can be accessed
            case row

            /// The results are complete, there is no new row data
            case done
        }

        /// Perform a step operation for a statement that does not return row data.
        ///
        /// - Returns: the step result.
        ///            If this call returns ``StepResult/done`` then ``step()`` should be
        ///            called again without calling ``reset()`` first.
        ///
        @discardableResult
        public func step() throws -> StepResult {
            let rc = SQLite3.sqlite3_step(statementPtr)
            if rc == SQLite3.SQLITE_DONE { return .done }
            if rc == SQLite3.SQLITE_ROW { return .row }
            throw Error.result(.fromSQLite(code: rc))
        }

        deinit {
            SQLite3.sqlite3_finalize(statementPtr)
        }
    }
}

public extension SQLight.Connection {

    /// Create a prepared statement.
    ///
    /// - Parameter statement: the single SQL statement to compile.
    func prepare(statement: String) throws -> SQLight.PreparedStatement {

        var ptr: OpaquePointer? = nil
        let rc = SQLite3.sqlite3_prepare_v2(sqlite3ptr, statement, -1, &ptr, nil)

        guard rc == SQLite3.SQLITE_OK else {
            throw SQLight.Error.result(.fromSQLite(code: rc))
        }

        guard let ptr else {
            throw SQLight.Error.message("while preparing statement \(statement)")
        }

        return .init(statementPtr: ptr)
    }
}
