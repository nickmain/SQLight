// Copyright (c) 2026 David N Main

import Foundation
import SQLiteCore

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

        /// The pointer to ["sqlite3_stmt"](https://www.sqlite.org/c3ref/stmt.html) structure for the statement.
        public let statementPtr: OpaquePointer

        fileprivate init(statementPtr: OpaquePointer) {
            self.statementPtr = statementPtr
        }

        /// The number of columns that the statement will return in each result row.
        public var columnCount: Int { Int(sqlite3_column_count(statementPtr)) }

        /// Reset the prepared statement.
        ///
        /// This does not clear any parameters that were previously set.
        ///
        /// See [the sqlite3_reset() function](https://www.sqlite.org/c3ref/reset.html)
        public func reset() throws {
            let rc = sqlite3_reset(statementPtr)
            guard rc == SQLITE_OK else {
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

        /// Perform a step operation to fetch the first or next row of column values.
        ///
        /// - Returns: The step result.
        ///            If this call returns ``StepResult/done`` then ``step()`` should be
        ///            not be called again without calling ``reset()`` first.
        ///
        @discardableResult
        public func step() throws -> StepResult {
            let rc = sqlite3_step(statementPtr)
            if rc == SQLITE_DONE { return .done }
            if rc == SQLITE_ROW { return .row }
            throw Error.result(.fromSQLite(code: rc))
        }

        deinit {
            sqlite3_finalize(statementPtr)
        }
    }
}

public extension SQLight.Connection {

    /// Create a prepared statement.
    ///
    /// - Parameter statement: the single SQL statement to compile.
    func prepare(statement: String) throws -> SQLight.PreparedStatement {

        var ptr: OpaquePointer? = nil
        let rc = sqlite3_prepare_v2(sqlite3ptr, statement, -1, &ptr, nil)

        guard rc == SQLITE_OK else {
            throw SQLight.Error.result(.fromSQLite(code: rc))
        }

        guard let ptr else {
            throw SQLight.Error.message("while preparing statement \(statement)")
        }

        return .init(statementPtr: ptr)
    }
}
