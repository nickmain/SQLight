// Copyright (c) 2024 David N Main

import Foundation
import SQLite3

public extension SQLight {

    /// Base class for virtual table implementations
    ///
    /// Note that the ``SQLight/Module`` will keep a strong reference to any instances created in response
    /// to SQLight create or connect requests in order to ensure that the [sqlite3_vtab](https://www.sqlite.org/c3ref/vtab.html) passed to SQLight remains allocated.
    /// Make sure that there are no strong references from Tables back to Modules.
    ///
    class Table {
        
        /// The solution for a given set of query constraints
        public enum IndexSolution {
            case none
            case index(Index)
        }

        // cached Indexes
        private var solutions = [IndexSolution]()
        private var solutionIndices = [Index.Info: Int]()

        /// The table name
        public let name: String

        /// The schema name or "main" (the default schema) or "temp" (a temporary table)
        public let schema: String

        /// The additional arguments passed in the [create-virtual-table](https://www.sqlite.org/syntax/create-virtual-table-stmt.html) SQL statement
        public let arguments: [String]

        // The sqlite3_vtab structure passed back to SQLite
        internal var sqlite3VTab = SQLiteVTable(sqlite3_vtab: .init(pModule: nil, nRef: 0, zErrMsg: nil), table: nil)

        public init(name: String, schema: String, arguments: [String]) {
            self.name = name
            self.schema = schema
            self.arguments = arguments
        }

        /// The SQL create-table statement that will be used when defining the table.
        ///
        /// The default implementation uses the additional arguments verbatim as the column definitions.
        ///
        /// See ["Declare The Schema Of A Virtual Table"](https://www.sqlite.org/c3ref/declare_vtab.html)
        open var declarationSql: String {
            "CREATE TABLE \(schema).\(name) ( \(arguments.joined(separator: ", ")) )"
        }

        /// Determine an index for a query, given a set of input constraints.
        ///
        /// Refer to ["The xBestIndex Method"](https://www.sqlite.org/vtab.html#the_xbestindex_method) for more details.
        ///
        /// SQLite may call this method several times for a single query, with different sets of constraints, in order
        /// to determine the best index to use.
        /// The chosen index will be passed to the ``SQLight/Cursor/filter(index:arguments:)`` method of the query cursor.
        ///
        /// The index solution returned from this method will be cached, keyed by the input constraints.
        /// This method will not be called again if there a cached solution available. See ``bestIndexCaching(info:)`` if
        /// more control over caching is needed.
        ///
        /// This default implementation returns an Index with a default cost and just copies all the usable constraints
        /// into the usage array. This should be overridden.
        ///
        /// - Returns: An index for use with the given query constraints or
        ///            ``IndexSolution/none`` if there is no solution (this may
        ///            cause SQLite to raise a "Generic error - no query solution" error if there are no other
        ///            constraint combinations it can try.)
        ///
        open func bestIndex(info: Index.Info) -> IndexSolution {
            // copy all usable constraints
            var constraints = [Index.Constraint]()
            for constraint in info.constraints where constraint.isUsable {
                constraints.append(constraint)
            }

            return .index(.init(info: info, constraints: constraints))
        }

        /// This calls ``bestIndex(info:)`` and implements the caching layer. Override in order to bypass or control the
        /// caching behavior.
        open func bestIndexCaching(info: Index.Info) -> IndexSolution {
            if let solution = getIndex(for: info) { return solution }

            let solution = bestIndex(info: info)
            addCached(index: solution, for: info)
            return solution
        }

        /// Get a previously computed index solution for a set of query constraints.
        public final func getIndex(for info: Index.Info ) -> IndexSolution? {
            guard let indexIndex = solutionIndices[info], indexIndex < solutions.count else { return nil }
            return solutions[indexIndex]
        }

        /// Clear the cache of indexes
        public final func clearIndexCache() {
            solutions = []
            solutionIndices = [:]
        }

        // Called from xFilter
        internal func getIndex(at index: Int) -> Index? {
            guard index < solutions.count else { return nil }
            return switch solutions[index] {
            case .none: nil
            case .index(let index): index
            }
        }

        /// Add or replace a cached Index for a set of query constraints
        public final func addCached(index: IndexSolution, for info: Index.Info) {
            if let indexIndex = solutionIndices[info], indexIndex < solutions.count {
                solutions[indexIndex] = index
            } else {
                solutionIndices[info] = solutions.count
                solutions.append(index)
            }
        }

        /// Override to handle opening a new cursor.
        open func openCursor() -> Cursor {
            return Cursor(table: self)
        }

        /// Override to handle a disconnect from SQLite.
        ///
        /// This is called when a database connection is finished with a virtual table, such as when the
        /// connection is being closed.
        open func disconnect() {
            SQLight.logger.debug("Disconnect virtual table named \(self.schema).\(self.name)")
        }

        /// Override to handle a destroy request from SQLite.
        ///
        /// This is called when an SQL "DROP TABLE" is executed for the table.
        open func destroy() {
            SQLight.logger.debug("Destroy virtual table named \(self.schema).\(self.name)")
        }
    }
}

// extension of sqlite3_vtab to hold a Table reference
internal struct SQLiteVTable {
    var sqlite3_vtab: SQLite3.sqlite3_vtab
    weak var table: SQLight.Table?
}
