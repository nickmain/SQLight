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
        
        /// The table name
        public let name: String

        /// The schema name or "main" (the default schema) or "temp" (a temporary table)
        public let schema: String

        /// The additional arguments passed in the [create-virtual-table](https://www.sqlite.org/syntax/create-virtual-table-stmt.html) SQL statement
        public let arguments: [String]

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

// extension of sqlite3_vtab to hold Table reference
internal struct SQLiteVTable {
    var sqlite3_vtab: SQLite3.sqlite3_vtab
    weak var table: SQLight.Table?
}
