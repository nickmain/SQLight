// Copyright (c) 2024 David N Main

import XCTest
@testable import SQLight

final class VirtualTableTests: XCTestCase {

    class TestCursor: SQLight.Cursor {
        var currRow = 0
        var testRows = [[SQLight.Value]]()

        override var currentRowId: Int { currRow }
        override var hasCurrentRow: Bool { currRow < testRows.count }

        override func next() -> Bool {
            currRow += 1
            return true
        }

        override func columnValue(at index: Int) -> SQLight.Value? {
            guard currRow < testRows.count else { return nil }
            let row = testRows[currRow]
            guard index < row.count else { return nil }
            return row[index]
        }
    }

    class TestTable: SQLight.Table {
        static var testRows = [[SQLight.Value]]()

        override func openCursor() -> SQLight.Cursor {
            let cursor = TestCursor(table: self)
            cursor.testRows = Self.testRows
            return cursor
        }
    }

    class TestModule: SQLight.Module {
        override func createTable(name: String, schema: String, args: [String]) throws -> SQLight.Table {
            TestTable(name: name, schema: schema, arguments: args)
        }
    }

    func testSanity() throws {
        TestTable.testRows = [
            [.int(1), .double(1.1), .string("One")],
            [.int(2), .double(2.2), .string("Two")],
            [.int(3), .double(3.3), .string("Three")],
            [.int(4), .double(4.4), .string("Four")],
            [.int(5), .double(5.5), .string("Five")],
        ]

        let module = TestModule(name: "TestTables")
        let db = try SQLight.Connection.createInMemoryDatabase()
        try db.register(module: module)
        try db.execute(sql: "CREATE VIRTUAL TABLE foobar USING TestTables( a INTEGER, b REAL, c TEXT )")
        try db.execute(sql: "SELECT * FROM foobar") { num, row in
            print("[\(num)]: \(row)")
            return true
        }
    }
}
