// Copyright (c) 2024 David N Main

import XCTest
@testable import SQLight

final class VirtualTableTests: XCTestCase {

    class TestCursor: SQLight.Cursor {
        var currRow = 0
        var testRows = [[SQLight.Value]]()

        override init(table: SQLight.Table) {
            super.init(table: table)
            print("♦️ cursor init")
        }

        override var currentRowId: Int {
            print("♦️ currentRowId")
            return currRow
        }
        override var hasCurrentRow: Bool {
            print("♦️ hasCurrentRow")
            return currRow < testRows.count
        }

        override func next() -> Bool {
            print("♦️ next")
            currRow += 1
            filterRow()
            return true
        }

        override func close() {
            print("♦️ close")
        }

        override func filter(index: SQLight.Table.Index, arguments: [SQLight.Value]) {
            super.filter(index: index, arguments: arguments)

            print("♦️ filter: \(index.constraints.map { "{\($0.columnIndex): \($0.operation)}" }) <- \(arguments)")  
            filterRow()
        }

        // skip to the next row that matches the filter, unless the current one does
        func filterRow() {
            guard let filter else { return }
            while currRow < testRows.count {
                if filter.allows(row: testRows[currRow]) { return }
                currRow += 1
            }
        }

        override func columnValue(at index: Int) -> SQLight.Value? {
            guard currRow < testRows.count else { return nil }
            let row = testRows[currRow]
            guard index < row.count else { return nil }
            return row[index]
        }
    }

    class TestTable: SQLight.Table {
        static var testRows: [[SQLight.Value]] = [
            [.int(1), .double(1.1), .string("One")],
            [.int(2), .double(2.0), .string("Two")],
            [.int(3), .double(3.3), .string("Three")],
            [.int(4), .double(4.4), .string("Four")],
            [.int(5), .double(5.5), .string("Five")],
        ]

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
        let expected = [
            ["c": Optional("One"), "b": Optional("1.1"), "a": Optional("1")],
            ["b": Optional("2.0"), "a": Optional("2"), "c": Optional("Two")],
            ["b": Optional("3.3"), "a": Optional("3"), "c": Optional("Three")],
            ["b": Optional("4.4"), "a": Optional("4"), "c": Optional("Four")],
            ["b": Optional("5.5"), "a": Optional("5"), "c": Optional("Five")]
        ]

        let module = TestModule(name: "TestTables")
        let db = try SQLight.Connection.createInMemoryDatabase()
        try db.register(module: module)
        try db.execute(sql: "CREATE VIRTUAL TABLE foobar USING TestTables( a INTEGER, b REAL, c TEXT )")
        try db.execute(sql: "SELECT * FROM foobar") { num, row in
            print("[\(num)]: \(row)")
            XCTAssertEqual(row, expected[num-1])
            return true
        }
    }

    func testIndexFilterSanity() throws {
        let expected = [
            ["b": Optional("3.3"), "a": Optional("3"), "c": Optional("Three")],
            ["b": Optional("4.4"), "a": Optional("4"), "c": Optional("Four")],
        ]

        let module = TestModule(name: "TestTables")
        let db = try SQLight.Connection.createInMemoryDatabase()
        try db.register(module: module)
        try db.execute(sql: "CREATE VIRTUAL TABLE foobar USING TestTables( a INTEGER, b REAL, c TEXT )")
        try db.execute(sql: "SELECT * FROM foobar WHERE b > 2 and c NOT NULL AND b < 5") { num, row in
            print("[\(num)]: \(row)")
            XCTAssertEqual(row, expected[num-1])
            return true
        }
    }
}
