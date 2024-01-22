// Copyright (c) 2024 David N Main

import XCTest
@testable import SQLight

final class VirtualTableTests: XCTestCase {

    static var insertedRows = [[SQLight.Value]]()
    static var updatedRows = [[SQLight.Value]]()

    class TestCursor: SQLight.Cursor {
        var currRow = 0
        var testRows = [[SQLight.Value]]()

        override init(table: SQLight.Table) {
            super.init(table: table)
            print("♦️ \(table.name): cursor init")
        }

        override var currentRowId: Int {
            print("♦️ \(table!.name): currentRowId \(currRow)")
            return currRow
        }
        override var hasCurrentRow: Bool {
            print("♦️ \(table!.name): hasCurrentRow [\(currRow)] \(currRow < testRows.count)")
            return currRow < testRows.count
        }

        override func next() -> Bool {
            currRow += 1
            filterRow()
            print("♦️ \(table!.name): next -> [\(currRow)]: \(currRow < testRows.count ? testRows[currRow] : [])")
            return true
        }

        override func close() {
            print("♦️ \(table!.name): close")
        }

        override func filter(index: SQLight.Table.Index, arguments: [SQLight.Value]) {
            super.filter(index: index, arguments: arguments)

            print("♦️ \(table!.name): filter: \(index.constraints.map { "{\($0.columnIndex): \($0.operation)}" }) <- \(arguments)")
            filterRow()
        }

        // skip to the next row that matches the filter, unless the current one does
        func filterRow() {
            guard let filter else { return }
            while currRow < testRows.count {
                if filter.allows(row: testRows[currRow]) {
                    print("♦️ \(table!.name): filter skipped to [\(currRow)]")
                    return
                }
//                print("♦️ filter skipped row [\(currRow)]: \(testRows[currRow])")
                currRow += 1
            }
            print("♦️ \(table!.name): filter skipped past last")
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
            [.int(6), .double(4.3), .null],
        ]

        override func insert(values: [SQLight.Value]) -> Int? {
            print("♦️ \(name): insert \(values)")
            VirtualTableTests.insertedRows.append(values)
            return nil
        }

        override func update(key: SQLight.Value, newKey: SQLight.Value? = nil, values: [SQLight.Value]) throws {
            if let newKey {
                print("♦️ \(name): update: \(key) -> \(newKey) = \(values)")
            } else {
                print("♦️ \(name): update: \(key) = \(values)")
            }
            // replace rows with the primary key
            VirtualTableTests.updatedRows = VirtualTableTests.updatedRows.map {
                $0[0] == key ? values : $0
            }
        }

        override func delete(key: SQLight.Value) throws {
            print("♦️ \(name): delete \(key)")
            VirtualTableTests.updatedRows = VirtualTableTests.updatedRows.filter {
                $0[0] != key // include except for the delete key
            }
        }

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
            ["b": Optional("5.5"), "a": Optional("5"), "c": Optional("Five")],
            ["b": Optional("4.3"), "a": Optional("6"), "c": nil]
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

    func testInsertSanity() throws {
        Self.insertedRows = []
        let module = TestModule(name: "TestTables")
        let db = try SQLight.Connection.createInMemoryDatabase()
        try db.register(module: module)
        try db.execute(sql: "CREATE VIRTUAL TABLE foobar USING TestTables( a INTEGER PRIMARY KEY, b REAL, c TEXT )")
        try db.execute(sql: """
            INSERT INTO foobar VALUES
                (1, 1.1, "Bar"),
                (2, 2.2, "dslkjfsd"),
                (3, 3.3, "Bar"),
                (4, 4.4, "Bar"),
                (5, 5.5, null)
        """)

        let expected: [[SQLight.Value]] = [
            [.int(1), .double(1.1), .string("Bar")],
            [.int(2), .double(2.2), .string("dslkjfsd")],
            [.int(3), .double(3.3), .string("Bar")],
            [.int(4), .double(4.4), .string("Bar")],
            [.int(5), .double(5.5), .null],
        ]

        XCTAssertEqual(Self.insertedRows, expected)
    }

    func testInsertWhereClauseSanity() throws {
        Self.insertedRows = []
        let module = TestModule(name: "TestTables")
        let db = try SQLight.Connection.createInMemoryDatabase()
        try db.register(module: module)
        try db.execute(sql: "CREATE VIRTUAL TABLE source USING TestTables( a INTEGER PRIMARY KEY, b REAL, c TEXT )")
        try db.execute(sql: "CREATE VIRTUAL TABLE receiver USING TestTables( a INTEGER PRIMARY KEY, b REAL, c TEXT )")
        try db.execute(sql: "INSERT INTO receiver SELECT * FROM source WHERE b > 3 AND b < 5")

        let expected: [[SQLight.Value]] = [
            [.int(3), .double(3.3), .string("Three")],
            [.int(4), .double(4.4), .string("Four")],
            [.int(6), .double(4.3), .null],
        ]

        XCTAssertEqual(Self.insertedRows, expected)
    }

    func testUpdateSanity() throws {
        Self.updatedRows = TestTable.testRows
        let module = TestModule(name: "TestTables")
        let db = try SQLight.Connection.createInMemoryDatabase()
        try db.register(module: module)
        try db.execute(sql: "CREATE VIRTUAL TABLE foobar USING TestTables( a INTEGER PRIMARY KEY, b REAL, c TEXT )")
        try db.execute(sql: "UPDATE foobar SET c = 'hello' WHERE b > 3 AND b < 5")

        let expected: [[SQLight.Value]] = [
            [.int(1), .double(1.1), .string("One")],
            [.int(2), .double(2.0), .string("Two")],
            [.int(3), .double(3.3), .string("hello")], // updated
            [.int(4), .double(4.4), .string("hello")], // updated
            [.int(5), .double(5.5), .string("Five")],
            [.int(6), .double(4.3), .string("hello")]  // updated
        ]

        XCTAssertEqual(Self.updatedRows, expected)
    }

    func testUpdateKeySanity() throws {
        Self.updatedRows = TestTable.testRows
        let module = TestModule(name: "TestTables")
        let db = try SQLight.Connection.createInMemoryDatabase()
        try db.register(module: module)
        try db.execute(sql: "CREATE VIRTUAL TABLE foobar USING TestTables( a INTEGER PRIMARY KEY, b REAL, c TEXT )")
        try db.execute(sql: "UPDATE foobar SET a = a + 10, c = 'hello' WHERE b > 3 AND b < 5")

        let expected: [[SQLight.Value]] = [
            [.int(1), .double(1.1), .string("One")],
            [.int(2), .double(2.0), .string("Two")],
            [.int(13), .double(3.3), .string("hello")], // updated
            [.int(14), .double(4.4), .string("hello")], // updated
            [.int(5), .double(5.5), .string("Five")],
            [.int(16), .double(4.3), .string("hello")]  // updated
        ]

        XCTAssertEqual(Self.updatedRows, expected)
    }

    func testDeleteSanity() throws {
        Self.updatedRows = TestTable.testRows
        let module = TestModule(name: "TestTables")
        let db = try SQLight.Connection.createInMemoryDatabase()
        try db.register(module: module)
        try db.execute(sql: "CREATE VIRTUAL TABLE foobar USING TestTables( a INTEGER PRIMARY KEY, b REAL, c TEXT )")
        try db.execute(sql: "DELETE FROM foobar WHERE b > 3 and b < 4.4")

        let expected: [[SQLight.Value]] = [
            [.int(1), .double(1.1), .string("One")],
            [.int(2), .double(2.0), .string("Two")],
            // deleted
            [.int(4), .double(4.4), .string("Four")],
            [.int(5), .double(5.5), .string("Five")],
            // deleted
        ]

        XCTAssertEqual(Self.updatedRows, expected)
    }
}
