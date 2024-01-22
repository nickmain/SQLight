// Copyright (c) 2024 David N Main

import Foundation
import SQLite3

public extension SQLight.Table {

    /// An index used by a virtual table query.
    ///
    /// This is returned from the ``Table/bestIndex(info:)`` method to inform SQLight of the cost and
    /// constraint usage determined by the virtual table, given the input constraints.
    ///
    struct Index: Hashable {

        /// The input constraints that resulted in this index instance.
        public let info: Info

        /// The constraints that this index will use when filtering the query.
        ///
        /// Each constraint should only appear once in this array.
        ///
        /// The arguments passed by SQLite to SQLight and then to the ``Cursor/filter(index:arguments:)`` method will
        /// correspond, in count and order, to the constraints in this array.
        ///
        /// Constraints such as ``SQLight/Table/Index/Constraint/Operator/isNotNull`` that do not require an argument can be
        /// included in this array and the corresponding value passed to ``Cursor/filter(index:arguments:)`` will be
        /// ``SQLight/Value/null``.
        ///
        public let constraints: [Constraint]

        /// Set to true if the virtual table guarantees that the result rows will already be in the correct
        /// order specified in the input ``OrderByTerm`` array. This is an optimization and it is safe to leave it
        /// as false.
        public let orderByConsumed: Bool

        /// The relative cost of using this index. SQLite may call ``Table/bestIndex(info:)`` more than once with
        /// different sets of constraints and then choose the index with the lowest cost.
        ///
        /// SQLight documentation states: "The estimatedCost field should be set to the estimated number of disk
        /// access operations required to execute this query against the virtual table". For a virtual table that is not
        /// doing disk i/o some other comparable cost may be used.
        ///
        public let estimatedCost: Double

        /// Estimated number of rows returned. Omit this to let SQLite use its default.
        public let estimatedRows: Int?

        /// Set this to true if the given query constraints will only return zero or one rows. This is an optimization.
        public let zeroOrOneRow: Bool

        /// See the properties for descriptions of each corresponding argument.
        public init(info: Info, constraints: [Constraint] = [], orderByConsumed: Bool = false, estimatedCost: Double = 1000, estimatedRows: Int? = nil, zeroOrOneRow: Bool = false) {
            self.info = info
            self.constraints = constraints
            self.orderByConsumed = orderByConsumed
            self.estimatedCost = estimatedCost
            self.estimatedRows = estimatedRows
            self.zeroOrOneRow = zeroOrOneRow
        }
    }
}

public extension SQLight.Table.Index {

    /// A representation of the input part of the "sqlite3_index_info" structure. This is a set of query constraints.
    ///
    /// Refer to ["The xBestIndex Method"](https://www.sqlite.org/vtab.html#the_xbestindex_method) for more details.
    struct Info: Hashable {

        /// The constraints in the SQL query being analyzed
        public let constraints: [Constraint]

        /// The terms in the order-by clause
        public let orderByTerms: [OrderByTerm]

        /// The bits indicating whether a given column is used in the query
        public let colUsedBits: UInt64

        internal init(constraints: [Constraint], orderByTerms: [OrderByTerm], colUsedBits: UInt64) {
            self.constraints = constraints
            self.orderByTerms = orderByTerms
            self.colUsedBits = colUsedBits
        }
    }

    /// A representation of the "sqlite3_index_orderby" structure.
    ///
    /// Refer to ["The xBestIndex Method"](https://www.sqlite.org/vtab.html#the_xbestindex_method) for more details.
    struct OrderByTerm: Hashable {

        /// The zero-based index of the column the ordering applies to.
        public let columnIndex: Int

        /// Whether the order is descending (otherwise ascending)
        public let isDescending: Bool

        internal init(columnIndex: Int, isDescending: Bool) {
            self.columnIndex = columnIndex
            self.isDescending = isDescending
        }
    }

    /// A representation ofthe "sqlite3_index_constraint" structure.
    ///
    /// Refer to ["The xBestIndex Method"](https://www.sqlite.org/vtab.html#the_xbestindex_method) for more details.
    struct Constraint: Hashable {

        /// The index of this constraint
        public let constraintIndex: Int

        /// The zero-based index of the column the constraint applies to.
        /// A constraint on the row id has an index of -1.
        public let columnIndex: Int

        /// Whether the constraint is usable.
        /// Some of the constraints might not be usable because of the way tables are ordered in a join.
        /// Constraints that are not usable should not be considered when determining the best index to use.
        public let isUsable: Bool

        /// The operator of the constraint.
        public let operation: Operator

        /// The righ hand side of the operator, if any.
        ///
        /// This might only be available if the value is a literal constant in the statement SQL.
        public let argument: SQLight.Value?

        internal init(constraintIndex: Int, columnIndex: Int, isUsable: Bool, operation: Operator, argument: SQLight.Value?) {
            self.constraintIndex = constraintIndex
            self.columnIndex = columnIndex
            self.isUsable = isUsable
            self.operation = operation
            self.argument = argument
        }

        /// The possible SQLite constraint operators
        public enum Operator {
            case unknown
            case equal
            case greaterThan
            case lessOrEqual
            case lessThan
            case greaterOrEqual
            case match
            case like
            case glob
            case regex
            case notEqual
            case isNot
            case isNotNull
            case isNull
            case is_
            case limit
            case offset
            case function
        }
    }
}

extension SQLight.Table.Index.Constraint.Operator {
    static func from(op: Int32) -> Self {
        switch op {
        case SQLite3.SQLITE_INDEX_CONSTRAINT_EQ:        .equal
        case SQLite3.SQLITE_INDEX_CONSTRAINT_GT:        .greaterThan
        case SQLite3.SQLITE_INDEX_CONSTRAINT_LE:        .lessOrEqual
        case SQLite3.SQLITE_INDEX_CONSTRAINT_LT:        .lessThan
        case SQLite3.SQLITE_INDEX_CONSTRAINT_GE:        .greaterOrEqual
        case SQLite3.SQLITE_INDEX_CONSTRAINT_MATCH:     .match
        case SQLite3.SQLITE_INDEX_CONSTRAINT_LIKE:      .like
        case SQLite3.SQLITE_INDEX_CONSTRAINT_GLOB:      .glob
        case SQLite3.SQLITE_INDEX_CONSTRAINT_REGEXP:    .regex
        case SQLite3.SQLITE_INDEX_CONSTRAINT_NE:        .notEqual
        case SQLite3.SQLITE_INDEX_CONSTRAINT_ISNOT:     .isNot
        case SQLite3.SQLITE_INDEX_CONSTRAINT_ISNOTNULL: .isNotNull
        case SQLite3.SQLITE_INDEX_CONSTRAINT_ISNULL:    .isNull
        case SQLite3.SQLITE_INDEX_CONSTRAINT_IS:        .is_
        case SQLite3.SQLITE_INDEX_CONSTRAINT_LIMIT:     .limit
        case SQLite3.SQLITE_INDEX_CONSTRAINT_OFFSET:    .offset
        case SQLite3.SQLITE_INDEX_CONSTRAINT_FUNCTION:  .function
        default: .unknown
        }
    }
}
