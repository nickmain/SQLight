// Copyright (c) 2024 David N Main

import Foundation
import SQLite3

public extension SQLight {

    /// The type for scalar SQL function callback closures.
    ///
    /// Use ``SQLight/Connection/createFunction(named:numArgs:funcBody:)`` to register a
    /// scalar function with a connection
    ///
    /// - Throws: an error message that will be passed to SQLite. Use ``SQLight/Error/message(_:)``
    ///           otherwise the error's localized description will be used.
    ///
    typealias ScalarFunction = (([Value]) throws -> Value?)

    /// The type for aggregate SQL function factories.
    ///
    /// This could be the no-arg initializer of an ``AggregateFunction`` subclass.
    ///
    /// Use ``SQLight/Connection/createFunction(named:numArgs:factory:)`` to register an aggregate
    /// function with a connection.
    typealias AggregateFunctionFactory = (() -> AggregateFunction)

    /// The base for an aggregate SQL function.
    ///
    /// Extend this class to implement an aggregate function and override the
    /// ``stepCall(args:)`` and ``finalCall()`` methods. Use properties in the
    /// subclass to accumulate the step call arguments and then compute the
    /// aggregate result in the final call.
    ///
    /// Use ``SQLight/Connection/createFunction(named:numArgs:factory:)`` to register an aggregate
    /// function with a connection.
    class AggregateFunction {
        public init() {}

        /// Call for each step of the aggregate
        ///
        /// - Throws: an error message that will be passed to SQLite. Use ``SQLight/Error/message(_:)``
        ///           otherwise the error's localized description will be used.

        open func stepCall(args: [Value]) throws {}

        /// Final call to return the aggregate result
        open func finalCall() -> Value { .null }
    }
}

public extension SQLight.Connection {

    /// Register a new scalar SQL function with the database.
    ///
    /// - Parameters:
    ///   - name: the function name visible to SQL
    ///   - numArgs: the number of arguments expected (0...127), nil for the default of any number up to 6.
    ///   - funcBody: the function closure. This is retained until the database is closed.
    ///
    func createFunction(named name: String, numArgs: UInt? = nil, funcBody: @escaping SQLight.ScalarFunction) throws {
        let wrapper = ScalarFunctionWrapper(function: funcBody, name: name)
        let argCount = if let numArgs { Int32(numArgs) } else { Int32(-1) }

        // pass a retained ptr so that SQLite owns the wrapper
        let wrapperPtr = UnsafeMutableRawPointer(Unmanaged.passRetained(wrapper).toOpaque())

        let rc = SQLite3.sqlite3_create_function_v2(self.sqlite3ptr,
                                                    name,
                                                    argCount,
                                                    SQLite3.SQLITE_UTF8,
                                                    wrapperPtr,
                                                    scalarFunction(_:_:_:),
                                                    nil,
                                                    nil,
                                                    releaseWrapper(_:))
        guard rc == SQLite3.SQLITE_OK else {
            throw SQLight.Error.result(.fromSQLite(code: rc))
        }
    }

    /// Register a new aggregate SQL function with the database.
    ///
    /// An aggregate function takes one or more "step" calls, one for each set of row values
    /// to be aggregated, and a "final" call to compute the result. This is implemented by extending the
    /// ``SQLight/AggregateFunction`` class and overriding the ``SQLight/AggregateFunction/stepCall(args:)``
    /// and ``SQLight/AggregateFunction/finalCall()`` methods. Properties of the subclass can be used
    /// to gather the step arguments and compute the aggregate result.
    ///
    /// - Parameters:
    ///   - name: the function name visible to SQL
    ///   - numArgs: the number of arguments expected (0...127), nil for the default of any number up to 6.
    ///   - factory: the factory for instances of the aggregate function. This is retained until the database is closed. This could be the no-args initializer of the ``SQLight/AggregateFunction`` subclass.
    ///
    func createFunction(named name: String, numArgs: UInt? = nil, factory: @escaping SQLight.AggregateFunctionFactory) throws {
        let wrapper = AggregateFunctionWrapper(factory: factory, name: name)
        let argCount = if let numArgs { Int32(numArgs) } else { Int32(-1) }

        // pass a retained ptr so that SQLite owns the wrapper
        let wrapperPtr = UnsafeMutableRawPointer(Unmanaged.passRetained(wrapper).toOpaque())

        let rc = SQLite3.sqlite3_create_function_v2(self.sqlite3ptr,
                                                    name,
                                                    argCount,
                                                    SQLite3.SQLITE_UTF8,
                                                    wrapperPtr,
                                                    nil,
                                                    aggregateStep(_:_:_:),
                                                    aggregateFinal(_:),
                                                    releaseWrapper(_:))
        guard rc == SQLite3.SQLITE_OK else {
            throw SQLight.Error.result(.fromSQLite(code: rc))
        }
    }
}

// Wrapper class for callback closure to allow it to be retained by SQLite
fileprivate class ScalarFunctionWrapper {
    let function: SQLight.ScalarFunction
    let name: String

    init(function: @escaping SQLight.ScalarFunction, name: String) {
        SQLight.logger.debug("Initializing scalar sql function '\(name)'")
        self.function = function
        self.name = name
    }

    deinit {
        let funcName = name
        SQLight.logger.debug("Deinitializing scalar sql function '\(funcName)'")
    }
}

// Wrapper class for AggregateFunction
//
// An instance of this is stored in the sqlite3_user_data() associated with each
// function registration.
// The actual AggregateFunction instance is created for each usage of the function
// in the first step call - using the factory retained here.
// That AggregateFunction instance is retained in the sqlite3_aggregate_context()
// and released in the final call.
fileprivate class AggregateFunctionWrapper {
    let factory: SQLight.AggregateFunctionFactory
    let name: String

    init(factory: @escaping SQLight.AggregateFunctionFactory, name: String) {
        SQLight.logger.debug("Initializing aggregate sql function '\(name)'")
        self.factory = factory
        self.name = name
    }

    deinit {
        let funcName = name
        SQLight.logger.debug("Deinitializing aggregate sql function '\(funcName)'")
    }
}

// Aggregate step function
fileprivate func aggregateStep(_ context: OpaquePointer?, _ argCount: Int32, _ argsPtr: UnsafeMutablePointer<OpaquePointer?>?) {
    guard let context,
          // get or allocate space in the aggregate context for a pointer to an AggregateFunction instance
          let aggContextPtr = SQLite3.sqlite3_aggregate_context(context, Int32(MemoryLayout<UnsafeMutableRawPointer>.size))
        else { return }

    // reinterpret as a pointer to a nullable pointer
    let aggContextValuePtr = aggContextPtr.assumingMemoryBound(to: UnsafeMutableRawPointer?.self)

    let function: SQLight.AggregateFunction
    if let functionPtr = aggContextValuePtr.pointee {
        // subsequent call to step - use existing function instance
        function = Unmanaged.fromOpaque(functionPtr).takeUnretainedValue()
    } else {
        // this is the first call to step - get the factory and create a function instance
        guard let wrapperPtr = SQLite3.sqlite3_user_data(context) else { return }
        let wrapper: AggregateFunctionWrapper = Unmanaged.fromOpaque(wrapperPtr).takeUnretainedValue()
        function = wrapper.factory()

        // store retained reference to the new AggregateFunction instance in the aggregate context
        // this will be released in the associated aggregateFinal() call
        aggContextValuePtr.pointee = UnsafeMutableRawPointer(Unmanaged.passRetained(function).toOpaque())
    }

    // marshall args into array for step call
    var args: [SQLight.Value] = []
    if argCount > 0, let argsPtr {
        for argIndex in 0..<Int(argCount) {
            if let argPtr = argsPtr[argIndex] {
                args.append(.init(from: argPtr))
            }
        }
    }

    do {
        try function.stepCall(args: args)
    } catch SQLight.Error.message(let errorMsg) {
        SQLite3.sqlite3_result_error(context, errorMsg, -1)
    } catch {
        SQLite3.sqlite3_result_error(context, error.localizedDescription, -1)
    }
}

// Aggregate final function
fileprivate func aggregateFinal(_ context: OpaquePointer?) {
    guard let context,
          let aggContextPtr = SQLite3.sqlite3_aggregate_context(context, 0)
        else { return }

    // reinterpret as a pointer to a nullable pointer
    let aggContextValuePtr = aggContextPtr.assumingMemoryBound(to: UnsafeMutableRawPointer?.self)

    if let functionPtr = aggContextValuePtr.pointee {
        // take a retained reference in order to own the function and then release it
        let function: SQLight.AggregateFunction = Unmanaged.fromOpaque(functionPtr).takeRetainedValue()

        let result = function.finalCall()
        result.setReturnValue(for: context)
    }
}

// Callback for function
fileprivate func scalarFunction(_ context: OpaquePointer?, _ argCount: Int32, _ argsPtr: UnsafeMutablePointer<OpaquePointer?>?) {

    guard let context,
          // let dbPtr = SQLite3.sqlite3_context_db_handle(context),  <-- TODO: pass this to callback?
          let wrapperPtr = SQLite3.sqlite3_user_data(context)
    else { return }

    // get wrapper without taking ownership from SQLite
    let wrapper: ScalarFunctionWrapper = Unmanaged.fromOpaque(wrapperPtr).takeUnretainedValue()

    // marshall args into array for callback
    var args: [SQLight.Value] = []
    if argCount > 0, let argsPtr {
        for argIndex in 0..<Int(argCount) {
            if let argPtr = argsPtr[argIndex] {
                args.append(.init(from: argPtr))
            }
        }
    }

    // call the callback and set the SQL result
    do {
        let result = try wrapper.function(args) ?? .null
        result.setReturnValue(for: context)
    } catch SQLight.Error.message(let errorMsg) {
        SQLite3.sqlite3_result_error(context, errorMsg, -1)
    } catch {
        SQLite3.sqlite3_result_error(context, error.localizedDescription, -1)
    }
}

// Callback to release function wrapper
fileprivate func releaseWrapper(_ wrapperPtr: UnsafeMutableRawPointer?) {
    guard let wrapperPtr else { return }

    // take a retained reference in order to own the wrapper and then release it
    let _: AnyObject = Unmanaged.fromOpaque(wrapperPtr).takeRetainedValue()
}
