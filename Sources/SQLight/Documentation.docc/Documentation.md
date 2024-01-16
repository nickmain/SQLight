# ``SQLight``

A minimal wrapper for using SQLite from Swift.

## Overview

Use the methods of ``SQLight/Connection`` to start using this library.

The original source repository is at [https://github.com/nickmain/SQLight](https://github.com/nickmain/SQLight)

This library deliberately avoids concurrency concerns. SQLite itself will serialize requests
made on a connection from different threads, so everything should be OK. However, this library
assumes that it will be used by code that uses an actor or other such mechanism to manage
concurrency.

## Topics

### Creating a database connection

- ``SQLight/Connection/createInMemoryDatabase()``
- ``SQLight/Connection/open(file:option:)``

### Registering SQL functions implemented in Swift

- ``SQLight/Connection/createFunction(named:numArgs:funcBody:)``
- ``SQLight/Connection/createFunction(named:numArgs:factory:)``

### Executing SQL

- ``SQLight/Connection/execute(sql:callback:)``
- ``SQLight/Connection/prepare(statement:)``
