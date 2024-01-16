# ``SQLight/SQLight/Connection``

## Topics

### Creating a database connection

- ``createInMemoryDatabase()``
- ``open(file:option:)``

### Registering SQL functions implemented in Swift

- ``createFunction(named:numArgs:funcBody:)``
- ``createFunction(named:numArgs:factory:)``

### Executing SQL

- ``execute(sql:callback:)``
- ``prepare(statement:)``
