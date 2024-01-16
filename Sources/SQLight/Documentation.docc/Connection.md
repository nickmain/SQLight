# ``SQLight/SQLight/Connection``

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
