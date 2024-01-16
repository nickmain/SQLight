# ``SQLight``

A minimal wrapper for using SQLite from Swift.

## Overview

Use the methods of ``SQLight/Connection`` to start using this library.

The original source repository is at [https://github.com/nickmain/SQLight](https://github.com/nickmain/SQLight)

This library deliberately avoids concurrency concerns. SQLite itself will serialize requests
made on a connection from different threads, so everything should be OK. However, this library
assumes that it will be used by code that uses an actor or other such mechanism to manage
concurrency.
