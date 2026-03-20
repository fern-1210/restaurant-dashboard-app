"""
Local SQLite "warehouse" package.

This folder exists so the project can grow from "one script" into a small,
reusable set of building blocks:
- connect to SQLite in one consistent way
- create tables / indexes (schema)
- load trusted datasets (revenue first)

Keeping these helpers in a package makes the runnable scripts smaller and easier
to read while you learn.
"""

