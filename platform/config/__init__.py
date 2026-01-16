"""Platform-level configuration.

This package hosts the single source of truth for platform policies.

Note on naming:
The repository uses a top-level package named 'platform', which shadows the
Python standard library module of the same name. CLI scripts that import this
package should ensure the repository root is placed on sys.path and that the
stdlib 'platform' module is removed from sys.modules if it was preloaded.
"""
