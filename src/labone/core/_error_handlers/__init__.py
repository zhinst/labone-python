"""Subpackage for handling errors.

The package provides helper functionality to convert errors that
happens while assembling a request or after receiving a successful
response, which has an error status.
"""

from labone.core._error_handlers.schema_errors import convert_dynamic_schema_error

__all__ = ["convert_dynamic_schema_error"]
