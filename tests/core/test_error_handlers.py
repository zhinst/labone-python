"""Tests for `labone.core._error_handlers module."""
import capnp
import pytest
from labone.core import errors
from labone.core._error_handlers import schema_errors


class TestConvertDynamicSchemaError:
    def test_no_prefix(self):
        with pytest.raises(errors.LabOneCoreError, match="test message"):
            schema_errors.convert_dynamic_schema_error(RuntimeError("test message"))

    def test_with_prefix(self):
        msg_prefix = "my error: "
        orig_msg = "test message"
        out_msg = "my error: test message"
        with pytest.raises(errors.LabOneCoreError, match=out_msg):
            schema_errors.convert_dynamic_schema_error(
                RuntimeError(orig_msg),
                msg_prefix=msg_prefix,
            )

    def test_with_kj_exception(self):
        msg_prefix = "my error: "
        orig_msg = "test message"
        err = capnp.lib.capnp.KjException(orig_msg)
        with pytest.raises(errors.LabOneCoreError, match="my error: test message"):
            schema_errors.convert_dynamic_schema_error(
                err,
                msg_prefix=msg_prefix,
            )
