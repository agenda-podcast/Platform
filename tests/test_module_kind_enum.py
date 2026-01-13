from __future__ import annotations

import unittest

from _testutil import ensure_repo_on_path


class TestModuleKindEnum(unittest.TestCase):
    def test_allowed_values(self) -> None:
        ensure_repo_on_path()

        from platform.infra.models import MODULE_KIND_VALUES, ModuleKind, is_valid_module_kind
        from typing import get_args

        self.assertEqual(MODULE_KIND_VALUES, ("transform", "packaging", "delivery", "other"))
        self.assertEqual(get_args(ModuleKind), MODULE_KIND_VALUES)

        for v in MODULE_KIND_VALUES:
            self.assertTrue(is_valid_module_kind(v))

        self.assertFalse(is_valid_module_kind(""))
        self.assertFalse(is_valid_module_kind("PACKAGING"))
        self.assertFalse(is_valid_module_kind("email"))


if __name__ == "__main__":
    unittest.main()
