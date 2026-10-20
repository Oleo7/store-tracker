import importlib.util
from pathlib import Path
from unittest import TestCase

spec=importlib.util.spec_from_file_location("recovery",Path(__file__).resolve().parents[1]/"scripts/verify_crm_recovery.py")
recovery=importlib.util.module_from_spec(spec)
spec.loader.exec_module(recovery)


class RecoveryTests(TestCase):
    def test_later_edits_deletions_and_new_sheets_are_not_lost(self):
        backup={"sheets":{"contacts":{"values":[["id"],["old"],["deleted"]]}}}
        latest={"sheets":{"contacts":{"values":[["id"],["edited"]]},"events":{"values":[["id"],["new"]]}}}
        self.assertEqual(recovery.restore_preserving_later(backup,latest,recovery.digest(latest)),latest)
        with self.assertRaises(ValueError):
            recovery.restore_preserving_later(backup,latest,recovery.digest(backup))
        incomplete={"sheets":{}}
        with self.assertRaises(ValueError):
            recovery.restore_preserving_later(backup,incomplete,recovery.digest(incomplete))
