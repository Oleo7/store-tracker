"""Run stage-two frontend contracts against the real JavaScript functions."""
from pathlib import Path
import os
import shutil
import subprocess
from unittest import TestCase, skipUnless

@skipUnless(shutil.which("node"), "Node.js required for actual frontend helpers")
class PriorityWorkflowFrontendTests(TestCase):
    def test_planned_email_scope_and_contact_blocks_without_sending(self):
        script = Path(__file__).with_name("priority_email_frontend.cjs")
        result = subprocess.run(["node", str(script)], capture_output=True, text=True, timeout=20)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_priority_obligation_scope_dates_and_races(self):
        script = Path(__file__).with_name("priority_workflow_frontend.cjs")
        for timezone in ("America/Los_Angeles", "Asia/Tokyo"):
            result = subprocess.run(["node", str(script)], capture_output=True, text=True,
                                    timeout=20, env={**os.environ, "TZ": timezone})
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
