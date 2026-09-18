from pathlib import Path
import shutil
import subprocess
from unittest import TestCase, skipUnless


class GuidanceIntegrityFrontendTests(TestCase):
    @skipUnless(shutil.which('node'), 'Node.js required for executing real frontend helpers')
    def test_real_frontend_date_volume_and_filter_contracts(self):
        result = subprocess.run(
            ['node', str(Path(__file__).with_name('guidance_integrity_frontend.cjs'))],
            capture_output=True, text=True, timeout=20,
        )
        self.assertEqual(result.returncode,0,result.stdout+'\n'+result.stderr)
        self.assertIn('PASS',result.stdout)
