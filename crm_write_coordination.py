"""Repository-level entry point for the shared web/job coordination module.

The canonical implementation lives inside web-app so Render's existing rootDir
contains it. Alias the module, rather than copying globals: all imports in a
process must share the same lease and reentrant lock.
"""
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
import sys

_spec = spec_from_file_location(__name__, Path(__file__).resolve().parent / 'web-app' / 'crm_write_coordination.py')
_module = module_from_spec(_spec)
sys.modules[__name__] = _module
_spec.loader.exec_module(_module)
