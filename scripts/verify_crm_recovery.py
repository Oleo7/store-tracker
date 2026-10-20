"""Offline recovery rehearsal. Never connects to or writes to Google Sheets.

The input is a private snapshot, the output contains only counts/checksums.
This tests serialization and preservation of post-backup state. It does not
establish a complete production journal or an atomic live backup.
"""
import argparse
import copy
import hashlib
import json
from pathlib import Path


def digest(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,ensure_ascii=False).encode()).hexdigest()


def restore_preserving_later(backup, latest, expected_latest_hash):
    """A current, stopped-writer export wins over an earlier backup.

    Refuse changed inputs. Do not guess how to merge CRM row deletions, edits
    or event ordering; recover forward from the complete latest export.
    """
    if digest(latest) != expected_latest_hash:
        raise ValueError("Latest export changed during recovery")
    if not set(backup["sheets"]) <= set(latest["sheets"]):
        raise ValueError("Latest export is incomplete")
    return copy.deepcopy(latest)


def rehearsal(snapshot):
    restored = json.loads(json.dumps(snapshot))
    if digest(snapshot) != digest(restored):
        raise AssertionError("Backup round trip differs")
    latest = copy.deepcopy(restored)
    for name in ["sales_activities","planned_activities","customer_state_events"]:
        sheet = latest["sheets"].setdefault(name,{"values":[["synthetic_event_id"]]})
        sheet["values"].append(["synthetic-post-backup-"+name])
    recovered=restore_preserving_later(restored,latest,digest(latest))
    assert digest(recovered)==digest(latest)
    assert digest(recovered)!=digest(restored)
    try:
        restore_preserving_later(restored,latest,digest(restored))
    except ValueError:
        pass
    else:
        raise AssertionError("Conflicting export accepted")
    return {"sheets":len(snapshot["sheets"]),"snapshot_sha256":digest(snapshot),
            "backup_roundtrip_equal":True,"later_synthetic_events_preserved":3,
            "conflict_rejected":True,"production_journal_verified":False,
            "strategy":"stop/drain all writers; preserve latest complete export; compatible v3 forward recovery"}


if __name__ == "__main__":
    p=argparse.ArgumentParser();p.add_argument("--snapshot",required=True);p.add_argument("--output",required=True)
    args=p.parse_args()
    if Path(args.snapshot).resolve()==Path(args.output).resolve():
        p.error("Output must not overwrite snapshot")
    result=rehearsal(json.loads(Path(args.snapshot).read_text(encoding="utf-8")))
    Path(args.output).write_text(json.dumps(result,indent=2),encoding="utf-8")
    print(json.dumps(result))
