"""One-off, guarded builder. Only the resulting four-file diff is released."""
from pathlib import Path
import subprocess

EXPECTED = {
    "web-app/app.py": "4e02f90c8c63f992b7e5a537c843269d8f501204",
    "web-app/priority.py": "d4ee24f76029f55a24cc9888b5123a8f215c1f29",
    "web-app/index.html": "c58c1abe6b2e485514b389a5961405a40c4c2186",
}
contents = {}
for name, sha in EXPECTED.items():
    actual = subprocess.check_output(["git", "rev-parse", f"HEAD:{name}"], text=True).strip()
    if actual != sha:
        raise RuntimeError(f"Unexpected source version: {name}: {actual}")
    contents[name] = Path(name).read_text(encoding="utf-8")


def replace(name, old, new):
    count = contents[name].count(old)
    if count != 1:
        raise RuntimeError(f"Expected one replacement in {name}, found {count}: {old[:120]!r}")
    contents[name] = contents[name].replace(old, new, 1)


priority = "web-app/priority.py"
replace(priority,
    "    index, *, customer_id, customer_key, sales_person, latest_human_contact, now,\n",
    "    index, *, customer_id, customer_key, sales_person, latest_human_contact, now,\n"
    "    owner_user_name=None,\n")
replace(priority,
    '''        row_owner = normalize_customer_key(item["row"].get("sales_person"))
        if row_owner and owner_key and row_owner != owner_key:
            continue
''',
    '''        if owner_user_name is not None:
            # Production resolves the current customer owner through users once
            # per snapshot. Match the booking's canonical login, never its
            # display name (which may be old or formatted differently).
            canonical_owner = normalize_customer_key(owner_user_name)
            booking_owner = normalize_customer_key(item["row"].get("user_name"))
            if not canonical_owner or booking_owner != canonical_owner:
                continue
        else:
            # Compatibility for pure/legacy callers without a users snapshot.
            # An explicit booking login still takes precedence over display text.
            row_owner = normalize_customer_key(
                item["row"].get("user_name") or item["row"].get("sales_person")
            )
            if row_owner and owner_key and row_owner != owner_key:
                continue
''')
replace(priority,
    '''    scoring_version: str = SCORE_VERSION,
    now: datetime | None = None,
) -> list[dict]:''',
    '''    scoring_version: str = SCORE_VERSION,
    now: datetime | None = None,
    planning_owner_user_names: dict[str, str] | None = None,
) -> list[dict]:''')
replace(priority,
    '''            latest_human_contact=contact.get("latest_human_contact_datetime"),
            now=guidance_now,
        )''',
    '''            latest_human_contact=contact.get("latest_human_contact_datetime"),
            now=guidance_now,
            owner_user_name=(
                planning_owner_user_names.get(customer_id, "")
                if planning_owner_user_names is not None else None
            ),
        )''')

app = "web-app/app.py"
replace(app,
    '''PRIORITY_SNAPSHOT_CACHE_TITLES = frozenset({
    "customers_enriched",
    "order_rows",''',
    '''PRIORITY_SNAPSHOT_CACHE_TITLES = frozenset({
    "customers_enriched",
    "users",
    "order_rows",''')
replace(app,
    '''    planned_activity_rows=(),
    responsible=None,
):
    """Calculate the authoritative priority snapshot used by all endpoints."""
    order_features = build_order_features(order_rows)''',
    '''    planned_activity_rows=(),
    responsible=None,
    users=None,
):
    """Calculate the authoritative priority snapshot used by all endpoints."""
    planning_owner_user_names = None
    if users is not None:
        # Reuse the same active, unambiguous name/user_name resolution as other
        # planning flows. Passing users explicitly avoids any per-customer IO.
        planning_owner_user_names = {}
        for customer in customers:
            customer_id = str(customer.get("customer_id") or "").strip()
            owner = canonical_owner_for_customer(None, customer, users=users)
            planning_owner_user_names[customer_id] = str(
                (owner or {}).get("user_name") or ""
            ).strip()
    order_features = build_order_features(order_rows)''')
replace(app,
    '''        planned_activities=planned_activity_rows,
        now=stockholm_now(),
    )''',
    '''        planned_activities=planned_activity_rows,
        now=stockholm_now(),
        planning_owner_user_names=planning_owner_user_names,
    )''')
replace(app,
    '''            "customer": str(row.get("customer") or "").strip(),
            "status": str(row.get("status") or "").strip().casefold(),
            "scheduled_at": planning_datetime_text(row.get("scheduled_at")),''',
    '''            "customer": str(row.get("customer") or "").strip(),
            "user_name": normalize_key(row.get("user_name")),
            "sales_person": normalize_key(row.get("sales_person")),
            "status": str(row.get("status") or "").strip().casefold(),
            "scheduled_at": planning_datetime_text(row.get("scheduled_at")),''')
replace(app,
    '''        priorities, email_snapshot = build_current_priority_snapshot(
            customers=customers,''',
    '''        # Resolve calendar ownership from one cached users snapshot. Old
        # installations without a users sheet retain the exact-match fallback;
        # genuine read errors still surface rather than fabricating status.
        users = None
        if planned_activity_rows:
            try:
                users = get_user_rows(spreadsheet)
            except (WorksheetNotFound, AttributeError):
                pass
        priorities, email_snapshot = build_current_priority_snapshot(
            customers=customers,''')
replace(app,
    '''            today=today,
            planned_activity_rows=planned_activity_rows or (),
        )''',
    '''            today=today,
            planned_activity_rows=planned_activity_rows or (),
            users=users,
        )''')

front = "web-app/index.html"
replace(front,
    '''        if (activityId) {
          await openPlanningView({ date: button.dataset.planDate || "" });''',
    '''        if (activityId) {
          if (!await selectPlanningOwnerForCustomer(customer)) return;
          await openPlanningView({ date: button.dataset.planDate || "" });''')
old = '''  async function openPlanningEditorForCustomer(customer, suggestedDate = "") {
    if (!customer) return;
    const targetDate = planningDateFromKey(suggestedDate)
      ? suggestedDate
      : (planningSelectedDate || planningTodayKey());
    if (userIsAdmin()) {
      if (!planningData?.available_users?.length) {
        await openPlanningView();
      }
      const responsibleKey = normalizeRouteIdentity(customer.sales_person);
      const responsible = (planningData?.available_users || [])
        .map(normalizePlanningUser)
        .find(user => normalizeRouteIdentity(user.name) === responsibleKey);
      if (!responsible) {
        showView("planning");
        showToast("Välj en aktiv säljare i Planering innan kontakten bokas.");
        return;
      }
      if (planningSelectedUserName !== responsible.user_name) {
        planningSelectedUserName = responsible.user_name;
        await loadPlanningWeek();
      }
    }
    openPlanningEditor({
      customer,
      date: targetDate,
    });
  }
'''
new = '''  async function selectPlanningOwnerForCustomer(customer) {
    if (!customer) return false;
    if (!userIsAdmin()) return true;
    if (!planningData?.available_users?.length) {
      await openPlanningView();
    }
    const responsibleKey = normalizeRouteIdentity(customer.sales_person);
    const matches = (planningData?.available_users || [])
      .map(normalizePlanningUser)
      .filter(user => user.user_name && responsibleKey &&
        [user.user_name, user.name].some(value =>
          normalizeRouteIdentity(value) === responsibleKey
        )
      );
    // Match only declared aliases and never select the first of ambiguous names.
    if (matches.length !== 1) {
      showView("planning");
      showToast("Kundens ansvariga säljare kunde inte identifieras entydigt. Kontrollera kundens säljare.");
      return false;
    }
    planningSelectedUserName = matches[0].user_name;
    return true;
  }

  async function openPlanningEditorForCustomer(customer, suggestedDate = "") {
    if (!customer) return;
    const targetDate = planningDateFromKey(suggestedDate)
      ? suggestedDate
      : (planningSelectedDate || planningTodayKey());
    const previousOwner = planningSelectedUserName;
    if (!await selectPlanningOwnerForCustomer(customer)) return;
    if (userIsAdmin() && planningSelectedUserName !== previousOwner) {
      await loadPlanningWeek();
    }
    openPlanningEditor({
      customer,
      date: targetDate,
    });
  }
'''
replace(front, old, new)

for name, content in contents.items():
    Path(name).write_text(content, encoding="utf-8")
print("Applied owner identity and booking match fix to three source files.")
