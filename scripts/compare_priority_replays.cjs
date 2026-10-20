// Compare actual backend replays with the checked-in browser comparators.
// The output contains customer identities: keep it outside a public repository.
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const crypto = require('node:crypto');
const assert = require('node:assert/strict');

const args = {};
for (let index = 2; index < process.argv.length; index += 2) {
  const name = process.argv[index];
  assert(['--new', '--old', '--new-repo', '--old-repo', '--output'].includes(name), `Unknown option: ${name}`);
  assert(process.argv[index + 1], `Missing value for ${name}`);
  args[name.slice(2)] = path.resolve(process.argv[index + 1]);
}
for (const name of ['new', 'old', 'new-repo', 'old-repo', 'output']) assert(args[name], `--${name} is required`);
assert(![args.new, args.old].includes(args.output), 'Output must not overwrite a source replay');
const newer = JSON.parse(fs.readFileSync(args.new, 'utf8'));
const older = JSON.parse(fs.readFileSync(args.old, 'utf8'));
assert.equal(newer.provenance.as_of, older.provenance.as_of, 'Replay times differ');
assert.equal(newer.provenance.snapshot_file_sha256, older.provenance.snapshot_file_sha256, 'CRM snapshots differ');

function extract(source, name) {
  const start = source.indexOf(`function ${name}(`);
  assert(start >= 0, `Missing checked-in browser function ${name}`);
  let end = source.indexOf('{', start), depth = 1;
  for (end++; depth > 0 && end < source.length; end++) {
    if (source[end] === '{') depth++;
    if (source[end] === '}') depth--;
  }
  assert.equal(depth, 0, `Unclosed browser function ${name}`);
  return source.slice(start, end);
}

function sorter(repo, data, isNew) {
  const source = fs.readFileSync(path.join(repo, 'web-app/index.html'), 'utf8');
  const names = isNew
    ? ['sortedModelValue', 'stableCustomerTieBreak', 'compareDescendingMissingLast', 'getWorkPriorityValue', 'contactPrioritySort', 'businessValueSort']
    : ['segmentRank', 'hasPriorityScore', 'prioritySort'];
  const customers = new Map(data.priorities.map(row => [row.customer_id, row]));
  const context = {getCustomerInsight: row => customers.get(row.customer_id) || {}};
  vm.createContext(context);
  vm.runInContext(names.map(name => extract(source, name)).join('\n'), context, {timeout: 5000});
  return {
    compare: context[isNew ? 'contactPrioritySort' : 'prioritySort'],
    business: context.businessValueSort,
    frontend_sha256: crypto.createHash('sha256').update(source).digest('hex'),
  };
}

const newSort = sorter(args['new-repo'], newer, true);
const oldSort = sorter(args['old-repo'], older, false);
const wrap = row => ({...row, customer_segment: row.segment});
const oldById = new Map(older.priorities.map(row => [row.customer_id, wrap(row)]));
const newById = new Map(newer.priorities.map(row => [row.customer_id, wrap(row)]));
assert.equal(oldById.size, older.priorities.length, 'Duplicate old customer IDs');
assert.equal(newById.size, newer.priorities.length, 'Duplicate new customer IDs');
assert.deepEqual([...newById.keys()].sort(), [...oldById.keys()].sort(), 'Different active portfolios');
const sellerNames = [...new Set(newer.priorities.map(row => row.sales_person))].sort();
const result = {
  provenance: {
    new: newer.provenance, old: older.provenance,
    new_frontend_sha256: newSort.frontend_sha256, old_frontend_sha256: oldSort.frontend_sha256,
    comparator: 'Actual checked-in contactPrioritySort, businessValueSort and production prioritySort; Swedish collation.',
    matched_population: 'Identical owner and identical set eligible for unscheduled contact in the new backend, ranked by each comparator.',
    score_meaning: 'All = work priority including relevant booked work; Act = eligible unscheduled contact. Indices are not probabilities.',
  },
  sellers: {},
};

for (const seller of sellerNames) {
  const ns = [...newById.values()].filter(row => row.sales_person === seller);
  const os = [...oldById.values()].filter(row => row.sales_person === seller);
  assert.deepEqual(ns.map(row => row.customer_id).sort(), os.map(row => row.customer_id).sort(), `Owner mismatch: ${seller}`);
  const eligible = row => row.can_start_unscheduled_contact === true && row.contact_priority_raw > 0;
  const ids = new Set(ns.filter(eligible).map(row => row.customer_id));
  const allNew = ns.toSorted(newSort.compare), allOld = os.toSorted(oldSort.compare);
  const actNew = ns.filter(row => ids.has(row.customer_id)).toSorted(newSort.compare);
  const matchedOld = os.filter(row => ids.has(row.customer_id)).toSorted(oldSort.compare);
  const rankMap = rows => new Map(rows.map((row, index) => [row.customer_id, index + 1]));
  const ranks = {allNew: rankMap(allNew), allOld: rankMap(allOld), actNew: rankMap(actNew), matchedOld: rankMap(matchedOld)};
  function project(row) {
    const current = newById.get(row.customer_id), old = oldById.get(row.customer_id);
    const guidance = current.list_guidance || {};
    return {
      customer_id: row.customer_id, customer: current.customer, sales_person: seller, segment: current.segment,
      rank_all_new: ranks.allNew.get(row.customer_id), rank_all_old: ranks.allOld.get(row.customer_id),
      rank_act_new: ranks.actNew.get(row.customer_id) ?? null, rank_act_old_same_population: ranks.matchedOld.get(row.customer_id) ?? null,
      work_score: current.work_priority_score, work_raw: current.work_priority_raw,
      contact_score: current.contact_priority_score, contact_raw: current.contact_priority_raw,
      business_score: current.business_value_score, business_raw: current.business_value_raw,
      old_score: old.priority_score, status: guidance.status_key, reason: guidance.reason_code, action: guidance.action_label,
      can_start_unscheduled_contact: current.can_start_unscheduled_contact,
      can_execute_planned_contact: current.can_execute_planned_contact,
      has_planned_contact: current.has_planned_contact, next_contact_at: guidance.next_contact_at,
      review_only: current.review_only, requires_handling: current.requires_handling,
      requires_review: guidance.requires_review || false,
      verified_deliveries: current.business_verified_delivery_count, lifecycle: current.lifecycle,
      days_since_reached: current.days_since_reached,
      days_since_delivery: current.business_days_since_delivery,
      expected_cycle_days: current.business_expected_cycle_days,
      overdue_days: current.business_overdue_days,
      expected_dfp_per_delivery: current.business_expected_dfp_per_delivery,
      business_confidence: current.business_value_confidence,
      old_reason: old.customer_guidance?.reason_code, old_action: old.customer_guidance?.action_label,
    };
  }
  const newTopIds = new Set(actNew.slice(0, 20).map(row => row.customer_id));
  result.sellers[seller] = {
    active: ns.length, actionable: actNew.length,
    planned: ns.filter(row => row.has_planned_contact).length,
    top20_matched_overlap: matchedOld.slice(0, 20).filter(row => newTopIds.has(row.customer_id)).length,
    top20_all: allNew.slice(0, 20).map(project),
    top20_act: actNew.slice(0, 20).map(project),
    top20_old_same_population: matchedOld.slice(0, 20).map(project),
    top20_old_all: allOld.slice(0, 20).map(project),
    top20_business: ns.toSorted(newSort.business).slice(0, 20).map(project),
    all: allNew.map(project),
  };
}
fs.mkdirSync(path.dirname(args.output), {recursive: true});
fs.writeFileSync(args.output, JSON.stringify(result, null, 2) + '\n');
console.log(JSON.stringify({output: args.output, sellers: Object.fromEntries(Object.entries(result.sellers).map(([name, rows]) => [name, {
  active: rows.active, actionable: rows.actionable, planned: rows.planned,
  top20_matched_overlap: rows.top20_matched_overlap,
  green_in_top20_all: rows.top20_all.filter(row => row.status === 'planned').length,
}]))}, null, 2));
