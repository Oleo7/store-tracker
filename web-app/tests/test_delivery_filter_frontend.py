from pathlib import Path
import shutil
import subprocess
from unittest import TestCase, skipUnless


INDEX_PATH = Path(__file__).resolve().parents[1] / "index.html"


class DeliveryFilterFrontendTests(TestCase):
    @skipUnless(shutil.which("node"), "Node.js required for real frontend helpers")
    def test_delivery_dropdown_filter_and_customer_lookup_in_both_api_formats(self):
        script = r'''
const fs = require('node:fs'), vm = require('node:vm'), assert = require('node:assert/strict');
const html = fs.readFileSync(process.argv[1], 'utf8');
const source = name => {
  const start = html.indexOf(`  function ${name}(`);
  assert.ok(start >= 0, name);
  const end = html.indexOf('\n  }', start);
  assert.ok(end > start, name);
  return html.slice(start, end + 4);
};
const context = vm.createContext({assert});
vm.runInContext(`
let insights = {}, searchQuery = '', emailClickNoOrderActive = false;
const listWorkFilter = 'all', FOCUS_LABELS = ['retain', 'reactivate'];
const _sheetValues = {}, chips = [];
const updateChip = key => chips.push(key);
const filterState = Object.fromEntries(
  ['segment','city','region','focus','risk','delivery','latestContact','emailProposal']
    .map(key => [key, new Set()])
);
const customers = [
  {customer_id:'a',customer:'Store A',city:'Stockholm',region_google:'East',customer_segment:'A'},
  {customer_id:'b',customer:'Store B',city:'Malmo',region_google:'South',customer_segment:'B'},
  {customer_id:'c',customer:'Store C',city:'Stockholm',region_google:'East',customer_segment:'B'},
  {customer_id:'d',customer:'No delivery',city:'Malmo',region_google:'South',customer_segment:'C'},
];
const record = month => ({latest_delivery_month:month,customer_risk:'Aktiv',
  customer_guidance:{focus_key:'retain'},email_proposal_recipient_count:1,
  email_proposal_type_label:'Follow up',email_click_without_order:true});
const records = {a:record('2026-09'),b:record('2026-08'),c:record('2026-09'),d:{}};
${['customerInsightKey','getCustomerInsight','buildDropdowns','buildInsightDropdowns',
   'getFilteredCustomers'].map(source).join('\n')}
const ids = () => getFilteredCustomers().map(c => c.customer_id);
for (const format of ['compact', 'legacy']) {
  insights = format === 'compact'
    ? {__by_customer_id:records,__record_defaults:{},__meta:{latest_delivery_month:'2099-01'}}
    : Object.fromEntries(customers.map(c => [customerInsightKey(c.customer),records[c.customer_id]]));
  for (const values of Object.values(filterState)) values.clear();
  buildDropdowns();
  buildInsightDropdowns();
  assert.deepEqual(_sheetValues.delivery,['2026-09','2026-08'],format + ' delivery options');
  assert.ok(chips.includes('delivery'));
  assert.deepEqual(_sheetValues.segment,['A','B','C']);
  assert.deepEqual(_sheetValues.city,['Malmo','Stockholm']);
  assert.deepEqual(_sheetValues.region,['East','South']);
  assert.deepEqual(ids(),['a','b','c','d']);
  for (const c of customers) assert.equal(getCustomerInsight(c),records[c.customer_id]);
  assert.equal(getCustomerInsight({customer_id:'missing',customer:'Missing'}).latest_delivery_month,undefined);
  filterState.delivery.add(_sheetValues.delivery[0]);
  assert.deepEqual(ids(),['a','c'],format + ' selected September');
  filterState.delivery.clear();
  filterState.delivery.add(_sheetValues.delivery[1]);
  assert.deepEqual(ids(),['b'],format + ' selected August');
  filterState.delivery.clear();
  filterState.delivery.add('2026-09');
  for (const [key,value,expected] of [
    ['segment','A',['a']],['city','Malmo',[]],['region','East',['a','c']],
    ['focus','retain',['a','c']],['risk','Aktiv',['a','c']],
    ['emailProposal','Follow up',['a','c']],
  ]) {
    filterState[key].add(value);
    assert.deepEqual(ids(),expected,format + ' combined ' + key);
    filterState[key].clear();
  }
  searchQuery = 'store c';
  assert.deepEqual(ids(),['c']);
  searchQuery = '';
  filterState.delivery.clear();
}
insights = {__by_customer_id:records,'store a':record('1999-01')};
assert.equal(getCustomerInsight({customer_id:'b',customer:'Store A'}),records.b,
  'customer ID must take precedence over a colliding name');
assert.equal(getCustomerInsight({customer_id:'missing',customer:'Store A'}).latest_delivery_month,
  undefined,'missing IDs must not borrow another customer history');
insights = {'store a':records.a};
assert.equal(getCustomerInsight({customer:'  STORE  A  '}),records.a,'normalized legacy lookup');
`, context);
'''
        result = subprocess.run(
            ["node", "-e", script, str(INDEX_PATH)], capture_output=True,
            text=True, encoding="utf-8", timeout=20,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
