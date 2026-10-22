"""Focused, synthetic coverage for the additive customer stats order detail."""
from datetime import date
from unittest import TestCase
from unittest.mock import patch

from test_planning import PlanningApiTestCase
from stockfiller_orders.sync import flatten_order
import app as application

TODAY = date(2026, 10, 22)
CID = '11111111-1111-4111-8111-111111111111'


def line(reference='future', **extra):
    return {'customer_id': CID, 'Customer': 'Butik A', 'Customer number': 'C-1',
            'Reference': reference, 'Order date': '2026-10-19', 'Delivery date': '2026-11-04',
            'Product': 'Hallon', 'SKU': 'SKU1', 'Total weight': '12', 'Total': '4 320,00',
            'Currency': 'SEK', **extra}


class LatestCustomerOrderTests(TestCase):
    def latest(self, rows):
        return application.build_latest_customer_order(rows, today=TODAY)

    def test_future_delivery_multi_sku_identical_positions_and_unchanged_source(self):
        rows = [line('past', **{'Delivery date': '2026-10-20'}), line(), line(),
                line(Product='Blåbär', SKU='SKU2', **{'Total weight': '6', 'Total': '2160'})]
        original = [dict(row) for row in rows]
        result = self.latest(rows)
        self.assertEqual(result['reference'], 'future')
        self.assertEqual(result['delivery_date'], '2026-11-04')
        self.assertEqual(result['total_dfp'], 30)
        self.assertEqual(result['order_value'], 10800)
        self.assertEqual(result['total_dfp'], sum(application.order_volume(row) for row in rows[1:]))
        self.assertEqual(result['order_value'], sum(application.parse_number_value(row['Total']) for row in rows[1:]))
        self.assertEqual(len(result['lines']), 2)
        self.assertEqual(rows, original)

    def test_identical_physical_positions_are_both_counted(self):
        row = line(**{'Total weight': '6', 'Total': '2160'})
        result = self.latest([row, dict(row)])
        self.assertEqual(result['total_dfp'], 12)
        self.assertEqual(result['order_value'], 4320)
        self.assertEqual(result['lines'], [{'article': 'Hallon', 'sku': 'SKU1', 'dfp': 12}])

    def test_actual_stockfiller_pending_order_wins_without_invented_financial_values(self):
        rows = flatten_order({
            'reference': 'pending-stockfiller', 'buyerName': 'Butik A', 'buyerExternalId': 'C-1',
            'createdAtDateTime': '2026-10-19T10:00:00Z', 'deliveryDate': '2026-11-04', 'currency': 'SEK',
            'orderRows': [{'productSku': 'SKU1', 'productName': 'Hallon', 'unit': 'pieces',
                           'ordered': 12, 'delivered': 0, 'price': 36000}],
        })
        self.assertEqual(len(rows), 1)
        self.assertEqual(application.parse_number_value(rows[0]['Quantity']), 12)
        self.assertEqual(application.order_volume(rows[0]), 0)
        self.assertEqual(application.parse_number_value(rows[0]['Total']), 0)
        original = [dict(row) for row in rows]
        result = self.latest([line('delivered', **{'Delivery date': '2026-10-20'}), *rows])
        self.assertEqual(result['reference'], 'pending-stockfiller')
        self.assertEqual(result['delivery_date'], '2026-11-04')
        self.assertIsNone(result['total_dfp'])
        self.assertIsNone(result['order_value'])
        self.assertIsNone(result['lines'][0]['dfp'])
        self.assertEqual(rows, original)

    def test_future_zero_order_requires_positive_order_and_article_evidence(self):
        base = line(**{'Quantity': '12', 'Total weight': '0', 'Total': '0'})
        for changes in [{'Quantity': ''}, {'Quantity': '0'}, {'Quantity': '-12'}, {'Quantity': 'bad'},
                        {'Order date': ''}, {'Delivery date': ''}, {'Reference': ''}, {'Product': '', 'SKU': ''},
                        {'Delivery date': '2026-10-20'}]:
            with self.subTest(changes=changes):
                self.assertIsNone(self.latest([{**base, **changes}]))

    def test_delivery_then_order_date_then_reference_is_deterministic(self):
        rows = [line('A'), line('Z'), line('older', **{'Order date': '2026-10-18'})]
        self.assertEqual(self.latest(rows), self.latest(list(reversed(rows))))
        self.assertEqual(self.latest(rows)['reference'], 'Z')

    def test_distinct_rows_of_same_sku_are_additive(self):
        result = self.latest([line(), line(**{'Total weight': '6', 'Total': '2160'})])
        self.assertEqual(result['lines'][0]['dfp'], 18)
        self.assertEqual(result['total_dfp'], 18)
        self.assertEqual(result['order_value'], 6480)

    def test_partial_and_unknown_future_values_do_not_invent_totals(self):
        for value in ['', 'not available']:
            with self.subTest(value=value):
                result = self.latest([line(), line(SKU='SKU2', **{'Total weight': value, 'Total': value, 'Quantity': '999'})])
                self.assertIsNone(result['total_dfp'])
                self.assertIsNone(result['order_value'])
                self.assertIsNone(result['lines'][1]['dfp'])
        result = self.latest([line(**{'Total weight': '', 'Total': '', 'Quantity': '999'})])
        self.assertIsNotNone(result)
        self.assertIsNone(result['total_dfp'])
        self.assertIsNone(result['order_value'])

    def test_currencies_are_not_added_together(self):
        result = self.latest([line(), line(SKU='SKU2', Currency='EUR')])
        self.assertEqual(result['currency'], '')
        self.assertIsNone(result['order_value'])
        self.assertEqual(self.latest([line(Currency='EUR')])['currency'], 'EUR')

    def test_invalid_orders_dates_and_commercial_zero_are_not_selected(self):
        for row in [line(Reference=''), line(**{'Delivery date': '2026-02-30'}),
                    line(**{'Delivery date': ''}), line(Total='0'), line(Total=0), line(Total='-100'),
                    line(**{'Total weight': '0'}), line(**{'Total weight': '-3'}),
                    line(**{'Delivery date': '2026-10-01', 'Total': ''})]:
            with self.subTest(row=row):
                self.assertIsNone(self.latest([row]))
        self.assertIsNone(self.latest([]))
        self.assertIsNone(self.latest([line(**{'Order date': ''})])['order_date'])


class LatestOrderStatsApiTests(PlanningApiTestCase):
    def append_order(self, row):
        sheet = self.spreadsheet.worksheet('order_rows')
        sheet.append_row([row.get(key, '') for key in application.ORDER_COLUMNS])

    def test_additive_stats_kpis_unchanged_and_single_order_read(self):
        self.append_order(line()); self.append_order(line())
        self.append_order(line(SKU='SKU2', Product='Blåbär', **{'Total weight': '6', 'Total': '2160'}))
        with patch.object(application, 'get_order_rows', wraps=application.get_order_rows) as read:
            response = self.client.get('/customers/Butik%20A/stats?customer_id=' + CID)
        self.assertEqual(response.status_code, 200, response.get_json())
        data = response.get_json()
        self.assertEqual(read.call_count, 1)
        self.assertEqual(data['total_sales'], 10800)  # Existing historical KPI is unchanged.
        self.assertEqual(data['order_count'], 1)
        self.assertEqual(data['currency'], 'SEK')
        self.assertEqual(data['latest_order_date'], '2026-10-19')
        self.assertEqual(data['latest_order']['order_value'], data['total_sales'])
        self.assertEqual(data['latest_order']['total_dfp'], 30)
        self.assertTrue({'contacts', 'timeline', 'next_follow_up'} <= data.keys())

    def test_same_name_and_reference_cannot_cross_customer_identity_or_access(self):
        sheet = self.spreadsheet.worksheet('customers_enriched')
        sheet.values[2][sheet.values[0].index('customer')] = 'Butik A'
        self.append_order(line())
        self.append_order(line(customer_id='22222222-2222-4222-8222-222222222222',
            **{'Customer number': 'C-2', 'Product': 'Foreign product', 'Total': '99999'}))
        response = self.client.get('/customers/Butik%20A/stats?customer_id=' + CID)
        self.assertEqual(response.status_code, 200, response.get_json())
        self.assertEqual(response.get_json()['latest_order']['order_value'], 4320)
        self.assertNotIn('Foreign product', response.get_data(as_text=True))
        self.assertEqual(self.client.get('/customers/Butik%20A/stats?customer_id=22222222-2222-4222-8222-222222222222').status_code, 404)

    def test_no_order_is_null(self):
        response = self.client.get('/customers/Butik%20A/stats?customer_id=' + CID)
        self.assertEqual(response.status_code, 200, response.get_json())
        self.assertIsNone(response.get_json()['latest_order'])
