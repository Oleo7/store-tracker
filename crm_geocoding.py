"""Sparse geography patches shared with the enrichment job."""
import math
from gspread.utils import rowcol_to_a1
from crm_write_coordination import crm_write_lock, assert_write_owned

GEO_COLUMNS = ["city_google", "address_google", "address_number_google",
               "postal_code_google", "region_google", "latitude_google", "longitude_google",
               "enriched", "modified"]


def apply_geocoding_results(sheet, source_rows, results, *, sheet_id):
    """Patch only unchanged geography by stable ID; never rewrite customer data."""
    with crm_write_lock(sheet_id):
        values = sheet.get_all_values()
        headers = values[0]
        if any(headers.count(c) != 1 for c in ['customer_id', 'customer', *GEO_COLUMNS]):
            raise ValueError('Geocoding requires unique customer_id and geography columns')
        by_id = {}
        for index, cells in enumerate(values[1:], 2):
            row = dict(zip(headers, cells + [''] * (len(headers)-len(cells))))
            by_id.setdefault(str(row.get('customer_id') or '').strip(), []).append((index, row))
        source_by_id = {}
        for row in source_rows:
            source_by_id.setdefault(str(row.get('customer_id') or '').strip(), []).append(row)
        changes = []
        for cid, geo in results.items():
            if not cid or len(by_id.get(cid, [])) != 1 or len(source_by_id.get(cid, [])) != 1:
                continue
            index, current = by_id[cid][0]
            old = source_by_id[cid][0]
            # A manual geography/name edit or another geocoder wins. Other CRM
            # edits (owner, bans, notes) survive because they are never written.
            if any(str(current.get(c) or '') != str(old.get(c) or '')
                   for c in ['customer', *GEO_COLUMNS]):
                continue
            for column, value in zip(GEO_COLUMNS[:7], geo):
                if value is None or (isinstance(value, float) and not math.isfinite(value)):
                    value = ''
                if column in {'latitude_google', 'longitude_google'} and value != '':
                    value = round(float(value), 7)
                cell = rowcol_to_a1(index, headers.index(column)+1)
                changes.append({'range': f'{cell}:{cell}', 'values': [[value]]})
            for column, value in [('enriched', True), ('modified', '')]:
                cell = rowcol_to_a1(index, headers.index(column)+1)
                changes.append({'range': f'{cell}:{cell}', 'values': [[value]]})
        if changes:
            assert_write_owned()
            sheet.batch_update(changes, value_input_option='RAW')
        return len(changes) // len(GEO_COLUMNS)

