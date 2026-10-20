"""Route saves against isolated copies; all network I/O is substituted."""
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import os
import threading
from unittest.mock import patch
import fakeredis

from test_planning import PlanningApiTestCase
import test_planning as fixture
import app as a
import crm_write_coordination as coordination


class CoordinatedRouteSaveTests(PlanningApiTestCase):
    route_stop = fixture.PlanningRouteApiTests.route_stop
    route_payload = fixture.PlanningRouteApiTests.route_payload
    create_preview = fixture.PlanningRouteApiTests.create_preview

    def preview(self):
        response, _ = self.create_preview([self.route_stop(2, 1, 21)])
        self.assertEqual(response.status_code, 200, response.get_json())
        return response.get_json()['preview_token']

    def save(self, token, request_id='coordinated-route', client=None):
        return (client or self.client).post('/planning/route-apply', json={
            'preview_token':token, 'client_request_id':request_id})

    def test_staging_write_failure_retries_without_duplicate(self):
        token = self.preview()
        sheet = self.spreadsheet.worksheet(a.PLANNED_ACTIVITIES_SHEET)
        sheet.fail_next_batch_update = RuntimeError('Sheets unavailable')
        self.assertEqual(self.save(token).status_code, 503)
        self.assertEqual(self.save(token).status_code, 200)
        self.assertEqual(len(self.planning_rows()), 1)

    def test_activation_failure_keeps_inactive_rows_and_retry_activates_once(self):
        token = self.preview()
        sheet = self.spreadsheet.worksheet(a.PLANNED_ACTIVITIES_SHEET)
        sheet.fail_batch_update_at = sheet.batch_update_count + 2
        self.assertEqual(self.save(token).status_code, 503)
        self.assertEqual([r['status'] for r in self.planning_rows()], ['route_pending'])
        self.assertEqual(a.public_planned_activity(self.planning_rows()[0])['status'], 'route_pending')
        self.assertEqual(self.save(token).status_code, 200)
        self.assertEqual([r['status'] for r in self.planning_rows()], ['planned'])

    def test_lost_receipt_response_recovers_without_duplicate(self):
        token = self.preview()
        with patch.object(a, 'save_route_proposal', side_effect=RuntimeError('lost receipt')):
            self.assertEqual(self.save(token).status_code, 503)
        self.assertEqual(self.save(token).status_code, 200)
        self.assertTrue(self.save(token).get_json()['duplicate'])
        self.assertEqual(len(self.planning_rows()), 1)

    def test_lost_activation_response_is_read_back_before_retry(self):
        token = self.preview()
        original = a.batch_update_sheet_changes
        def lost(*args, **kwargs):
            original(*args, **kwargs)
            raise RuntimeError('Sheets committed but response lost')
        with patch.object(a, 'batch_update_sheet_changes', side_effect=lost):
            self.assertEqual(self.save(token).status_code, 503)
        self.assertEqual(self.save(token).status_code, 200)
        self.assertEqual(len(self.planning_rows()), 1)

    def test_manual_ban_after_preview_aborts_before_staging(self):
        token = self.preview()
        self.append_contact_row(contact_id='manual-ban',result='Återkom ej',follow_up_date='')
        self.assertEqual(self.save(token).status_code, 409)
        self.assertEqual(self.planning_rows(), [])

    def test_manual_ban_during_staging_never_activates_new_visit(self):
        token = self.preview()
        real = a.append_dict_rows
        def edited(sheet, *args, **kwargs):
            indexes = real(sheet, *args, **kwargs)
            if sheet.title == a.PLANNED_ACTIVITIES_SHEET:
                self.append_contact_row(contact_id='manual-ban',result='Återkom ej',follow_up_date='')
            return indexes
        with patch.object(a, 'append_dict_rows', side_effect=edited):
            response = self.save(token)
        self.assertEqual(response.status_code, 409, response.get_json())
        self.assertEqual([r['status'] for r in self.planning_rows()], ['route_pending'])
        self.assertEqual(self.save(token).status_code, 409)

    def test_manual_ban_during_activation_quarantines_optional_visit(self):
        token = self.preview()
        real = a.batch_update_sheet_changes
        fired = False
        def edited(sheet, *args, **kwargs):
            nonlocal fired
            result = real(sheet, *args, **kwargs)
            if not fired and sheet.title == a.PLANNED_ACTIVITIES_SHEET:
                fired = True
                self.append_contact_row(contact_id='manual-ban',result='Återkom ej',follow_up_date='')
            return result
        with patch.object(a, 'batch_update_sheet_changes', side_effect=edited):
            response = self.save(token)
        self.assertEqual(response.status_code, 409, response.get_json())
        self.assertEqual([r['status'] for r in self.planning_rows()], ['route_pending'])

    def test_manual_calendar_sort_during_staging_aborts_positional_updates(self):
        self.append_planning_row(planned_activity_id='other',customer_row=4)
        token = self.preview()
        real = a.append_dict_rows
        def edited(sheet, *args, **kwargs):
            result = real(sheet, *args, **kwargs)
            if sheet.title == a.PLANNED_ACTIVITIES_SHEET:
                sheet.values[1:] = list(reversed(sheet.values[1:]))
            return result
        with patch.object(a, 'append_dict_rows', side_effect=edited):
            response = self.save(token)
        self.assertEqual(response.status_code, 409, response.get_json())
        other = next(r for r in self.planning_rows() if r['planned_activity_id']=='other')
        self.assertEqual(other['status'], 'planned')
        self.assertEqual(other['customer_id'], '33333333-3333-4333-8333-333333333333')

    def test_phone_created_before_save_aborts(self):
        token = self.preview()
        self.append_planning_row(planned_activity_id='phone',contact_type='phone')
        response = self.save(token)
        self.assertEqual(response.status_code, 409, response.get_json())
        self.assertEqual(len(self.planning_rows()), 1)

    def test_selected_customer_scope_avoids_whole_portfolio_rescore(self):
        token = self.preview()
        real = a.build_current_priority_snapshot
        sizes = []
        def measured(**kwargs):
            sizes.append(len(kwargs['customers']))
            return real(**kwargs)
        with patch.object(a, 'build_current_priority_snapshot', side_effect=measured):
            response = self.save(token)
        self.assertEqual(response.status_code, 200, response.get_json())
        self.assertEqual(sizes, [1])

    def test_two_concurrent_saves_create_one_route(self):
        token = self.preview()
        user = a.get_user_rows(self.spreadsheet)[0]
        with self.client.session_transaction() as session:
            user = dict(session['user'])
        def apply(_):
            with a.app.test_client() as client:
                with client.session_transaction() as session:
                    session['user'] = user
                response = self.save(token,client=client)
                return response.status_code, response.get_json()
        redis = fakeredis.FakeRedis()
        with patch.dict(os.environ, {'CRM_WRITE_LOCK_URL':'redis://isolated','CRM_LOCK_SHEET_KEY':(a.SHEET_ID or 'test-sheet')}), patch.object(coordination, 'redis_client', return_value=redis):
            with ThreadPoolExecutor(max_workers=2) as pool:
                results = list(pool.map(apply, range(2)))
        self.assertEqual([r[0] for r in results], [200,200], results)
        self.assertEqual(sorted(r[1]['duplicate'] for r in results), [False,True])
        self.assertEqual(len(self.planning_rows()), 1)

    def test_lost_lease_prevents_activation_then_retry_recovers(self):
        token = self.preview()
        redis = fakeredis.FakeRedis()
        real = a.append_dict_rows
        def expire(sheet, *args, **kwargs):
            result = real(sheet, *args, **kwargs)
            if sheet.title == a.PLANNED_ACTIVITIES_SHEET:
                redis.delete(coordination.lock_key(a.SHEET_ID))
            return result
        with patch.dict(os.environ, {'CRM_WRITE_LOCK_URL':'redis://isolated','CRM_LOCK_SHEET_KEY':(a.SHEET_ID or 'test-sheet')}), patch.object(coordination,'redis_client',return_value=redis):
            with patch.object(a, 'append_dict_rows', side_effect=expire):
                response = self.save(token)
            self.assertEqual(response.status_code, 503, response.get_json())
            self.assertEqual([r['status'] for r in self.planning_rows()], ['route_pending'])
            self.assertEqual(self.save(token).status_code, 200)

    def test_production_activation_uses_coordinated_flag_without_exclusive_claim(self):
        token = self.preview()
        redis = fakeredis.FakeRedis()
        # Keep APP_ENV=test's sheet identity, but exercise the non-testing gate.
        with patch.dict(a.app.config, {'TESTING':False}), patch.dict(os.environ, {
                'APP_ENV':'development','CRM_WRITE_LOCK_URL':'redis://isolated',
                'CRM_AUTOMATED_WRITERS_COORDINATED':'true','CRM_EXCLUSIVE_WRITER_CONFIRMED':'false',
                'CRM_LOCK_SHEET_KEY':(a.SHEET_ID or 'test-sheet')}), patch.object(coordination,'redis_client',return_value=redis):
            response = self.save(token)
        self.assertEqual(response.status_code, 200, response.get_json())

    def test_stockfiller_sync_and_route_save_share_the_same_lock(self):
        import sys
        from pathlib import Path
        sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'tests'))
        import test_stockfiller_sync as stock_fixture
        import stockfiller_orders.sync as stock
        book = stock_fixture.FakeSpreadsheet({
            'order_rows':[stock.ORDER_COLUMNS],
            'customers_enriched':[['customer','customer_number','customer_id','email','email_last_order','city_google']],
            stock.STATE_SHEET_NAME:[['key','value','updated_at']],
        })
        token = self.preview()
        entered, release = threading.Event(), threading.Event()
        def fetch(*args):
            entered.set()
            if not release.wait(5):
                raise RuntimeError('test synchronization timed out')
            return []
        redis = fakeredis.FakeRedis()
        user = None
        with self.client.session_transaction() as session:
            user = dict(session['user'])
        def save_route():
            with a.app.test_client() as client:
                with client.session_transaction() as session:
                    session['user'] = user
                return self.save(token, client=client).status_code
        with patch.dict(os.environ, {'CRM_WRITE_LOCK_URL':'redis://isolated','CRM_LOCK_SHEET_KEY':(a.SHEET_ID or 'test-sheet')}), patch.object(coordination,'redis_client',return_value=redis), patch.object(stock,'fetch_orders_for_window',side_effect=fetch):
            with ThreadPoolExecutor(max_workers=2) as pool:
                from dataclasses import replace
                config = replace(stock_fixture._config(), sheet_key=(a.SHEET_ID or 'test-sheet'))
                sync = pool.submit(stock.sync_orders, config,
                    stock.SyncWindow('incremental',{},None), spreadsheet=book)
                self.assertTrue(entered.wait(3))
                save = pool.submit(save_route)
                self.assertFalse(save.done())
                release.set()
                self.assertFalse(sync.result(timeout=5).dry_run)
                self.assertEqual(save.result(timeout=5), 200)
        self.assertEqual(len(self.planning_rows()), 1)

    def test_real_adapter_batches_fresh_decision_sources(self):
        from unittest.mock import MagicMock
        book = MagicMock(spec=a.gspread.Spreadsheet)
        sheets = list(self.spreadsheet.sheets.values())
        book.worksheets.return_value = sheets
        titles = [t for t in (*a.ROUTE_DECISION_SHEETS,a.PLANNED_ACTIVITIES_SHEET,a.ROUTE_PROPOSALS_SHEET)
                  if t in self.spreadsheet.sheets]
        book.values_batch_get.return_value = {'valueRanges':[
            {'values':deepcopy(self.spreadsheet.sheets[t].values)} for t in titles]}
        with a.fresh_decision_reads(book):
            customers = a.get_customer_rows(book)
            _, _, plans = a.read_planned_activity_snapshot(book)
            self.assertEqual(len(customers),len(a.get_customer_rows(self.spreadsheet)))
            self.assertEqual(plans,[])
        book.worksheets.assert_called_once()
        book.values_batch_get.assert_called_once()
        book.worksheet.assert_not_called()

    def test_incomplete_bulk_source_response_aborts(self):
        from unittest.mock import MagicMock
        book = MagicMock(spec=a.gspread.Spreadsheet)
        book.worksheets.return_value = list(self.spreadsheet.sheets.values())
        book.values_batch_get.return_value = {'valueRanges':[]}
        with self.assertRaises(ValueError):
            with a.fresh_decision_reads(book):
                self.fail('partial safety read must never authorize writes')

    def test_pending_visit_cannot_be_manually_activated_or_completed(self):
        row=self.append_planning_row(planned_activity_id='pending',status='route_pending',contact_type='phone')
        before=len(self.contact_rows())
        activate=self.client.patch('/planning/activities/pending',json={
            'client_request_id':'activate-pending','expected_revision':row['revision'],'status':'planned'})
        self.assertEqual(activate.status_code,409,activate.get_json())
        complete=self.client.post('/customers/Butik%20A/contacts',json={
            'client_request_id':'complete-pending','planned_activity_id':'pending',
            'contact_channel':'Telefon','result':'Neutral','comment':'must remain inactive',
            'date_time':'2026-07-27 10:12'})
        self.assertEqual(complete.status_code,409,complete.get_json())
        self.assertEqual(len(self.contact_rows()),before)
        cancel=self.client.patch('/planning/activities/pending',json={
            'client_request_id':'cancel-pending','expected_revision':row['revision'],'status':'cancelled'})
        self.assertEqual(cancel.status_code,200,cancel.get_json())
        self.assertEqual(self.planning_rows()[0]['status'],'cancelled')

    def test_actual_booking_with_stale_owner_still_blocks_new_visit(self):
        for channel,when in [('phone','2026-07-28T11:00:00+02:00'),('visit','2026-07-29T11:00:00+02:00')]:
            with self.subTest(channel=channel):
                sheet=self.spreadsheet.worksheet(a.PLANNED_ACTIVITIES_SHEET)
                sheet.values=sheet.values[:1]
                token=self.preview()
                self.append_planning_row(planned_activity_id='stale-owner-booking',contact_type=channel,
                    owner={'user_name':'sofia','name':'Sofia'},scheduled_at=when)
                response=self.save(token)
                self.assertEqual(response.status_code,409,response.get_json())
                self.assertEqual(len(self.planning_rows()),1)

    def test_route_origin_phone_is_still_an_actual_phone_booking(self):
        self.append_planning_row(planned_activity_id='route-phone',source='route',contact_type='phone')
        # A signed preview from before this booking must still be checked.
        with patch.object(a,'route_contact_customer_ids',return_value=set()):
            response,_=self.create_preview([self.route_stop(2,1,21)])
        if response.status_code==200:
            self.assertEqual(self.save(response.get_json()['preview_token']).status_code,409)
        self.assertEqual([r['planned_activity_id'] for r in self.planning_rows()],['route-phone'])

    def test_manually_documented_route_promise_is_never_automatically_cancelled(self):
        for evidence in [{'customer_agreement_confirmed':'Y'},{'note':'Reklamation: utlovad ersättningsleverans'}]:
            with self.subTest(evidence=evidence):
                sheet=self.spreadsheet.worksheet(a.PLANNED_ACTIVITIES_SHEET);sheet.values=sheet.values[:1]
                self.append_planning_row(planned_activity_id='route-promise',source='route',customer_row=4,**evidence)
                token=self.preview()
                before=deepcopy(self.planning_rows())
                response=self.save(token)
                self.assertEqual(response.status_code,409,response.get_json())
                self.assertEqual(self.planning_rows(),before)

    def test_web_app_starts_with_only_the_render_root_directory(self):
        import subprocess
        import sys
        import shutil
        import tempfile
        from pathlib import Path
        web=Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory(prefix='isolated-render-root-') as folder:
            target=Path(folder)/'web-app'
            shutil.copytree(web,target,ignore=shutil.ignore_patterns('tests','__pycache__','.env'))
            program="import sys; from pathlib import Path; sys.path.insert(0,sys.argv[1]); import app,crm_write_coordination; assert Path(crm_write_coordination.__file__).resolve().parent == Path(sys.argv[1]).resolve(); assert app.app is not None"
            env={**os.environ,'APP_ENV':'test','GOOGLE_CREDENTIALS':'{}','FLASK_SECRET_KEY':'isolated-render-root',
                 'CRM_WRITE_LOCK_URL':'','PLANNING_DISTRIBUTED_LOCK_URL':'','CRM_AUTOMATED_WRITERS_COORDINATED':'false'}
            result=subprocess.run([sys.executable,'-I','-c',program,str(target)],cwd=folder,
                                  env=env,capture_output=True,text=True,timeout=20)
            self.assertEqual(result.returncode,0,result.stdout+result.stderr)
