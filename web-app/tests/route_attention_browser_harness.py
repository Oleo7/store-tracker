"""Run only as a local QA server. CRM is an in-memory synthetic copy."""
import os
from pathlib import Path
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
os.environ.update(APP_ENV='test',GOOGLE_CREDENTIALS='{}',FLASK_SECRET_KEY='route-attention-browser-only',
                  CRM_WRITE_LOCK_URL='',PLANNING_DISTRIBUTED_LOCK_URL='',
                  CRM_AUTOMATED_WRITERS_COORDINATED='false',CRM_RISKY_OPERATIONS_STOPPED='false')
WEB=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(WEB))
import app as a
from flask import redirect, session
from test_planning import default_spreadsheet

NOW=datetime(2026,10,20,10,tzinfo=ZoneInfo('Europe/Stockholm'))
book=None


def append(sheet,row):
    sheet.append_row([row.get(h,'') for h in sheet.values[0]])


def reset():
    global book
    book=default_spreadsheet()
    sheet=book.worksheet('customers_enriched')
    sheet.values[2][sheet.values[0].index('customer')]='Coop Sydport Mariestad'
    customer=a.get_customer_by_row(book,3)
    for aid,day,channel in [('august','2026-08-25','phone'),('october','2026-10-02','phone'),('latest','2026-10-19','email')]:
        row=a.build_planned_activity_row(activity_id=aid,owner={'user_name':'sofia','name':'Sofia'},
            customer=customer,contact_type=channel,scheduled_at=day+'T09:00:00+02:00',
            note='Vanlig kunduppföljning',appointment_confirmed=False,created_at='2026-08-01T09:00:00+02:00')
        append(book.worksheet(a.PLANNED_ACTIVITIES_SHEET),row)
    a._sheet_read_cache.clear();a.invalidate_priority_snapshot()
    return {'ok':True}


@a.app.get('/__test__/login')
def route_attention_login():
    reset()
    session['user']=a.public_user(a.find_active_user(book,'sofia'))
    return redirect('/')


@a.app.post('/__test__/reset-route-attention')
def reset_route():
    return reset()


@a.app.post('/__test__/service')
def service():
    row=a.build_planned_activity_row(activity_id='service',owner={'user_name':'sofia','name':'Sofia'},
        customer=a.get_customer_by_row(book,3),contact_type='phone',scheduled_at='2026-10-18T09:00:00+02:00',
        note='Separat reklamation: utlovad ersättningsleverans',customer_agreement_confirmed=True,
        created_at='2026-08-01T09:00:00+02:00')
    append(book.worksheet(a.PLANNED_ACTIVITIES_SHEET),row)
    a._sheet_read_cache.clear();a.invalidate_priority_snapshot()
    return {'ok':True}


@a.app.post('/__test__/ban')
def ban():
    append(book.worksheet('sales_activities'),{'contact_id':'new-ban','customer_id':'22222222-2222-4222-8222-222222222222',
        'customer':'Coop Sydport Mariestad','sales_person':'Sofia','contact_channel':'Telefon','result':'Återkom ej',
        'date_time':'2026-10-20T09:00:00+02:00','comment':'Kunden har förbjudit ny kontakt'})
    a._sheet_read_cache.clear();a.invalidate_priority_snapshot()
    return {'ok':True}


@a.app.post('/__test__/fail-activation')
def fail_activation():
    sheet=book.worksheet(a.PLANNED_ACTIVITIES_SHEET)
    sheet.fail_batch_update_at=sheet.batch_update_count+2
    return {'ok':True}


if __name__=='__main__':
    from test_planning import ConstantRoadProvider
    reset()
    a.app.config.update(TESTING=False,SECRET_KEY='route-attention-browser-only')
    a.stockholm_now=lambda:NOW
    a.stockholm_today=lambda:NOW.date()
    a.get_spreadsheet_with_retry=lambda:book
    a.get_spreadsheet=lambda **kwargs:book
    a.get_route_travel_time_provider=lambda:ConstantRoadProvider()
    a.app.run(host='127.0.0.1',port=5088,threaded=True,use_reloader=False)
