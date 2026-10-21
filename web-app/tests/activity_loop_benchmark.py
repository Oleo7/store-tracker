"""Isolated synthetic API timings; never opens external CRM or a road API."""
from pathlib import Path
import os,sys,time,json,statistics
root=Path(sys.argv[1]);sys.path[:0]=[str(root/'web-app/tests'),str(root/'web-app')]
os.environ.update(APP_ENV='test',GOOGLE_CREDENTIALS='{}',FLASK_SECRET_KEY='benchmark-only')
from test_planning import PlanningApiTestCase,NOW
from test_route_save_coordination import CoordinatedRouteSaveTests
result={}
for label,url in [('customers','/customer-insights'),('attention','/planning/attention?user_name=olle'),('calendar','/planning/activities?start=2026-07-20&end=2026-07-30&include_followups=0')]:
 samples=[]
 for _ in range(20):
  test=PlanningApiTestCase();test.setUp()
  try:
   for index in range(40):test.append_planning_row(planned_activity_id=f'p-{index}',scheduled_at=f'2026-07-{20+index%6}T09:00+02:00',created_at='2026-07-01T09:00+02:00')
   start=time.perf_counter();response=test.client.get(url);elapsed=(time.perf_counter()-start)*1000
   assert response.status_code==200,(label,response.get_json());samples.append(elapsed)
  finally:test.tearDown()
 result[label]={'median_ms':round(statistics.median(samples),3),'p95_ms':round(sorted(samples)[18],3),'runs':len(samples)}
for label in ['contact','route_save']:
 samples=[]
 for index in range(20):
  test=CoordinatedRouteSaveTests();test.setUp()
  try:
   token=test.preview() if label=='route_save' else None
   start=time.perf_counter()
   response=test.save(token) if token else test.client.post('/customers/Butik%20A/contacts',json={'customer_id':'11111111-1111-4111-8111-111111111111','date_time':NOW.isoformat(),'contact_channel':'Telefon','result':'Neutral','comment':'Synthetic benchmark','client_request_id':f'benchmark-{index}'})
   elapsed=(time.perf_counter()-start)*1000
   assert response.status_code==200,(label,response.get_json());samples.append(elapsed)
  finally:test.tearDown()
 result[label]={'median_ms':round(statistics.median(samples),3),'p95_ms':round(sorted(samples)[18],3),'runs':len(samples)}
print(json.dumps(result,indent=2))
