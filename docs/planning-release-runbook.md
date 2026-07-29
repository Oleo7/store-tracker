# Sales-planering: kontrollerad produktionspilot och rollback

Den 29 juli 2026 godkändes ett medvetet riskbeslut att hoppa över en separat
stagingkopia. Produktionssättning får ändå inte ske förrän en fullständig
backup är verifierad och produktionskommandots skrivskyddade dry-run är utan
blockerande fel.

## 1. Driftskontrakt

Applikationen använder ett processlokalt `RLock` för alla mutationer som kan
ändra planering, kontaktlogg eller planeringskopplade uppföljningar. Pilotens
driftsgräns är därför:

- exakt en Gunicorn-worker;
- exakt en applikationsinstans;
- fyra trådar och 120 sekunders timeout;
- ingen autoskalning;
- publikt health check på `/health`.

Bindande startkommando:

```text
gunicorn --workers 1 --threads 4 --timeout 120 --bind 0.0.0.0:$PORT app:app
```

Obligatoriska produktionsvärden:

```text
APP_ENV=production
WEB_CONCURRENCY=1
APP_INSTANCE_COUNT=1
PRODUCTION_SHEET_KEY=<production>
GOOGLE_CREDENTIALS=<service-account-json>
GOOGLE_ROUTES_API_KEY=<server-only-key>
FLASK_SECRET_KEY=<unique-random-secret>
ROUTE_MATRIX_CANDIDATE_LIMIT=60
ROUTE_MATRIX_TIMEOUT_SECONDS=15
ROUTE_MATRIX_CACHE_TTL_SECONDS=600
```

`PLANNING_DISTRIBUTED_LOCK_URL` får inte sättas. Något distribuerat lås är
inte implementerat och variabeln gör uttryckligen tjänsten osäker.
Sessionscookies är `HttpOnly`, `SameSite=Lax` och `Secure` i staging,
pilot och produktion.

`/health` ska svara HTTP 200 med `ok=true`, `mode=process_local`,
`worker_count=1`, `safe=true` och tom `reason`. HTTP 503 eller `safe=false`
stoppar releasen.

## 2. Roller i piloten

- Säljare: skapa, ändra och slutföra egna aktiviteter samt skapa rutt.
- Administratör: läsa säljares kalendrar och agera för uttryckligen vald
  aktiv säljare.
- Analys/annan roll: ingen skrivåtkomst till planering.

Pilotgruppen ska begränsas till namngivna användare. Behörighetsmatrisen ska
smoke-testas med minst en användare per roll före release.

## 3. Backup och produktionsgrind

1. Skapa en full, tidsstämplad kopia av `CRM_DATABASE` i samma betrodda
   Drive-mapp.
2. Verifiera att kopian går att öppna, har samma worksheetstruktur, saknar
   extern delning och har samma eller mer begränsade behörigheter.
3. Dokumentera backupens namn och Sheet-ID samt releasecommit. Inga
   produktionsskrivningar får göras innan dessa kontroller är gröna.
4. Produktionskommandot kräver `APP_ENV=production`,
   `PRODUCTION_SHEET_KEY`, `--confirm-production`, verifierat `--backup-id`
   och en lokal JSON-rapport. Det använder aldrig `STAGING_SHEET_KEY` eller
   `SHEET_KEY` som fallback.

## 4. Dry-run och migrering

Kör från repository-roten med produktionsmiljön laddad. UUID:t i apply ska
vara exakt det som granskades i dry-run:

```powershell
python scripts/planning_production_release.py `
  --confirm-production `
  --backup-id <verified-backup-id> `
  --repair-master-row 2090 `
  --report outputs/planning-production-dry-run.json

python scripts/planning_production_release.py `
  --confirm-production `
  --backup-id <verified-backup-id> `
  --repair-master-row 2090 `
  --replacement-uuid <uuid-from-dry-run> `
  --apply `
  --report outputs/planning-production-apply.json

python scripts/planning_production_release.py `
  --confirm-production `
  --backup-id <verified-backup-id> `
  --repair-master-row 2090 `
  --replacement-uuid <uuid-from-dry-run> `
  --apply `
  --report outputs/planning-production-idempotency.json
```

Dry-run måste visa:

- unika, giltiga och icke-tomma `customer_id` i `customers_enriched`;
- inga icke-tomma ogiltiga eller föräldralösa kundreferenser;
- inga okända eller tvetydiga kundreferenser i `planned_activities`;
- inga dubbla `planned_activity_id`;
- inga blockerande schemafel.

Historiska tomma kundreferenser utanför `planned_activities` som inte kan
matchas säkert rapporteras som legacyvarningar och lämnas oförändrade. Första
apply reparerar endast det granskade ogiltiga master-ID:t och backfillar
tomma, entydigt matchade `customer_id`. Befintliga giltiga UUID:n skrivs
aldrig över. Andra apply måste ge noll skrivningar, noll nya UUID:n, noll
ytterligare backfills och `idempotent=true`.

Spara samtliga tre JSON-resultat som releasebevis.

## 5. Produktions-smokematris

Testa över HTTPS på desktop och fysisk mobil:

1. Logga in som säljare, admin och läsroll; verifiera behörigheter.
2. Skapa, ändra, flytta, slutför och avbryt aktiviteter.
3. Logga kontakt från kalendern och skapa uppföljning; verifiera exakt en
   kontakt och exakt en uppföljning efter retry.
4. Infoga en kundrad ovanför en planerad kund och sortera sedan
   `customers_enriched`; aktiviteten måste fortfarande öppna rätt kund via
   `customer_id`.
5. Verifiera att okänt eller saknat `customer_id`, äldre route-preview och
   dubbla kundnamn stoppas utan skrivning.
6. Kör rutt med 0, 1, 15 och fler än 60 kandidater. Provideranropet får högst
   ta emot `ROUTE_MATRIX_CANDIDATE_LIMIT` kandidater och loggen ska redovisa
   antal före/efter urval, matrispar, cacheträffar, anrop och total tid utan
   API-nycklar.
7. Verifiera tomlägen, provider-timeout, nekad GPS, utgången preview,
   dubbelklick/retry och samtidig ändring.
8. Verifiera befintlig CRM, prioritering, kontaktlogg, orderimport,
   mejlflöde och dagens rutt.

För fysisk GPS ska Safari/Chrome visa en riktig behörighetsdialog på
stagingens HTTPS-URL. Testa Tillåt, Neka och tidigare blockerad behörighet.

## 6. Backup, release och smoke

1. Skapa och tidsstämpla en komplett produktionskopia direkt före release.
2. Dokumentera Sheet-ID, applikationsversion/commit och ansvarig.
3. Driftsätt den sparade versionen med `render.yaml`.
4. Kontrollera `/health` före inloggning.
5. Smoke-testa inloggning, kundlista, prioritering, kontaktlogg, kalender,
   route-preview/apply och en idempotent retry med pilotkonto.
6. Kontrollera loggar efter fel, dubbla ID:n och oväntad matrisvolym.

## 7. Rollback

Rollback utlöses av osäker health-status, fel kundbindning, dubletter,
dataförlust, trasig kontaktlogg eller återkommande Routes-fel.

1. Stoppa nya pilotmutationer.
2. Rulla tillbaka webbtjänsten till dokumenterad föregående version.
3. Återställ den kompletta Sheet-kopian om produktionsdata har ändrats
   felaktigt; behåll den felaktiga filen skrivskyddad för analys.
4. Starta med exakt en worker och instans.
5. Verifiera `/health`, inloggning, kundläsning och kontaktlogg innan piloten
   öppnas igen.

Byt aldrig Sheet-nyckel till staging som en snabb produktionsrollback.
