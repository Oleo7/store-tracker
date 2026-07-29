# Release-QA – Polarbär Sales-planering

Datum: 2026-07-29
Status: **inte redo för produktion – extern staging och databeslut återstår**

## Implementerat

### Fail-closed låsning och drift

- Planeringsmutationer delar ett processlokalt `threading.RLock`.
- Pilot, staging och produktion kräver känd konfiguration med exakt en
  worker och en applikationsinstans.
- `PLANNING_DISTRIBUTED_LOCK_URL` accepteras inte som säkerhetssignal när
  något distribuerat lås inte finns.
- Startkommandots `--workers` jämförs med `WEB_CONCURRENCY`.
- `/health` är publik och svarar 200 när låset är säkert, annars 503.
- Health-svaret innehåller `mode`, `worker_count`, `safe` och `reason`.
- Långsamma kund- och Routes-läsningar sker före mutationslåset när de inte
  måste ingå i den atomiska read/compare/write-sektionen.
- Gunicorn, Procfile och Render-konfiguration är låsta till en worker och en
  instans. Sessionscookies är Secure i staging/produktion och en explicit
  `FLASK_SECRET_KEY` krävs.

### Kundidentitet

- Nya planeringsaktiviteter kräver `customer_id`.
- Route-preview signerar kund-ID, nummer, namn, adress/ort, radcache,
  utgångstid och planfingerprint.
- Route-apply accepterar aldrig radnummer som identitet och läser aldrig kund
  via rad när ID saknas.
- Äldre preview utan kund-ID stoppas med
  `route_preview_expired_or_legacy`.
- Frontend binder i ordningen kund-ID, unikt kundnummer, unikt
  namn+adress/ort. Osäker bindning visar:
  `Kunden kunde inte bindas säkert. Ladda om eller kontakta administratör.`
- Den äldre kontaktloggens URL-baserade namnmatchning bevaras endast när
  namnet är unikt; dubbla namn stoppas.

### Stagingmigrering

- `scripts/planning_release_migration.py` är staging-only och saknar
  produktionsfallback.
- Dry-run granskar master-ID, planeringsschema, dubbla aktivitets-ID och
  kundreferenser i kontakt-, mejl-, order- och planeringsark.
- Apply skapar eller ordnar exakt 26 planeringskolumner, skriver bara tomma
  säkert matchade kund-ID och är idempotent.
- Mejlrecipienter kan backfillas säkert via det befintliga `email_id`-sambandet
  till mejlmeddelandet.
- Befintliga UUID:n skrivs aldrig över och olösta poster gissas inte.

### Routes och driftkostnad

- Kandidater begränsas före första provideranropet.
- Obligatoriska stopp behålls; frivilliga väljs deterministiskt efter poäng
  och radcache.
- Logg och payload redovisar kandidater före/efter, matrispar, cacheträffar,
  provideranrop, tid och slutligt antal stopp utan nycklar.
- Pilot/staging/produktion kräver en separat `GOOGLE_ROUTES_API_KEY` och får
  inte falla tillbaka till webbläsarens Maps-nyckel.

## Automatiska tester

Senaste fulla körning före rapport:

```text
Ran 285 tests
OK
```

Sviten omfattar CRM, prioritering, kontaktlogg, order-/kundsynk,
påminnelsemejl, route-solver/provider, adminbehörighet, frontendkontrakt,
planerings-API och stagingmigrering.

Särskilda releasefall:

- två samtidiga revisioner: exakt en vinner och en får 409;
- två samtidiga skapanden för olika kunder: båda sparas utan kollision;
- rad infogad ovanför kund och omsorterad master: rätt kund binds via ID;
- saknat/okänt ID, äldre preview och dubbla namn stoppas utan skrivning;
- migrering dry-run/apply/andra apply är idempotent;
- kandidatcap sker före första provideranrop;
- startup och health är fail-closed vid okänd/fel worker/instans.

Felstackar som syns under testsuiten är avsiktligt injicerade partial-save-,
timeout- och retryfall; respektive tester passerar.

## Browser-QA

Isolerad in-memory Sheet användes; ingen produktion skrevs.

- Desktop 1280×720: kalender, agenda och backlog utan overflow.
- Mobil 390×844: samma flöden utan horisontell overflow.
- Inloggning som Olle.
- Skapa aktivitet för Butik B och öppna detaljer via kund-ID.
- Lokal GPS-harness, route-preview och route-apply.
- Nästa veckas tomläge.
- Inga console errors eller warnings.

## Skrivskyddad audit av nuvarande produktions-Sheet

Ingen data ändrades.

- 2 089 masterkunder.
- 0 tomma master-ID.
- 1 ogiltigt master-ID, rad 2090.
- 0 dubbla master-ID.
- `planned_activities` har rätt 26-kolumnsschema och 0 dubbla aktivitets-ID.
- Säkra möjliga backfills:
  - `email_messages`: 11;
  - `email_recipients`: 14;
  - `order_rows`: 217.
- Olösta poster som migreringen vägrar gissa:
  - `sales_activities`: 3;
  - `email_messages`: 2;
  - `email_recipients`: 2;
  - `order_rows`: 159.

## Kvarvarande release-gates

1. En full stagingkopia av CRM-Sheeten måste uttryckligen godkännas eftersom
   kopian duplicerar känslig CRM-data i Google Drive.
2. Master-ID på rad 2090 måste beslutas och rättas i staging först; verktyget
   skriver inte över ett befintligt ogiltigt ID automatiskt.
3. Olösta historiska referenser måste granskas eller uttryckligen lämnas
   omigrerade enligt ett affärsbeslut.
4. En separat backendbegränsad `GOOGLE_ROUTES_API_KEY` saknas.
5. Fysisk mobil-GPS över staging-HTTPS kan bara verifieras av användaren.
6. Stagingdeploy, andra idempotenta apply, riktiga Routes-scenarier,
   backup/rollbackövning och produktions-smoke är därför inte utförda.

Produktionssättning ska inte ske innan dessa gates är stängda enligt
`docs/planning-release-runbook.md`.
