# Slutrapport – Sales-planering hardening punkt 1–8

Datum: 2026-07-29
Underlag: utvecklingsspecifikationen i Google Docs
Status: lokalt färdig och regressionsverifierad. Produktionsrelease är spärrad tills samma migrering och ruttflöde har verifierats mot en uttryckligen angiven staging-Sheet med riktiga Google Routes-anrop.

## Resultat

De åtta kritiska punkterna är implementerade i applikationskod, synkflöden, datakontrakt och UI. De prioriterade flödena fungerar end-to-end i den lokala browserharnessen:

1. Permanent `customer_id` följer kunden genom kundmaster, order, kontaktlogg, mejllogg och planerade aktiviteter.
2. Ruttplaneringen respekterar fasta bokade besök, telefon/mejl-block, max 15 stopp, mindre än sju timmars total aktiv tid samt start och retur till samma GPS-position.
3. Innehållsredigering av ett ruttstopp konverterar det atomiskt till manuellt; statusändring behåller ruttkällan.
4. Kandidater filtreras globalt över säljare före matrisberäkningen, med neutral varning vid uttryckligt manuellt undantag.
5. Den globala kön skiljer försenade och kommande uppföljningar och sammanfogar inte olika uppföljningar för samma kund.
6. Kundväljaren är en sökbar, tangentbordsstyrd och diakritiktolerant ARIA-combobox.
7. Idempotens använder kanoniska SHA-256-fingerprints, användar-/ägar-scope och heltalsrevisioner.
8. Sheets-mutationer är batchade och `/health` varnar när flera workers körs utan distribuerat lås.

## Väsentliga kodändringar

### Identitet och schema

- `customers_enriched.customer_id` är den permanenta identiteten. Befintliga ID:n bevaras; endast nya kunder får UUID.
- `customer_id` följer med i `order_rows`, `contacts`, `email_messages`, `email_recipients` och `planned_activities`.
- Den centrala resolveringsordningen är:
  1. exakt `customer_id`;
  2. unikt kundnummer;
  3. unikt normaliserat namn, med adress som extra verifiering;
  4. radnummer endast som verifierad cache tillsammans med namn eller kundnummer.
- Saknad eller tvetydig match ger strukturerat konfliktfel och skapar inte en osäker koppling.
- `planned_activities` använder det fastställda 26-kolumnskontraktet, inklusive fingerprints, mutations-ID och `revision`.
- Kundmastersynken uppdaterar historiska snapshots i planeringen via `customer_id` och stoppar dubbletter av samma ID för manuell granskning.
- Orderimporten binder både befintliga och nytillkomna orderrader till kundmasterns ID efter kundsynken.

### Planering, samtidighet och idempotens

- Create/update/complete använder kanoniska SHA-256-fingerprints i stället för instabila processhashar.
- Samma request-ID med ändrad payload ger `409 idempotency_payload_mismatch`.
- Samma slumpmässiga request-ID kan användas av olika användare utan kollision.
- Update använder `expected_revision`; den äldre tidsstämpeln finns kvar som kompatibilitetsfallback.
- En stale edit ger `409 revision_conflict` och skriver inget.
- All innehållsändring av ett ruttstopp sätter i samma batch:
  - `source=manual`;
  - tomt `route_group_id`;
  - tom `route_sequence`;
  - `time_is_estimated=N`;
  - ny revision och mutationsfingerprint.
- `skip`/`cancel` är statusmutationer och konverterar inte ruttkällan.

### Rutt

- Fasta manuella/follow-up-besök importeras som ankare endast när tiden inte är uppskattad.
- Telefon och mejl med fast tid blockerar körning och service under intervallet.
- Valfria besök placeras mellan ankare utifrån prioritet och marginalkostnad.
- Samma Google Routes-matris och cache används för både optimering och tidslinje.
- Förslaget innehåller körning till första stoppet och retur till samma GPS-position.
- Servern avvisar schema som inte ryms och returnerar fast aktivitet, bokad tid och beräknad ankomst.
- Kandidatfiltreringen sker före Routes-anropet och tar hänsyn till alla säljares:
  - planerade telefon-/mejlaktiviteter samma dag;
  - genomförda kontakter samma dag;
  - aktiva framtida planer;
  - fasta besök som ägs av annan säljare.
- Ruttens samlade aktiviteter, ersatta ruttposter och avbokningar skrivs som en sammanhängande batch i `planned_activities`.

### Kö och kundväljare

- `Att planera` returnerar två separata datamängder: försenade och kommande uppföljningar.
- Kommande omfattar vald vecka och de närmaste 30 dagarna; försenade omfattar alla tidigare ej hanterade uppföljningar.
- Källkontakt-ID används som primär uppföljningsidentitet, med kontrollerad legacy-fallback.
- UI visar 20 poster per sektion med “Visa alla/färre”.
- Kundsökningen matchar namn, stad, adress och kundnummer, utan krav på exakta svenska diakritiska tecken.
- Egna aktiva kunder sorteras först, därefter övriga kunder.

### Sheets-anrop och drift

- En normal aktivitetsuppdatering gick tidigare via flera separata cellanrop; den gör nu ett enda `batch_update`.
- Kontaktens aktivitetscompletion gick tidigare via separata cellskrivningar; den gör nu ett batchanrop.
- En ruttapply som tidigare växte med antalet stopp och avbokningar gör nu ett sammanhängande batchanrop mot planeringsbladet.
- Kors-worksheet-flöden behåller strukturerad “partial success” eftersom Google Sheets inte erbjuder en transaktion över flera worksheets i gspread-abstraktionen.
- `/health` redovisar planeringslåsets status. `WEB_CONCURRENCY>1` utan `PLANNING_DISTRIBUTED_LOCK_URL` ger en tydlig varning. Rekommenderad första release är en worker.

## Testresultat

Körda 2026-07-29:

```text
python -m unittest discover -s web-app/tests -p "test*.py"
Ran 172 tests in 1.410s
OK

python -m unittest discover -s tests -p "test*.py"
Ran 90 tests in 0.086s
OK

python -m compileall -q web-app customer_master_sync stockfiller_orders
OK

git diff --check
OK
```

De 262 testerna täcker bland annat:

- kund-ID, felaktig radcache och tvetydiga kundmatchningar;
- idempotensscope, payload-mismatch och stale revision;
- route-to-manual i ett batchanrop samt statusändring utan konvertering;
- fasta ankare, telefonblock, returkörning, sju-timmarsgräns och otillräcklig tidslucka;
- global kandidatfiltrering och manuellt undantag;
- separata uppföljningar för samma kund;
- partiella fel vid kontaktcompletion/follow-up mirror;
- befintlig CRM-, prioriterings-, rutt-, order- och kundmastersynk.

Felstackar som syns under webbtesterna är avsiktlig felinjicering för partial-success-scenarier; sviten avslutas med `OK`.

## Browser-QA

Verifierat med lokal Flask-harness, deterministisk GPS och deterministisk Routes-provider:

- skapa aktivitet på 320 px;
- söka “butik c”, välja med `ArrowDown` + `Enter`, spara och återläsa med korrekt kund-ID;
- visa försenad och kommande uppföljning separat;
- skapa ruttpreview från GPS, spara två ruttstopp och återläsa dem;
- öppna ruttstopp, visa exakt konverteringsnotis, ändra innehåll och verifiera källa “Manuell”;
- tom dag och planerad dag;
- 320, 360, 390, 430 och 1440 px;
- ingen horisontell sidöverskridning vid någon bredd;
- inga synliga interaktiva mål mindre än 44 × 44 px.

Skärmdumpar:

- `planning-320-final.png`
- `planning-360-final.png`
- `planning-390-final.png`
- `planning-430-final.png`
- `planning-1440-final.png`

## Medvetna implementationval

- Den nya ankarschemaläggaren används för färska kalenderförslag. Det fristående äldre ruttförslaget behåller sin säkra servervalda kandidatmodell men delar Routes-provider och cache.
- Segment A/B/C används endast som sekundär prioriteringssignal när den ordinarie prioriteringspoängen inte skiljer kandidater.
- Cross-sheet-mutationer är inte falskt presenterade som atomiska. De har i stället idempotens och ett strukturerat partiellt resultat som kan repareras säkert.
- Legacy-radnummer finns kvar som cache för bakåtkompatibilitet, men accepteras aldrig ensamt som kundidentitet.

Inga kärnregler i briefen har ändrats.

## Stagingmigrering och rollback

Före produktion:

1. Ta en full kopia av produktions-Sheet till en separat staging-Sheet.
2. Ange staging-Sheet uttryckligen; använd inte produktionsvärdet i `SHEET_KEY`.
3. Kör kundmastersynkens dry-run och stoppa vid:
   - tomt `customer_id` på befintlig kund;
   - duplicerat `customer_id`;
   - tvetydigt kundnummer eller normaliserat namn/adress;
   - planeringsrad som inte kan bindas entydigt.
4. Kör apply endast på staging och verifiera radantal, ID-unikhet och att ingen historik duplicerats.
5. Kör kontaktlogg, uppföljning, ruttpreview/apply och orderimport mot staging.
6. Verifiera riktiga webbläsar-GPS-behörigheter och riktiga Google Routes-anrop.
7. Ta en ny produktionsbackup precis före migreringen.

Rollback är att återställa den senaste kompletta Sheet-kopian och rulla tillbaka applikationsversionen. Eftersom den nya koden bevarar befintliga ID:n och använder append/batch utan destruktiv omskrivning kan staging-diffen granskas före release.

## Kvarvarande release gate

Det finns ingen `STAGING_SHEET_KEY` eller annan uttryckligt avgränsad staging-konfiguration i arbetsytan. Endast den ordinarie `SHEET_KEY` finns. Därför har inga live-Sheets skrivits och ingen produktionsdata har riskerats.

Följande återstår innan en produktionsrekommendation kan ges:

- tillgång till en separat staging-Sheet/kopia;
- dry-run och apply mot den stagingkopian;
- verifiering med riktig webbläsar-GPS;
- verifiering med riktig Google Routes-nyckel/provider;
- bekräftelse att driften startar med en worker, eller tillhandahåller distribuerat lås.

Detta är en extern release-gate, inte ett känt lokalt kod- eller testfel.
