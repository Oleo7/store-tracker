# Sales-planering och kalender – slutrapport

Datum: 2026-07-27

## Resultat

Polarbärs CRM har fått en mobilanpassad säljaragenda där planerade aktiviteter
är separerade från genomförda kontakter, men sammankopplade med stabila ID:n.
De prioriterade flödena fungerar i den isolerade webbläsar- och
Google Sheets-harnessen:

- skapa, redigera, flytta, hoppa över och avboka aktiviteter
- planera direkt från en kund och från en äldre uppföljning utan klockslag
- logga en planerad kontakt och samtidigt skapa en tidsatt uppföljning
- återuppta en delvis sparad kontakt utan en extra kontaktloggrad
- förhandsgranska och tillämpa en dagsrutt med obligatoriska aktiviteter
- visa historiska oplanerade kontakter på rätt dag
- låta säljare se sin egen planering och administratörer välja aktiv säljare
- dölja planerings- och skrivfunktioner för användare utan säljbehörighet

## Väsentliga ändringar

### Databas och API

- Nytt blad `planned_activities` med stabila aktivitets-, kund-, ägar-,
  käll- och request-ID:n, tidszonssatt tid, status samt optimistic-concurrency-
  fält.
- `sales_activities` har kompletterats med `contact_id` och
  `planned_activity_id`.
- API för veckohämtning, skapande, uppdatering, statusändring,
  ruttförhandsgranskning, ruttsparning och import från befintligt
  ruttförslag.
- Idempotenta skrivningar reparerar ofullständiga speglingar och skiljer
  säkert mellan retry och verkligt konfliktande innehåll.
- Google Sheets-skrivningar skyddas med lås i processen och batchskrivningar
  där flera celler/rader hör ihop.
- Produktionsstart stoppar om `FLASK_SECRET_KEY` saknas.

### Kontaktlogg och prioritering

- `Logga kontakt` från en planerad aktivitet markerar aktiviteten genomförd
  och länkar exakt en kontaktloggrad.
- Kommentar är obligatorisk. Frysfält krävs endast för fysiska besök.
- En uppföljning kan skapas i samma sparning med typ, datum, tid och
  anteckning.
- Uppföljningens ägare valideras som aktiv säljare. Administratören måste
  välja säljare för fristående uppföljningar.
- Äldre `follow_up_date` visas i “Uppföljningar utan bokad tid” tills den
  tidsätts.
- Historiska oplanerade kontakter visas skrivskyddat på sin faktiska dag.

### Rutt

- “Fyll dagen automatiskt” använder GPS-start och GPS-retur för idag och
  framtida dagar.
- Manuella och uppföljningsskapade besök är obligatoriska stopp.
- Fasta telefon- och mejlaktiviteter ligger kvar på sin tid och dras från
  sjutimmarsbudgeten.
- Den fullständiga tidslinjen tar hänsyn till både körning och fasta
  aktiviteter, konfliktmarginalen ±15 minuter, maximalt 15 besök och strikt
  total tid under sju timmar inklusive retur.
- Ny körning ersätter endast säljarens oavslutade ruttaktiviteter för dagen.
- Befintligt ruttförslag kan importeras när kartan fortfarande motsvarar det
  servervaliderade förslaget.

### Frontend och användbarhet

- Sjudagarsremsa, datumväljare, dagagenda, tomläge, äldre uppföljningar,
  status- och källemblem samt aktivitetsdialog i CRM:ets befintliga visuella
  språk.
- Mobil dialog visas i helskärm. Fokus fångas i dialogen, `Escape` stänger
  och fokus återgår till startkontrollen.
- Alla planeringskontroller har minst 44 × 44 px tryckyta. Vid 320–375 px
  blir veckoremsan lokalt horisontellt rullningsbar utan sidöverflöde.
- Kontaktformuläret har explicita etiketter och tydliga alternativ för
  Besök, Telefon och Mejl.
- Vid HTTP 207 eller osäkert nätverksfel behålls exakt request-ID och
  payload; formuläret visar “Försök slutföra sparningen”.

## Tekniska tester

| Kontroll | Resultat |
|---|---:|
| `web-app/tests` | 162 godkända |
| rotens integrations-/synktester | 90 godkända |
| JavaScript-syntax i `index.html` | Godkänd |
| Python-kompilering av ändrade Python-filer | Godkänd |
| `git diff --check` | Godkänd |
| Webbläsarkonsol, varning/fel | 0 |

Testerna täcker bland annat rollgränser, ägarskap, DST-gap, statusövergångar,
optimistisk låsning, idempotens, partial save, dubblettskydd,
kundkoppling, historiska kontakter, ruttbudget, obligatoriska stopp,
konflikter under körsträckor och ersättning av ruttaktiviteter.

## Webbläsarverifiering

Kontrollerat i Codex inbyggda Chromium mot en isolerad minnesdatabas,
deterministisk GPS och konstant road-time-provider.

| Steg | Bevis |
|---|---|
| 1. Desktopagenda och veckoval | [01-desktop-agenda.png](01-desktop-agenda.png) |
| 2. Skapa/redigera aktivitet | [02-desktop-activity-editor.png](02-desktop-activity-editor.png) |
| 3. Ruttförhandsgranskning | [03-desktop-route-preview.png](03-desktop-route-preview.png) |
| 4. Partial save och säker retry | [04-desktop-partial-save-retry.png](04-desktop-partial-save-retry.png) |
| 5. Mobilagenda, 390 px | [05-mobile-agenda-390.png](05-mobile-agenda-390.png) |
| 6. Mobilt tomläge | [06-mobile-empty-state-390.png](06-mobile-empty-state-390.png) |
| 7. Mobil aktivitetsdialog | [07-mobile-editor-390.png](07-mobile-editor-390.png) |
| 8. Kompakt mobil, 320 px | [08-mobile-compact-320.png](08-mobile-compact-320.png) |
| 9. Administratör väljer aktiv säljare | [09-desktop-admin-owner.png](09-desktop-admin-owner.png) |
| 10. Läsroll utan planeringsknappar | [10-desktop-viewer-permissions.png](10-desktop-viewer-permissions.png) |

Responsivitet kontrollerades vid 1280 px samt 430, 390, 375, 360 och 320 px.
Ingen viewport fick horisontellt sidöverflöde eller en synlig
planeringskontroll mindre än 44 × 44 px. Tangentbordsflödet för dialogen,
fokusretur och `Escape` verifierades separat.

## UX-hälsa

Bedömning: **god och redo för pilot**, med kvarvarande driftsrisker nedan.

Styrkor:

1. Vecka, dagsläge, status och nästa handling är synliga utan att lämna
   säljarens arbetsflöde.
2. Planering och utförd kontakt har tydliga källor och länkar, vilket minskar
   risken för dubbla eller “försvunna” aktiviteter.
3. Mobilflödet behåller full funktion vid 320 px och har tydlig
   återhämtning när bara en del av sparningen lyckas.

## Avvikelser och skäl

1. Kartjusterade ruttstopp importeras inte tyst. Om kartan skiljer sig från
   serverns validerade förslag blockeras importen och användaren får skapa ett
   nytt validerat förslag. Det bevarar sjutimmars- och konfliktreglerna.
2. En framtida dagsrutt startar 09:00; dagens rutt startar vid närmaste
   kommande halvtimme. Briefen angav inte en exakt starttid för framtida dag.
3. Två obligatoriska besök hos samma kund samma dag avvisas med ett
   handlingsbart 422-svar, eftersom nuvarande kundmodell annars inte kan
   representera två separata ruttstopp säkert.
4. En separat `planning_enabled`-flagga infördes inte. Projektet saknar
   befintlig flagginfrastruktur och planeringen är redan behörighetsstyrd.
   Detta ändrar ingen kärnregel, men produktionsaktivering bör därför ske
   genom vanlig release/pilot i stället för runtime-flagga.

## Kvarvarande risker och evidensgränser

1. QA skrev inte till produktionsarket och anropade inte riktiga
   väg-/GPS-tjänster. Bevisen kommer från samma API och UI mot testdubblar;
   en pilot bör därför kontrollera anslutning, kvoter och behörigheter mot
   det riktiga Google Sheet-kontot.
2. Skrivlåset är processlokalt. Idempotens skyddar retries, men en installation
   med flera WSGI-processer bör kompletteras med distribuerat lås eller en
   transaktionell databas för absolut samtidighetsgaranti.
3. Detta är en lokal verifiering, inte en produktionsdriftsättning och inte
   ett påstående om full WCAG-överensstämmelse. Semantik, fokus,
   tangentbord, tryckytor och kontrastnära visuella tillstånd har kontrollerats.

Valfri mänsklig slutkontroll: öppna pilotmiljön på en fysisk säljartelefon,
godkänn/avvisa den riktiga GPS-dialogen och bedöm läsbarheten i direkt solljus.
Detta påverkar inte de automatiserade acceptansresultaten ovan.
