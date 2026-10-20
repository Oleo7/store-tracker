# Store Tracker V3.5 – prestanda och driftsäkerhet

## Omfattning och utgångspunkt

Senaste `master` och aktiv Render-deploy verifierades skrivskyddat till
`99bb4312f8f2473c68c1a4a77278d774e3287ebf` (PR48). Webbservicen har automatisk
deploy avstängd och kör en Gunicorn-worker med fyra trådar. Ingen produktions-
skrivning, merge, deployment eller ändring av Render-konfiguration har utförts.

Den ursprungliga lokala kopian saknade `.git/HEAD`. Arbetet gjordes därför i en
separat klon på `perf/v35-shared-snapshot`, från verifierad `master`.

En privat snapshot hämtades med OAuth-scopen `spreadsheets.readonly`, efter
uttryckligt godkännande. Den innehåller 2 110 kundrader, 3 333 orderrader,
2 010 kontakter, 1 026 planerade aktiviteter, 26 mejl och 38 mottagare.
2 098 kunder ingår i den aktiva prioriteringen. Snapshot, råa replayresultat
och profiler ligger endast i den lokala, Git-ignorerade `.codex_tmp/`.
Rapporten innehåller enbart aggregerade mätningar.

## Rotorsaker

1. Kalendern och Att hantera använder explicita `as_of`-tider med mikrosekunder.
   Därmed får samtidiga liveanrop olika nycklar och bygger var sin global
   snapshot. Den tidigare single-flight-lösningen skyddar bara samma nyckel.
2. Hela underlaget djupkopieras vid lagring och retur. Vid cacheträff sker
   kopieringen dessutom under cachelåset. Den spårade föremätningen registrerade
   cirka 3,58 miljoner rekursiva `deepcopy`-anrop för två fulla kopior.
3. Orderfeatures byggs både i åtagandereduceringen och prioriteringsbygget.
   Kontaktkontrollerna i kalender/Att hantera bygger dessutom om globala
   features, och kan bygga om åtagandetillståndet när `as_of` skiljer sig.
4. Kalendern reducerar hela kundtillståndet även för en vecka utan planerade
   kontakter. Att hantera bearbetar hela prioriteringen före sidindelning.
5. `/customer-insights` skickar både namn- och kund-ID-index. Flera
   förklaringsobjekt och många standardfält upprepas också i varje kundpost.
   Den privata snapshoten återskapade ett svar på cirka 28,6 MB.

`cProfile` tillsammans med `tracemalloc` på oförändrad kod registrerade cirka
28,2 miljoner funktionsanrop. Under denna kraftigt instrumenterade körning
upptog två djupkopior cirka 20 procent av tiden; prioriteringsbygget,
åtagandetillståndet och dubbla orderfeatures var övriga stora poster.
Instrumenterade tider är **inte** API-latens eller ett SLO-resultat.
Spårad allokeringspeak före ändringen var cirka 84,7 MB; detta är extra Python-
allokeringar under profileringen, inte processens RSS.

Render rapporterade `server_failed` den 20 oktober kl. 07:54:07 svensk tid:
HTTP-hälsokontrollen svarade inte inom fem sekunder. Tjänsten var åter
tillgänglig kl. 07:54:20. En femminuters mätpunkt före felet visar
504 135 680 byte, cirka 480,8 MiB RSS; efter omstart cirka 93,6 MiB.
Händelsen bekräftar en hälsokontroll-timeout. Den bevisar inte en OOM-kill.
Samtidiga CPU-tunga byggen, stora svar och kopior är en förklaring som stöds
av kodprofilen, men den exakta resursorsaken till avbrottet är inte isolerad.

## Implementation och säkerhet

- En global, användaroberoende läsmodell delas mellan vyerna. Nyckeln innehåller
  datagenerationer, planeringsunderlag, tillståndshändelser, modellversion och
  en invalidationsepok. Högst två poster behålls; dyra läsbyggen serialiseras.
  Identiska pågående kompakta kundsvar samordnas dessutom separat per fullständig
  användarbehörighet, modell-/källversion och workflow-generation. Bara immutabla
  svarsbyte delas; ingen färdig svarscache eller sessionscookie återanvänds.
- Vanliga liveanrop delar resultat mellan affärsmässiga tidsövergångar.
  Giltigheten begränsas av nästa lokala midnatt och framtida tidpunkter i
  underlaget, inklusive registrerade fakta, förfall och snooze.
  Både föregående och nästa övergång avgränsar återanvändningen, så anrop
  som kommer fram i omvänd tidsordning kan dela resultat. UTC-ordning används
  även när klockan ställs tillbaka vid sommartidens slut.
  Exakta historiska `as_of`-anrop använder fortfarande mikrosekundidentitet.
- Läsmodellens valideringsintervall är högst tolv sekunder och aldrig längre
  än den äldsta inlästa Sheets-datakällans kvarvarande TTL. Vid utgång läses
  källorna på nytt. Endast en identisk innehållsfingerprint får förnya
  intervallet utan omräkning; tidsgränserna kontrolleras fortfarande separat.
  Detta är inte en blind förlängning av datacachens livslängd. Direktredigering syns därför
  normalt inom tolv sekunder plus tiden för nästa läsning/beräkning. Om
  `SHEETS_READ_CACHE_TTL_SECONDS` uttryckligen har ändrats gäller dess käll-TTL
  (befintlig gräns 1–60 sekunder); beräkningscachen förlänger den inte.
- Invalidation under ett bygge hindrar publicering av dess resultat.
  Befintliga färska lässcope för mutationer passerar inte läsmodellscachen
  och behöver inte köa bakom dess pågående läsbyggen.
  Kontaktförbud, ägarskap och åtaganden kontrolleras fortfarande i befintliga
  skrivflöden. Ingen skrivbehörighet hämtas från det kompakta API-formatet.
- Delade dict/list-strukturer skyddas rekursivt mot ändring. Endast de
  prioritetsposter som vyn faktiskt behöver får egna små presentationsobjekt.
  Övriga interna anrop får en vanlig oberoende, muterbar kopia.
- Orderfeatures beräknas en gång och återanvänds i åtagandereduceringen.
  Livevyer återanvänder också features i kontaktspecifika kontroller.
- En kalender utan planerade kontakter kan använda ett lättare tillstånd utan
  att köra affärspoäng för samtliga kunder. Ett senare fullständigt läsanrop
  kan uppgradera samma underlag. Kalenderns bearbetning begränsas till veckans
  kund-ID:n och Att hanteras till den begärda sidan.
- Webbläsaren begär ett valbart, förlustfritt kompakt transportformat. Det
  tar bort namnindexet, identisk dubbel vägledning och upprepade skalära
  standardvärden. Samma fält återställs innan gränssnittet använder dem.
  Äldre API-klienter får det tidigare formatet. Gzip används när klienten
  accepterar det; svaren är fortsatt `no-store`.
- Befintlig progressiv kundlista och förfrågningssamordning behålls.
  JSON-parsning och överföring av svarskroppen får separata browser-mått.
  Backendloggningen omfattar nu även `/planning/attention`, cache/väntan,
  källdata, innehållsverifiering, features, åtaganden, scoring, tidsgränser,
  frysning, kopiering, svarssamordning, JSON och gzip.
  Process-CPU, antal samtidiga instrumenterade anrop och GC-cykler loggas
  utan kund- eller användarvärden. Process-CPU kan överlappa andra trådar.

## Reproducerbar validering

Använd Python 3.12 och projektets låsta runtime-beroenden. Benchmark kräver
dessutom `psutil`, och replay `freezegun`. Installera testverktyg i en separat
virtuell miljö. Privata filer får aldrig checkas in.

```text
python -X utf8 -m pytest web-app/tests -q
python -X utf8 -m unittest discover -s tests -p test_*.py
python scripts/benchmark_read_views.py --repo BASELINE_CHECKOUT --snapshot PRIVATE.json --output PRIVATE-before.json --rounds 3 --no-profile
python scripts/benchmark_read_views.py --snapshot PRIVATE.json --output PRIVATE-after.json --rounds 3 --no-profile
python scripts/benchmark_read_views.py --snapshot PRIVATE.json --output PRIVATE-stability.json --stability-only --no-profile
python -X utf8 scripts/replay_priority_snapshot.py --repo CHECKOUT --snapshot PRIVATE.json --as-of 2026-10-20T10:00:00+02:00 --verify-views --output PRIVATE-replay.json
```

Före och efter ska köras sekventiellt, i separata processer, mot samma fil.
Benchmark fryser affärsdatum men varierar liveanropens mikrosekunder för att
inte dölja cachenyckelfelet. Sheets-I/O ersätts av en skrivförbjudande adapter,
och extern HTTP blockeras. Varm CPU-benchmark håller källcachen varm för att
isolera bearbetningen; den nya modellcachens tolvsekundersgräns kvarstår.

## Resultat och begränsningar

### Varm cache – hela mätmatrisen

Kundprioritet använder `X-PB-Value-Model: v3.5` och det nya kompakta formatet.
Kalender (UI) använder `include_followups=0`, precis som den befintliga klienten.
Baseline ignorerar det nya formatvalet och skickar sitt gamla svar.
Tre omgångar ger 3, 6 respektive 12 prov per rad. P95 är observerad i dessa små stickprov.
Varje roll använder en faktisk användaridentitet över de samtidiga klienterna.
Det är inte ett test med fyra olika administratörsidentiteter.

| Roll | Anrop | Samtidiga | Median före, s | Median efter, s | P95 före, s | P95 efter, s |
|---|---|---:|---:|---:|---:|---:|
| Admin | Kunder | 1 | 0.223 | 0.162 | 0.252 | 0.164 |
| Admin | Kunder | 2 | 0.577 | 0.407 | 0.745 | 0.465 |
| Admin | Kunder | 4 | 1.508 | 0.832 | 1.783 | 1.266 |
| Admin | Kundprioritet (kompakt) | 1 | 1.365 | 1.321 | 1.378 | 1.558 |
| Admin | Kundprioritet (kompakt) | 2 | 2.326 | 1.303 | 2.838 | 1.354 |
| Admin | Kundprioritet (kompakt) | 4 | 3.791 | 1.443 | 5.692 | 1.595 |
| Admin | Kalender (UI) | 1 | 0.722 | 0.168 | 0.777 | 0.198 |
| Admin | Kalender (UI) | 2 | 1.894 | 0.314 | 1.943 | 0.350 |
| Admin | Kalender (UI) | 4 | 3.885 | 0.563 | 4.366 | 0.705 |
| Admin | Att hantera | 1 | 3.450 | 0.080 | 3.505 | 0.080 |
| Admin | Att hantera | 2 | 7.327 | 0.148 | 7.841 | 0.225 |
| Admin | Att hantera | 4 | 20.274 | 0.252 | 24.545 | 0.397 |
| Admin | Uppföljningsinsikter | 1 | 0.412 | 0.802 | 0.455 | 0.906 |
| Admin | Uppföljningsinsikter | 2 | 1.358 | 1.328 | 1.513 | 1.576 |
| Admin | Uppföljningsinsikter | 4 | 4.497 | 2.723 | 5.167 | 5.957 |
| Säljare | Kunder | 1 | 0.088 | 0.076 | 0.096 | 0.077 |
| Säljare | Kunder | 2 | 0.147 | 0.160 | 0.163 | 0.257 |
| Säljare | Kunder | 4 | 0.326 | 0.286 | 0.418 | 0.764 |
| Säljare | Kundprioritet (kompakt) | 1 | 0.715 | 0.154 | 0.740 | 0.199 |
| Säljare | Kundprioritet (kompakt) | 2 | 1.103 | 0.172 | 1.448 | 0.184 |
| Säljare | Kundprioritet (kompakt) | 4 | 2.521 | 0.207 | 4.650 | 0.238 |
| Säljare | Kalender (UI) | 1 | 6.804 | 0.172 | 6.833 | 0.183 |
| Säljare | Kalender (UI) | 2 | 14.343 | 0.367 | 15.021 | 0.441 |
| Säljare | Kalender (UI) | 4 | 36.648 | 0.657 | 38.485 | 0.777 |
| Säljare | Att hantera | 1 | 5.258 | 0.062 | 5.307 | 0.065 |
| Säljare | Att hantera | 2 | 9.843 | 0.133 | 10.554 | 0.226 |
| Säljare | Att hantera | 4 | 20.965 | 0.236 | 26.570 | 0.365 |
| Säljare | Uppföljningsinsikter | 1 | 0.467 | 0.866 | 0.470 | 0.876 |
| Säljare | Uppföljningsinsikter | 2 | 1.572 | 2.707 | 1.584 | 2.853 |
| Säljare | Uppföljningsinsikter | 4 | 3.262 | 6.665 | 4.524 | 6.781 |

**De tre prioriterade UI-anropen uppfyller de lokala varmcachenivåerna
median <2 s och P95 <4 s för båda roller och 1/2/4 samtidiga klienter.**
Detta bevisar inte samma resultat på Renders begränsade instans.

Äldre varianter finns kvar: fyra samtidiga adminanrop i det stora äldre
kundformatet gav median 4,076 s/P95 6,136 s. Kalenderns äldre standardsvar
med inkluderade uppföljningar gav 3,581 s/P95 4,551 s för admin och
2,086 s/P95 4,360 s för säljare. Alla gamla API-varianter klarar alltså inte målen.
Samtliga varianter, CPU och RSS finns i den aggregerade JSON-filen.

Kontrollendpointen `/followup-insights` varierade tydligt mellan körningar.
En extra direkt efterföljande kontroll med samma oförändrade funktionskod
gav säljarens fyrklientmedian 5,990 s före och 5,380 s efter. Den ursprungliga
hela matrisen visar 3,262 respektive 6,665 s. Detta är skäl att inte dra en
orsaksslutsats om regression från en ensam sekventiell körning eller hävda
att kontrollendpointen har optimerats. Den är fortfarande CPU-tung.

### Kall cache och svarsstorlek

| Roll | Anrop | Kall före, s | Kall efter, s | Svar före, byte | Svar efter, byte |
|---|---|---:|---:|---:|---:|
| Admin | Kunder | 0.323 | 0.174 | 1324318 | 1324318 |
| Admin | Kundprioritet (kompakt) | 3.990 | 5.375 | 28571077 | 8155524 |
| Admin | Kalender (UI) | 0.835 | 2.350 | 6427 | 6427 |
| Admin | Att hantera | 3.699 | 4.366 | 2056 | 2056 |
| Admin | Uppföljningsinsikter | 0.536 | 0.940 | 4825 | 4825 |
| Säljare | Kunder | 0.159 | 0.088 | 193442 | 193442 |
| Säljare | Kundprioritet (kompakt) | 3.142 | 2.664 | 4363909 | 1538776 |
| Säljare | Kalender (UI) | 6.979 | 4.389 | 46811 | 46811 |
| Säljare | Att hantera | 7.699 | 4.406 | 31919 | 31919 |
| Säljare | Uppföljningsinsikter | 0.598 | 0.905 | 4825 | 4825 |

Den tidsfrysta transportkontrollen återställde alla 2110 kundposter exakt.
Dess gamla JSON var 28 512 333 byte, kompakt JSON 8 140 838 byte
och gzip 1 122 404 byte. Det motsvarar cirka 71.4 procent mindre
okomprimerad datamängd. Gzip är ett separat mått; äldre produktionsproxyns
eventuella komprimering har inte uppmätts och ingår inte i denna jämförelse.

### CPU, minne och stabilitet

| Fyrklientfall, tre omgångar | Process-CPU före, s | Process-CPU efter, s |
|---|---:|---:|
| Admin – Kundprioritet (kompakt) | 15.219 | 4.047 |
| Admin – Kalender (UI) | 11.500 | 1.969 |
| Admin – Att hantera | 60.344 | 0.969 |
| Säljare – Kundprioritet (kompakt) | 11.172 | 0.641 |
| Säljare – Kalender (UI) | 101.672 | 2.250 |
| Säljare – Att hantera | 63.703 | 0.844 |

Separat allokeringsprofil med `tracemalloc`, utan samtidig cProfile, gav
extra peak 80.7 → 71.4 MiB
och kvarhållet minne 52.5 → 33.1 MiB.
Före används den gamla obligatoriska kopieringen; efter används den delade
läsmodellen som de optimerade GET-vyerna faktiskt begär. Instrumenterade
körtider är inte API-latens och dessa Python-allokeringar är inte total RSS.

Slutligt stabilitetstest: 12 vågor, 36 tunga anrop och parallell `/health`.
Tre tunga läsningar och ett hälsoanrop körs per våg. Verklig kötid när alla
fyra Gunicorntrådar är upptagna återstår att verifiera.
Högsta samplade RSS var 182.2 MiB (20 ms sampling).
`/health` hade max/observerad P95 0.067 s.
Antal poängbyggen var 1; högst 1 samtidigt.
RSS efter GC låg under de sista sex vågorna mellan 147.1 och 147.7 MiB.
Det korta testet visar ingen fortsatt tydlig linjär minnesökning under dessa
vågor. Det ersätter inte ett långt soaktest. Totalt process-CPU finns i JSON.

### Webbläsare – dator och mobilstorlek

Riktig IAB/Chromium, adminportfölj med 2 110 poster, lokal HTTP och
skrivförbjudande snapshotadapter. Viewports: 1366×900 och 390×844.
Varje rad är en navigation, inte en median. Appens befintliga DOM-milstolpar
används; första målade bildrutan/FCP och fysisk mobilprestanda mäts inte.

| Version | Storlek | Cache | Synlig kundlista, ms | Färdig prioritering, ms |
|---|---|---|---:|---:|
| before | desktop | cold | 812 | 6840 |
| before | desktop | warm | 763 | 2785 |
| before | mobile | cold | 700 | 7134 |
| before | mobile | warm | 643 | 2852 |
| after | desktop | cold | 1190 | 6226 |
| after | desktop | warm | 819 | 2033 |
| after | mobile | cold | 970 | 6629 |
| after | mobile | warm | 576 | 1584 |

Efter ändringen tog JSON-parsning inklusive återställning av standardfält
55 ms på varm datorvy och 67 ms i varm mobilstorlek. Svarskroppens läsning
tog 47 respektive 39 ms. Ingen mobil CPU- eller nätverksstrypning användes.
Vyerna renderade prioriteringskort utan konsolfel. Kall prioritering tar
fortfarande cirka 6–7 s i denna lokala browserkontroll; det är en kvarvarande
kostnad och inte ett uppnått mål om omedelbar kall prioritering.

### Regressionsbevis och beslut

- 1 123 web-app-tester, 759 deltester och 106 rottester passerar.
- Slutlig GitHub CI för kodcommitten passerar även Linux och befintliga dator-,
  mobil-, guidance-, workflow- och kalenderdragtester.
- Exakt replayjämförelse på alla 2 098 aktiva kunder, samtliga sex användarroller,
  sex kalenderägare och 66 Att hantera-poster: identiska resultat.
- Hela äldre kund-API-svaret och åtagandetillståndet är identiska. Den kompakta
  representationen och gzip avkodades tillbaka till exakt samma 2 110 ID-poster.
- Prioritets-, värde-, kontaktkanals-, kontaktpermissions- och ruttpoängformlernas
  modulfiler är byteidentiska. Regressionerna omfattar ägarbyte, kontaktförbud,
  framtida förbud, förfall, snooze, midnatt, DST, samtidighet, invalidation under
  bygge, mutationer i delat underlag och direktändring efter käll-TTL.

**Merge: GO för granskning och merge med `[skip render]` i mergecommitten.**
Ingen merge utförs här. Webbens autodeploy är avstängd, men schemalagda
Render-tjänster följer `master` med autodeploy. Utan skip-markeringen kan en
merge därför oavsiktligt deploya dem. PR-titeln innehåller markeringen.

**Produktion: NO-GO tills kapacitetsbegränsad staging är verifierad och
användaren uttryckligen godkänt driftsättning.** Först därefter kan man
avgöra om Starter räcker. Nuvarande mätunderlag motiverar inte ett köp av
större plan. Kvarvarande kall CPU-kostnad bör i första hand angripas med
ytterligare profilerad återanvändning av oförändrade delberäkningar och
mindre presentationsarbete, med samma affärsreplay som spärr.

Fullständiga anonyma mätvärden: [resultat-JSON](performance-v35-2026-10-20-results.json).
Inga råa kundposter, användaridentiteter eller autentiseringsuppgifter ingår.

Lokala mätningar avser Flask-testklienter på Windows, utan nätverksöverföring
eller Gunicorns kö. Datorn saknar Linux/WSL och kan inte återskapa Renders
0,5 vCPU/512 MiB. Lokala latens- och minnesresultat ska inte framställas som
bevis på att produktionsmålen är uppnådda.

## Produktionsplan och GO/NO-GO

Merge kräver godkända regressioner, identisk affärsreplay och granskning av
den fokuserade PR:en. Produktion kräver dessutom separat uttryckligt godkännande
och ett kapacitetsbegränsat stagingtest. Ingen större server rekommenderas på
basis av obekräftad extrapolering från utvecklingsdatorn.

Efter godkännande:

1. Kör den privata snapshoten i Linux med 0,5 vCPU/512 MiB och befintlig
   `gunicorn --workers 1 --threads 4`. Verifiera kall/varm cache och 1/2/4
   klienter samt `/health` genom den verkliga HTTP-kön. Inkludera fyra
   olika identiteter och blandade roller, inte bara parallella klienter för
   samma identitet.
2. Kräv median <2 s och P95 <4 s för de tre berörda vyerna med varm cache.
   Följ RSS under minst 30 minuters upprepad användning och kontrollera
   direktredigering, kontaktförbud och ägarbyte med separat staging-CRM.
3. Upprepa den lokala browsermätningen av synlig kundlista och färdig
   prioritering i dator- och mobilwebbläsare med produktionslik kapacitet,
   inklusive nätverksprofil, svarsstorlek, JSON-parsning och rendering.
4. Deploya endast den godkända committen manuellt till webbservicen. Behåll
   oförändrade schemalagda synktjänster och verifiera exakt aktiv commit.
5. Kontrollera läsvyer och färska skrivbeslut samt följ per-endpoint-loggar,
   minne, fel och hälsokontroller. Avbryt vid regression eller minnesökning.
6. Vid fel: återgå manuellt till tidigare verifierad webb-deploy/commit
   `99bb431`. Ingen datamigration eller CRM-skrivning ingår i denna ändring,
   så återgången behöver inte reversera någon ny datamodell.

Renderreferenser kontrollerade 2026-10-20:
[deployflöde](https://render.com/docs/deploys) och
[hälsokontroller](https://render.com/docs/health-checks).
