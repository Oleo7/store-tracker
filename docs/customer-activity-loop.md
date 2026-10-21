# Gemensam kontakt- och planeringsloop

Utgångspunkt: master `3c6df7f6cc7d2d0732734a98fea521f847a9d723` (PR #50). Ändringen är avsedd för granskning och kräver ingen produktionsmigration, backfill eller historikradering. Ingen produktionsdata har lästs eller skrivits under utvecklingen.

## Operativt kontrakt

`customer_obligations.build_customer_state()` är den gemensamma, rena aktivitetsprojektionen. Det gamla modulnamnet och vissa läsfältnamn finns kvar för kompatibilitet; individuella kundlöften har ingen egen operativ livscykel. Bekräftad bokning, anteckning, kanal och hjälp med plock ligger på den faktiska aktiviteten.

Missade aktiviteter har ett passerat **datum i Europe/Stockholm**. Dagens aktiviteter hör till agendan. Kontaktens verkliga tid styr vilka tidigare missar den ersätter; `recorded_at` styr när kontakten blev känd i en snapshot. Alla jämförelser av händelseordning använder verkliga tidsinstanter, även under den upprepade timmen vid vintertidsomställningen.

En senare kontakt (Positiv, Neutral, Negativ, Ej anträffbar eller Order lagd!) ersätter tidigare missar för samma säkert identifierade kund. Endast den faktiskt kopplade kalenderaktiviteten kan bli genomförd. Övriga får lässtatus `superseded`; källraderna skrivs inte om. Ej anträffbar och Order lagd! blir inte nådd dialog. Importerade order och tekniska mejlhändelser hanterar inte missar. Ett verifierat skickat kalendermejl kan slutföra just sin bokade aktivitet enligt det befintliga utskickskontraktet och räknas inte som nådd dialog.

En sparad ny aktivitet eller verklig omplanering ersätter tidigare missar vid **beslutstillfället**, inte vid den framtida bokningens datum. Andra framtida aktiviteter bevaras. En anteckningsändring är inget planeringsbeslut. Avbokning av en ersättande aktivitet gör inte gamla missar aktuella igen.

Hoppa över på den senaste missen täcker den missen och äldre missar. En äldre överhoppning täcker inte en nyare miss; en framtida överhoppning är ingen kontakt. Återställning kan öppna den felaktigt överhoppade kalenderaktiviteten igen utan att återuppliva den äldre gruppen. Historiken visar överhoppad respektive ersatt aktivitet, utan extra genomföranden eller fabricerade kontaktloggar.

`attention_items()` väljer en representativ senaste miss per kund-ID och aktuell kanonisk kundägare före sökning, sortering, total och paginering. Kundlistan, vägledningen, nästa uppföljning, kalendern, mejlbesluten och båda ruttmotorerna använder samma projektion. Aktuell ägare får åtkomst till tidigare säljares kundarbete; historiska källrader behåller sin attribution.

Den kommersiella DFP-modellen, vikterna, normaliseringen, kontaktkurvorna och order-/leveransreglerna är kvar. Automatisk väntan kan begränsa rekommenderad utgående kontakt och utskick, men spärrar inte registrering av verklig kontakt, omplanering eller överhoppning.

## Skrivvägar och sparsäkerhet

Borttagna POST-vägar svarar **410 operation_retired**, utan tillståndsmutation:

- `/customer-workflow/action`
- `/customer-contact-permission`
- `/planning/suggestions/<id>/snooze`, `/dismiss` och `/resume`

Kontaktformulärets gamla åtagandeutfall, behörighetsbesked och åtaganderevisioner avvisas med 410. Gamla separata överenskommelsefält i kontaktens uppföljning och vanliga aktivitets-API:er avvisas också; historiska sådana flaggor påverkar inte kontaktpolicyn. Endast faktisk `appointment_confirmed` kan ge bokningsspecifik tidspolicy. Historiska kontaktresultat ändras inte till andra resultat. Direkta PATCH-anrop till `completed` kräver fortsatt kontaktloggning; `route_pending` kan inte aktiveras med vanlig statusändring.

Verkliga aktiviteter flyttas och hoppas över genom vanliga aktivitets-API:er med deras revisioner. `/customer-obligations/action` är en begränsad övergångsadapter för säkert identifierade **uppföljningsdatum utan kalenderpost**: endast `reschedule` och `skip`. Den skapar vid omplanering en riktig aktivitet, utan godtycklig tid eller kundbekräftelse på det gamla datumet. Request-ID, fingeravtryck och revisionskontroll skyddar mot dubletter och förlorade ändringar. Ett partiellt adaptersparande kan återhämtas även om senare kontakt sparats.

Kundmaster och aktuell ägare kontrolleras på nytt under skrivlåset för kontakt och ny planering. Kalenderändringar hämtar också aktuell kundmaster. Ett cachat kort eller en förhandsvisning kan inte ge rätt att skriva till en borttagen kund. Befintliga gemensamma lås, driftspärrar, ruttens källkontroller, obligatoriska återläsning och karantän av ofärdiga ruttposter är kvar.

Vid kontakt plus uppföljning sparas kontaktens datumvisning först utan en preliminär bokning. När aktiviteten sparats synkas datumvisningen. Om uppföljningen misslyckas svarar API:t 207 med `contact_saved=true` och `follow_up.saved=false`. UI låser det redan sparade underlaget och återförsöker samma request-ID. Timeout efter faktisk kontaktskrivning återanvänder den sparade kontakten. Ingen gammal miss försvinner från enbart klientavsikt.

## Äldre data och cache

Två tekniska kolumner tillkommer på befintliga aktivitetsrader vid normala skrivningar:

- `planning_decided_at`: skapande eller verklig ändring av genomförandetid. Befintlig `created_at` är konservativ fallback för äldre data; `updated_at` används inte som bevis på omplanering.
- `skipped_through_at`: den valda missens tidsgräns, som bevarar hanteringen av äldre missar även vid återställning av just kalenderposten.

Virtuella överhoppningar använder `activity_skip` i den befintliga händelsetabellen. Ingen ny tillståndstabell eller administrativ kö införs. Läsning skapar inte schema, tabeller eller migrationsskrivningar. Gamla verkliga avslut, avbokningar och ombokningar bevaras. Historiska snooze-/dismiss-/permissionbeslut ignoreras operativt. Legacy-ID:n kan överbryggas med unik kund + källtidsstämpel; tvetydiga eller felkopplade historiska poster isoleras och blockerar inte portföljen.

Aktivitetskontraktet är `customer-activity-v1` i projektion och cacheidentitet. Ruttens rankingkontrakt är `v35-priority-lexicographic-activity-1`; det ändrar inte ruttmålets kommersiella vikter. Signeringssalt för nya ruttförhandsvisningar har bytts. Gamla signerade förhandsvisningar och cachade ruttkontrakt avvisas. Planeringsfingeravtryck omfattar de nya tidsstämplarna. Skrivinvalidering och UI:s serialkontroller för sena svar behålls.

Återhämtning ska ske genom normala idempotenta återförsök och ny hämtning/beräkning av aktuellt underlag. Återställ inte en gammal datakopia över senare kundarbete. Ingen operativ återställning kräver att gamla dolda prioriteringsbeslut återupptas.

## Synkflöden och kundborttagning

`customers_enriched` är fortsatt aktiv kundmaster. Store Tracker återkopplar inte borttagna ID:n genom namn, orderhistorik eller gamla aktiviteter. `customer_master_sync.run_customer_sync()` kan däremot klassificera en orderkund som `new_customer` när motsvarande masterpost har raderats. Vid tillräckligt kundnummer eller fullständig adress kan den läggas till på nytt. `stockfiller_orders.sync` anropar denna rutin efter orderskrivning när kundsynk är aktiverad; fristående kundmastersynk kan göra samma sak.

Produktägaren behöver därför styra körläge/underlag för dessa synkjobb när kunder avsiktligt raderas, och vid behov använda befintlig inaktivering i mastern. Ingen ny spärrlista, generell importstrategi eller automatisk återkoppling av det borttagna ID:t införs här. Storepoint bygger sin externa export från order_rows och är inget aktivt Store Tracker-behörighetsregister; historiska order kan fortfarande medföra en exportrad där. Inget av dessa synkflöden fabricerar kontakt eller kalendergenomförande.

## Verifiering och testövergång

Den nya kontraktssviten `test_customer_activity_loop.py` och de uppdaterade fokuserade sviterna ersätter upprepade tester av individuell åtagandehantering. `customer-activity-test-replacements.json` listar de utfasade policyfallen. De nya testerna verifierar ändrade beteenden; övriga numeriska regressioner och skydd för identitet, åtkomst, revisioner, samtidighet, dubletter, delvisa sparfel och ruttintegritet är kvar.

| Acceptansfall i uppdraget | Automatiserad täckning |
| --- | --- |
| 1–3, 20–21, 40, 42–43 | ActivityChronologyTests, ActivityLoopApiTests, ActivityLoopSafetyTests; attention grouping och ownership |
| 4–10, 27–28, 44–46 | Kontaktresultat, verklig tid, sen registrering, upprepad DST-timme, tekniska mejl/importer, en logg per kontakt |
| 11–19, 29–33 | Beslutstid, ny miss, flera framtida bokningar, senaste/äldre/future skip, restore, note edit, avbokning, datum utan bokningstid |
| 22–26 | Inerta gamla händelser och planeringsförslag; 410 utan mutation; direkt completed avvisas |
| 13, 34–36 | Partiell uppföljning, kontakttimeout efter append, adapteråterhämtning, befintliga samtidighets- och revisionstester |
| 37–39, 41 | PR #50:s ruttkoordinering, route_pending, återläsning, stale preview, gamla kontrakt, borttagen kund med aktiv cache |
| 47–48 | JS-kontrakt och faktiska browserflöden med sena svar/fel; dataläsfel ger 503, inte tom arbetslista |
| Gemensamt konsumentkontrakt | Kalender/attention/next follow-up samt real snapshot genom mejlbeslut och båda ruttmotorerna utan ersatt miss som blockerare |

Lokal verifiering: 1 139 backendtester och 123 rottester; Python-kompilering och `git diff --check`. Exakt verifierad PR-head och CI-status anges i PR-beskrivningen.

Browserkontroller använder faktiska lokala Flask-harnessar med syntetiska Sheets och ersatt extern transport. Täckningen omfattar ny loop i mobil/desktop, säljuppföljning i mobil/desktop, vägledning vid 320/390/1440 px, ruttens misslyckade aktivering och återförsök samt drag-and-drop/touch med sena svar, konflikter och återställning. Inga verkliga mejl skickas.

### Syntetisk prestandajämförelse

20 isolerade körningar per operation, samma maskin och in-memory Sheets. Läsvyer har 40 historiska kalenderposter; kontakt och ruttsparande använder separata rena syntetiska fixtures. Detta mäter applikationsarbete, inte produktionslatens eller Google Sheets-nätverk. Läsfel injiceras i testsviten; ingen produktionsreplay har gjorts eftersom ingen godkänd CRM-snapshot var tillgänglig i uppdraget.

| Operation | Baseline median ms | Ny median ms | Baseline p95 ms | Ny p95 ms |
| --- | ---: | ---: | ---: | ---: |
| Kundinsikter | 13.386 | 8.864 | 15.839 | 10.740 |
| Att hantera | 13.073 | 9.014 | 13.591 | 10.971 |
| Kalender | 76.472 | 21.291 | 82.814 | 26.893 |
| Logga kontakt | 7.223 | 2.467 | 8.660 | 3.748 |
| Verifierat ruttsparande | 11.434 | 4.197 | 13.097 | 5.663 |

Kundmasterkontrollen kan lägga till färskhetsläsningar per mutation i en varm produktionscache; ingen läsning tillkommer per kund eller historisk aktivitetsrad. Följ därför ordinarie befintlig prestandamätning vid en separat godkänd driftsättning. `web-app/tests/activity_loop_benchmark.py <repo-root>` reproducerar mätningen mot en checkout eller en exporterad baseline.

Begränsningar: inga produktionsmigrationer, utskick, CRM-skrivningar, verklig Sheets-latens eller produktionsbaserad före/efter-lista har verifierats. Äldre ombokningar utan tillförlitlig beslutstid tolkas konservativt från skapandetid, utan att gissa från ändrade anteckningar. Produktionens driftflaggor och låsinställningar ändras inte av denna PR.
