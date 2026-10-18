# V3.3: en prioritet i säljarens arbetslista

## Produktbeslut

Butiker visar en framträdande poäng, **Prioritet**, samt kundens aktuella status,
åtgärd och planeringsdatum. Den poängen blandar affärspotential och aktuell
relevans. Underliggande Affärsvärde finns kvar i förklaringen och som alternativ
sortering för portföljöversikt.

**Alla** är en arbetsöversikt med både röda, ännu inte planerade kunder och gröna,
planerade kunder. **Att agera** väljer de kunder där ett nytt beslut eller en ny
obokad kontakt är tillåten nu. **Planerade** visar det planerade arbetet, även när
kunden inte längre har hög poäng för ett nytt säljsamtal. **Avvakta** och
**Granska** förklarar varför en kund behöver vänta eller få sitt underlag bedömt.

En bokning ändrar arbetsstatus. Den ska inte i sig radera uppgiftens betydelse.
Prioritet för planerat arbete räknas därför utan just den duplicerande
bokningsspärren. Verkliga spärrar som kontaktförbud och motstridiga orderhändelser
finns kvar. Planeringen ger inget artificiellt poängpåslag. Om en ny order gör
den gamla säljuppgiften inaktuell kan prioriteten sjunka även för en grön kund.

Färg är inte poäng. Grönt betyder att en faktisk aktivitet är planerad och att
datumet ska följas. Rött kräver en konkret åtgärd. En kontaktpaus eller avsaknad
av aktuellt kontaktbehov får inte beskrivas som en uppmaning att ringa.

## Beslut och poäng

Modellen behåller två beräkningsdimensioner och separerar dem från workflow:

- `business_value_raw`: volympotential som skattas från leveransstorlek,
  gruppprior, frekvens och provisorisk realisering över 90 dagar.
- `contact_priority_raw`: effektiv prioritet för en ny obokad kontakt. Noll när
  kontakt ska vänta eller samma uppgift redan är bokad.
- `work_priority_raw`: synlig arbetsprioritet, inklusive relevant planerat
  arbete. Under Alla sorteras den utan att alla gröna kunder placeras i en lägre
  bucket enbart för att de har bokats.
- `can_start_unscheduled_contact`: tillåtelse att börja en ny obokad kontakt.
- `can_execute_planned_contact`: tillåtelse att utföra en befintlig planerad
  aktivitet. Samma hårda kontaktförbud gäller båda tillstånden.
- `review_only` / `requires_handling`: ett gammalt åtagande kan behöva intern
  hantering utan att kunden ska kontaktas.

`can_contact_now` är kompatibilitetsfältet för obokad kontakt. Kalenderns
Genomför/Logga gäller den uttryckliga bokningen; en bokad kund kan korrekt ha
noll effektiv kontaktprioritet samtidigt som den planerade aktiviteten får
utföras.

Mejlförslag, faktisk mejlsändning och båda ruttmotorerna använder samma
kontaktbehörighet. En obligatorisk ruttpunkt får inte kringgå negativ paus,
orderpaus, återförsök eller kontaktförbud. För flera bokningar bedöms varje
aktivitet med sin egen källa och revision; kortets primära bokning ger inte
automatiskt tillåtelse till en annan aktivitet. Den alternativa beräkningen
återanvänder källfeatures utan att ändra kundens huvudbeslut.

Om ett bokat mejl har skickats men kontaktloggen, kalenderstatusen eller
åtagandehändelsen inte kunde sparas kan exakt samma låsta begäran reparera
återstående skrivningar utan omsändning. Originalets planeringskontext sparas
först vid mutation i den frivilliga kolumnen
`email_messages.planning_context_json`; vanlig läsning kräver inte kolumnen.

Varje separat uppföljning i Att hantera prövas också med sitt eget åtagande och
sin egen tidsordning. Ett nyare löftes undantag från negativ dialog får inte
ärvas av ett äldre löfte. Kundkortets huvudbeslut ändras inte av denna prövning.
Registrering av en faktiskt inträffad kontakt eller inkommande historik är
fortsatt möjlig; den är inte i sig ett tillstånd att initiera utgående kontakt.

Rangordning använder oavrundade råvärden. Ett index 0–100 är inte köpchans,
framtida täckningsbidrag eller kausalt värde av nästa säljsamtal. Den tidigare
v2.2-poängen finns kvar som jämförelse-/kompatibilitetsunderlag; gamla konsumenter
får inte kringgå den gemensamma kontaktbehörigheten.

## Kontaktregler som måste hålla ihop

### Order och återförsök

Ett generiskt misslyckat orderförsök blir inte ett nytt retry-uppdrag efter att en
senare order har lagts. Ett separat servicelöfte överlever, eftersom det har ett
annat syfte och egen källa.

`Order lagd!` är en rapporterad ordersignal, inte nådd dialog eller leverans.
Innan orderimporten hunnit ikapp gäller två arbetsdagars synkpaus för generisk
orderjakt. Därefter behövs intern orderstatusgranskning utan automatisk
kundkontakt om matchande order fortfarande saknas. Den rapporterade ordern får
inte fabricera köphistorik eller volym.

För registrerade order utan säkert leveransdatum gäller avvakta dag 0–14,
statuskontroll dag 15–60 och historisk granskning därefter. Historikgranskning ska
ha en lösningsväg; den ska inte bli permanent, oförklarad osynlighet.

### Första leveransen

Den första leveransen har egen klocka. En dialog före eller i samband med ordern
bevisar inte att placering och försäljningsstart har följts upp efter leveransen.

- Dag 0–6: ingen generell onboardinguppmaning.
- Dag 7–10: onboarding för A/B, en känd SKU eller legacy `value_index >= 70`.
- Dag 11–90: säkra andra köpet.
- Efter dag 90 utan nytt köp: enstaka-köp-återaktivering.

När den slutligt valda uppgiften är onboarding, andra köp eller dess
återförsök används följande startprofil. Separata uttryckliga åtaganden och
kvalificerade klick-/dialogsignaler följer sina egna kontaktregler:

| Ålder i dagar | Fasfaktor |
|---|---:|
| 7–10 | 0,35 |
| 11–23 | 0,20 |
| 24–30 | 0,75 |
| 31–45 | 0,90 |
| 46–60 | 0,80 |
| 61–90 | 0,60 |

En känd SKU ger +0,08 dag 7–60, med tak 1. Okänt antal SKU är inte en SKU.
`T_first = min(1, fasfaktor + SKU-tillägg)` och grundprioriteten är
`R_first = V * T_first`. Gemensamma spärrar och eventuell efterdämpning efter
negativ dialog gäller även här.
Generell rutin-T multipliceras inte in och används inte via max; annars
försvinner det avsedda förstaorderfönstret igen. Detta återför den gamla relativa
timingen som en uttrycklig, provisorisk policy.

Obesvarat försök ger två arbetsdagars återförsökspaus. Nådd likadan kontakt ger
minst tre kalenderdagars avstånd om uppgiften fortfarande är öppen. När säkert
kontaktsyfte saknas tillämpas samma konservativa avstånd för en nådd kontakt
efter första leveransen.
Dokumenterat slutförd onboarding stänger uppgiften; den återkommer inte bara för
att pausen löper ut.

### Dialog och återaktivering

Positiv prospektdialog kan följas upp från dag 7 och positiv köparkontakt från
dag 10. Den särskilda varma signalen gäller till dag 45. Dag 46 övergår en
fortfarande relevant möjlighet till normal prioritet utan varm boost; den blir
inte noll enbart genom en segmentbaserad prospektgräns. Historikreview kräver en
uttrycklig osäkerhet i underlaget.

Senaste nådda negativa dialog behålls även efter ett senare missat försök.
Negativ dialog ger 60 dagars paus och efterföljande dämpning. Ett uttryckligt
löfte givet i eller efter den negativa dialogen gäller enligt sin avtalade tid.
Ett äldre löfte efter separat senare negativ dialog behöver granskas. Tidsordning
inom samma dag ska bevaras.

En återköpare med mer än 90 dagars beräknad försening är en
återaktiveringskund. Det särskilda undantaget för materiellt försenad aktiv
återorder får inte ge rutinmässiga samtal var sjunde dag till en gammal inaktiv
relation. Återaktivering behåller normalt minst 30 dagar efter nådd likadan
dialog, med egna uttryckliga åtaganden och kvalificerade nya signaler separat.

### Kontaktförbud

`Återkom ej` är en explicit beständig spärr. Vanlig positiv eller neutral dialog,
ny order, missat försök och utgången negativ paus upphäver den inte.
`Kontakt tillåten` är en separat dokumenterad händelse som kräver ansvarig,
tidpunkt och grund för den nya tillåtelsen. Förbudet gäller även om ett gammalt
löfte eller en bokning behöver administrativ hantering.

## Åtaganden och Att hantera

Åtagandets identitet och resolution hålls separat från kalenderförsökets status.
Ett `completed` försök med resultatet `Ej anträffbar` är inte ett uppfyllt löfte.
Källkontakt, kund-ID, planerad aktivitet, ansvarig, revision och händelse-ID
bevaras genom loggning, ombokning och upplösning.

Att hantera får bara innehålla:

1. Fortfarande öppna kalenderåtaganden med lokalt kalenderdatum **före idag**.
2. Fortfarande öppna uttryckliga uppföljningar med förfallodatum **före idag**.

Dagens bokningar finns i kalendern även efter att dagens klockslag passerat. Ett
löfte med endast dagens datum är ännu inte förfallet. Samma åtagandes
kontaktuppföljning och kalenderpost ger en sammanhängande post, medan två olika
löften för samma kund bevaras var för sig. En ombokning måste bära samma källkoppling.

En orelaterad ny framtida aktivitet avslutar inte ett gammalt löfte. Osäker
historik visas som **Granska uppföljningen** och behöver inte ge tillåtelse till
ny utgående kontakt. Generiska prospekt, återköp, mejlintresse utan datumlöfte och
allmän datakvalitet blir inte extra kommersiella förslag i Att hantera.

Läsning av dessa tillstånd är ren. Virtuella förslag eller åtaganden
materialiseras endast genom en uttrycklig mutation. Mutationerna har stabil
identitet, ägarkontroll, revisionskontroll och idempotenta request-ID:n.
Samma nätverksförsök får inte skapa två kontakter eller två bokningar.

Detta läskontrakt gäller de nya kundinsikterna, kalendern och
`/planning/attention`. Den avvecklade kompatibilitetsvägen
`GET /planning/suggestions` behåller sitt äldre materialiserings- och
reconcile-beteende, eftersom dess äldre snooze/dölj/återuppta-mutationer kräver
lagrade förslags-ID:n och revisioner. Det nya gränssnittet anropar inte den vägen.
Att göra även denna gamla väg helt läsande kräver en samordnad migrering av dess
virtuella revisioner och mutationer; det ingår inte i denna modellväxel.

Snooze/dölj gäller det aktuella beslutets scope. De tar inte bort kunden ur
Affärsvärde eller sökning och raderar inte ett separat kundlöfte. En uttrycklig
åtagandesnooze har egen sluttid och säker återkomst. Återuppta finns även sedan
de gamla rekommendationsrutorna tagits bort.

Skyddet för A-prospekt från den tidigare modellen bevaras: de kan snoozas med
sluttid men inte döljas permanent med vanlig `dismiss`. Historiskt redan dolda
A-prospekt kan fortfarande återupptas. Kontaktförbud är ett separat, uttryckligt
kundbeslut och påverkas inte av detta strategiska portföljskydd.

En säker aktuell bokning kan samtidigt ha äldre separata ärenden som behöver
granskas. Då är bokningen primär på kortet, medan `requires_handling`,
`requires_review` och `review_obligation_ids` behåller granskningsvägen.
Det äldre osäkra ärendet ger inte tillåtelse att ta en extra kontakt och får
inte ensamt förbjuda den nya, giltiga bokningen.

Samma princip gäller en giltig förfallen uppföljning utan aktuell bokning.
Om den först valda äldre uppgiften är blockerad prövas andra förfallna
åtaganden med samma fullständiga beslutsfunktion. Ett giltigt kontaktbart
åtagande kan då bli huvuduppgift och ge kunden Prioritet i Butiker. Alla äldre
granskningsärenden, källkopplingar och revisionsvärden finns kvar. Detta är val
av rätt huvuduppgift, utan extra vikt eller undantag från kontaktspärrarna.

Äldre uppföljningar som har exakt länkad källa, kalenderaktivitet och nådd
slutförandekontakt kan behålla sitt dokumenterade avslut enligt det gamla
gränssnittets kontrakt. Ett senare olänkat samtal, en orderregistrering eller
ett obesvarat försök är inte motsvarande bevis. Äldre datumrader utan säkert
eget sammanhang klassificeras som historikgranskning och får ingen automatisk
ringuppmaning.

## Datakvalitet och tid

Kanonisk kundidentitet väljs före featureaggregering. Namnhistorik får bara kopplas
vid entydig exakt matchning utan motstridiga starka ID:n. Tidigare säljares
historik bevaras när kunden byter ägare. Oklara källkopplingar får inte lösas
med fuzzy matchning.

`confirmed_delivery` betyder i det befintliga källkontraktet ett tolkningsbart
leveransdatum. Det är inte oberoende fysisk leveranskvittens. Datumstyrd
leveranshistorik hålls separat från obekräftade order; en osäker order får inte
radera en känd leveransdag när datum sammanfaller. Ofullständig volym blir inte
noll eller ersatt med fel mängdfält.

En gemensam timezone-aware `as_of` används. Framtida kontaktutfall tas inte som
redan inträffade. Då kända framtida bokningar, löften och leveransplaner finns
kvar för tillgänglighet och spärrar, men räknas inte som genomförd historik.
Historiska utfallstest kräver dessutom informationens `recorded_at` och dåvarande
versioner av masterdata; att bara byta datum mot dagens CRM är inte ett giltigt
prediktivt backtest.

## Reproducerbar skrivskyddad verifiering

Hämta en komplett, privat CRM-snapshot med formatet
`{"sheets": {"sheet_name": {"values": [["header"], ["value"]]}}}`. Spara inte
snapshoten eller kundspecifika resultat i Git. Kör med exakt checkout och
explicit tid, med web-appens beroenden samt `freezegun==1.5.5` installerat:

```bash
python scripts/replay_priority_snapshot.py \
  --snapshot /private/crm.json \
  --as-of 2026-10-18T10:53:07+02:00 \
  --output /private/replay-v33.json

python scripts/review_followup_history.py \
  --snapshot /private/crm.json \
  --as-of 2026-10-18T10:53:07+02:00 \
  --replay /private/replay-v33.json \
  --output /private/followup-review.json

node scripts/compare_priority_replays.cjs \
  --new /private/replay-v33.json \
  --old /private/replay-v22.json \
  --new-repo /private/pr48-checkout \
  --old-repo /private/production-v22-checkout \
  --output /private/rankings.json
```

Replay kör faktisk Flask/backend, samma ägarregler för samtliga aktiva användare,
ren Sheets-läsning och blockerad utgående nätverkstrafik. Proveniens inkluderar
commit, tidsstämpel, snapshot-hash och eventuell lokal diff. Rapporten om gamla
uppföljningar gör inga databasändringar och har ingen apply-flagga.

Gamla completed-försök återöppnas inte blint. Kandidater klassificeras med
åtagandeursprung, källfingeravtryck och uttryckligt länkad evidens. Kontaktförbud
är en separat spärr, aldrig ett bevis på att löftet är öppet eller avslutat.
Eventuell senare migration kräver separat beslut och får inte skriva över
säljarändringar som gjorts efter granskningsrapporten.

## Release och uppföljning

`VALUE_PRIORITY_ENABLED=false` är en driftspärr för den nya prioriteringsvyn:
det versionerade insikts-API:t svarar med `503 value_priority_disabled` och
gränssnittet visar kundregistret med ett tydligt modellfel, utan påhittade poäng.
Det är inte en fullständig återgång till v2.2. Nya åtagandehändelser behålls;
en full återgång kräver ett separat versionsbeslut som respekterar redan sparade
åtaganden och kontaktförbud.

Python-, API- och browserregressioner måste täcka nya åtgärder efter slutligt
workflow, faktisk kontakt-POST och reload, dagens/missade aktiviteter, flera löften,
ombokning, snooze, kontaktförbud, kanal samt samma-dag-händelser. API-testet måste
kräva aktuell modellversion och ett positivt antal testade poster.

Lokal och CI-verifiering redovisas i PR48. Produktionsmigrering, merge och deploy
är separata åtgärder och utförs inte genom att denna kod uppdateras i PR-branchen.
Gamla numeriska parametrar utöver de dokumenterade logikrättningarna är fortsatt
provisoriska. Mät faktisk kontakt, återköp, andra köp, DFP och helst bruttobidrag
per kontakttid under en kontrollerad pilot; högre summerade modellpoäng är inte
uppmätt affärslyft.
