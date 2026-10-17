# Store Tracker – steg 1: Affärsvärde och Kontaktprioritet (v3.2-preview)

**Status:** PR #48 för granskning, ännu inte mergad eller deployad. Beräkningsmodellen är en policy/pilot, inte en validerad prognos för köpchans, omsättning eller täckningsbidrag.

## Varför två poäng?

- **Affärsvärde** är en DFP-baserad uppskattning av affärsmöjligheten oberoende av när kunden senast nåddes och när nästa kontakt är bokad.
- **Kontaktprioritet** använder samma underliggande affärsvärde och ett aktualitets-/arbetsflödesbeslut. En stor kund som nåddes i går kan ha högt Affärsvärde men låg Kontaktprioritet. En långvarigt bortglömd tidigare köpande kund kan ha höga båda.
- Det är en **sammanhängande modell**, inte två oberoende poängalgoritmer. Sortering använder råvärden före avrundade 0–100-index.

## Värdeunderlaget

Uppskattad storlek per verifierat genomfört leveranstillfälle:

```text
Q = (n * kundens historiska genomsnittliga DFP
     + k * segmentgruppens median-DFP) / (n + k)
```

`n` avser verifierade genomförda leveransdatum med komplett volym, inte orderrader. `k=3` är en **försiktig testparameter**. Förhandsvärden per A/B/C baseras på köpande aktiva butikskunders DFP per genomförd leverans (en kund = en observation i medianen). Saknas referensvärden används en dokumenterad fallback. Kundens egen historik ersätter gradvis förhandsvärdet. Gamla order med enbart orderdatum räknas **inte** som bekräftade leveranser i denna nya beräkning; äldre v2.2-fält får behålla tidigare semantik.

90-dagars DFP-proxy:

```text
V = Q * beräknat antal leveranser under perioden
      * antagen realiseringsfaktor
```

Prospekt 0,50, en tidigare leverans 0,70, återkommande 0,80 före återaktiverings-/stalenessjustering. Dessa är **antaganden**, inte modellerade sannolikheter. Förväntad frekvens från köpcykeln har ett dokumenterat tak. För mycket gamla relationer reduceras antagen realisering, men den historiska observerade leveransstorleken skrivs inte om. Saknat och ofullständigt underlag märks som lägre säkerhet.

## Kontaktprioritet och åtgärd

```text
R = V * T
```

`T` interpoleras från rutinankare: 0 d=0, 7 d=0,02, 20 d=0,15, 30 d=0,35, 40 d=0,55, 50 d=0,75, 60 d=0,90, 90+ d=1,00. Detta är arbetsregler och inte konverteringssannolikheter. Uppföljningar med verkliga åtaganden eller kvalificerade nya signaler kan motivera kontakt före vanlig tidskurva.

Butikslistans åtgärdsbeslut är **separat från det gamla Planering-kontraktet**. Kunden får inte uppmanas till ny rutinbearbetning vid uttryckligt `Återkom ej`, registrerad framtida kontakt/leverans, giltig paus, för ny dialog eller för kort intervall efter `Ej anträffbar`. Två arbetsdagar gäller för nytt försök. Efter tre missade försök i samma relevanta kontaktsekvens inom 30 dagar föreslås kontroll av kontaktväg. Obesvarat försök avslutar inte nya butiksvyns uttryckliga uppföljning. De äldre planeringsuppföljningarnas semantik och historiska KPI behålls av kompatibilitetsskäl fram till etapp 2.

Visade index:

```text
score = round(100 * raw / (raw + K))
```

Nuvarande **provisoriska** K=2,625. Det ger en aldrig nyligen nådd A-prospektkund vid segmentmedian 21 DFP och 50% antaget realiserande ungefär 80 poäng efter 90 dagar. Det är ett tolkningsankare, **inte** en empirisk kalibrering eller 80% köpchans. Skalan behöver jämföras med hela verkliga portföljen före produktionsbeslut. Sorteringsrank påverkas inte av den monotona avrundningen.

## Integration och kompatibilitet

- `GET /customer-insights` utan speciell header behåller befintligt namnnycklat format.
- `X-PB-Value-Model: v3.1` lägger till `__by_customer_id` med nya råvärden, poäng, senaste faktiska dialog, senaste försök och `list_guidance`. En säljares svar innehåller enbart dennes tillgängliga kunder. Icke-unika namn används inte som osäker matchningsnyckel.
- Butiker renderar `list_guidance`; befintlig `customer_guidance`, `priority_score` och Planering/Karta/Säljcoachning ändras inte numeriskt av val i butikslistan.
- Standardsortering: åtgärdsbar Kontaktprioritet. Alternativ sortering: Affärsvärde. Fallande råvärde, stabil kund-ID-tiebreak. Kunder utan aktuellt kontaktbehov kan ha högt affärsvärde utan att hamna överst på kontaktlistan.
- Planering har kvar sin nuvarande rekommendationsyta i etapp 1. Framtida etapp 2 ska ersätta den med **Att hantera**, endast för missade kalenderaktiviteter och förfallna uttryckliga uppföljningar – inte bokningar som redan visas i kalendern.

## Validering och återställning

CI kör Python-regressioner, API-test med faktisk Flask-endpoint, desktop/mobil Chromium-smoke med riktiga sorteringskontroller, äldre Planering/Karta tester och kalenderdragning. Fysisk iPhone/Safari är inte verifierad här.

**Kvar innan produktionsbeslut:** full portföljreplay med aktuellt CRM_DATABASE, kalibrering mot historiska utfall och användbar poängfördelning, kontroll av topp 20 per säljare och avvikande A/B/C-kunder. Parametrarna är versionsstyrda i Python; adminpanelen och styrd livekonfiguration byggs inte i etapp 1.

Rekommenderat införande: granska PR:s diff och testresultat, jämför simulerad ranking i skrivskyddat underlag, testa manuellt i staging innan merge. Vid akut regressionsbehov efter senare driftsättning ska senaste bevisat fungerande Render-commit deployas tillbaka (och datamigreringar ska undvikas i etapp 1). Det finns **ingen datamigrering** eller ändring i CRM-databasen i denna PR.

## Förfinade kommersiella signaler (PR48)

- **Positiv dialog med tidigare köpande kund:** när senaste verkliga dialog var `Positiv` och ingen senare order registrerats ges kontaktbehovet en särskild, tidstyrd förstärkning. Ett informellt positivt besked ger normalt ingen omedelbar uppmaning; 10–45 dagar utan order kan ge ”Följ upp positiv dialog”. En avtalad uppföljning har företräde. Effekten berör **Kontaktprioritet, inte Affärsvärde**, och upphör efter ny order eller annan verklig dialog.
- **Betydligt försenat återköp:** när en återkommande kund ligger minst 14 dagar efter beräknad återköpstid kan ”Säkra försenat återköp” aktiveras redan sju dagar efter senaste neutrala/positiva dialog. En nylig dialog kortare än sju dagar, kontaktförbud, negativ kontaktpaus, bokning och senare ny order får inte kringgås.
- **Registrerad order utan bekräftad leverans:** en ny, ännu obekräftad order efter senaste verifierade leverans stoppar rekommendationen om försenat återköp. Under 14 dagar visas ”Avvakta registrerad order”. Mellan dag 15 och 60 blir åtgärden ”Kontrollera orderstatus” i stället för ytterligare återorderpåtryckning. Äldre order utan leveransbekräftelse markeras som en **historisk datakvalitetsfråga**, inte som återkommande akut säljsamtal. **Det betyder inte att leveransen säkert är utebliven** – saknat leveransdatum är osäkert underlag. Uttryckliga kontaktlöften avslutas inte automatiskt av en order.
- **Negativ dialog:** ett verifierat `Negativ` medför fortsatt 60 dagars kontaktpaus om ingen senare order eller ny verklig dialog ger ändrad information. Även därefter sänks Kontaktprioriteten: faktor 0,35 dag 60–89; 0,55 dag 90–179; 0,75 från dag 180. Faktorn multipliceras **efter** övriga signalhöjningar och gäller tills en senare nådd dialog eller order ändrar läget. En uppföljning som kunden uttryckligen avtalade vid ett negativt besked behåller sin förfallna åtgärd. `Återkom ej` är separat och blockerar helt.

Dessa tider och faktorer är **pilotregler**, inte uppmätta sannolikheter; de måste utvärderas mot verklig återköpsutveckling och förändringar i topplistorna innan en produktionsmerge. V2.2:s gamla poäng och Planerings nuvarande rekommendationsgränssnitt lämnas intakta under etapp 1.

- **Köpcykel och robusthet:** v3.2 beräknar både segmentets referensköpcykel och varje köpares köpcykel enbart från verifierade leveransdatum. En senare obekräftad order kan därför inte artificiellt ändra Affärsvärde via gamla v2.2-benchmarkvärden.
