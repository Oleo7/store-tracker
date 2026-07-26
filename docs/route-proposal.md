# Ruttförslag: backendkonfiguration

Den autentiserade endpointen `POST /route-proposal` använder aktuell
serverberäknad prioritetspoäng och Google Routes API för riktade körtider.
Klientens tidsvärden ignoreras. Backend kräver alltid en total tid under
420 minuter, räknar med 20 minuters besökstid per stopp, inkluderar retur till
startpunkten och tillåter högst 15 butiker.

## Miljövariabler

```text
GOOGLE_ROUTES_API_KEY=<backend-only Google Routes API key>
ROUTE_ROUTING_PREFERENCE=TRAFFIC_UNAWARE
ROUTE_MATRIX_TIMEOUT_SECONDS=15
ROUTE_MATRIX_CACHE_TTL_SECONDS=600
```

`TRAFFIC_UNAWARE` är standard även om miljövariabeln saknas eller har ett
ogiltigt värde. Läget använder samma Routes API och samma API-nyckel som tidigare
men begär inte trafikberoende restider. Det undviker att en ny miljö
oavsiktligt använder Compute Route Matrix Pro.

Aktivera Google Routes API för `GOOGLE_ROUTES_API_KEY` och begränsa nyckeln till
webbtjänstens servermiljö. Nyckeln skickas aldrig till frontend. Om variabeln
saknas faller backend tillbaka till `GOOGLE_MAPS_API_KEY`; det är praktiskt
lokalt men rekommenderas inte i produktion eftersom Maps-nyckeln även används av
webbläsaren.

## Beräkningsgränser

Backend hämtar först vägkörtid från användarens position till alla giltiga
kandidater. En deterministisk shortlist begränsar därefter den fulla riktade
matrisen till högst 24 butiker, alltså högst `(1 + 24) × 24 = 600` element.
Ytterligare högst 24 element hämtas för returresan från varje shortlistad butik
till startpunkten.
Shortlisten väger samman prioritetspoäng, poäng per direkt restid, en begränsad
geografisk klustertäthet och en billig ungefärlig körordning. De geografiska
signalerna används bara i förvalet och är beräkningsmässigt begränsade så att
även hela standardunderlaget kan hanteras snabbt.

Endpointen accepterar högst 2 376 kandidatrader, så direktscanningen,
600-elementsmatrisen och de 24 returbenen tillsammans ryms inom standardkvoten
3 000 element per minut.
Fågelvägsavstånd används endast för shortlistens klusterrankning och aldrig för
att verifiera sjutimmarsvillkoret.

Upp till 15 kandidater löses exakt. Större mängder använder en deterministiskt
begränsad beam-sökning. Global optimalitet markeras bara som bevisad när ingen
shortlist behövdes och den exakta lösaren användes. Alla resultat verifieras
separat med heltalssekunder från Routes API innan de returneras.

## Behörighet och dagens sparade rutt

För rollen `Säljare` skapar backend alltid kandidatmängden från
`customers_enriched.sales_person` som matchar den inloggade användarens namn.
Rader som skickas från webbläsaren kan därför inte ge säljaren tillgång till
andra säljares kunder. Övriga roller kan fortsatt använda aktiva filter.

En lyckad beräkning sparas i worksheeten `route_proposals`, med en post per
användarnamn och Stockholmsdatum. `GET /route-proposal` hämtar dagens sparade
förslag utan geolokalisering eller ett nytt Routes API-anrop. `POST
/route-proposal` kontrollerar samma lagring igen under ett backendlås före en ny
beräkning, så upprepade klick samma dag återanvänder samma startpunkt,
stoppordning och tidsberäkning. Misslyckade beräkningar sparas inte och kan
försökas igen.
