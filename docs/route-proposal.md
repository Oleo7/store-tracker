# Ruttförslag: backendkonfiguration

Den autentiserade endpointen `POST /route-proposal` använder aktuell
serverberäknad prioritetspoäng och Google Routes API för riktade körtider.
Klientens tidsvärden ignoreras; backend använder alltid 480 minuter totalt och
20 minuters besökstid per stopp.

## Miljövariabler

```text
GOOGLE_ROUTES_API_KEY=<backend-only Google Routes API key>
ROUTE_ROUTING_PREFERENCE=TRAFFIC_AWARE
ROUTE_MATRIX_TIMEOUT_SECONDS=15
ROUTE_MATRIX_CACHE_TTL_SECONDS=600
```

Aktivera Google Routes API för `GOOGLE_ROUTES_API_KEY` och begränsa nyckeln till
webbtjänstens servermiljö. Nyckeln skickas aldrig till frontend. Om variabeln
saknas faller backend tillbaka till `GOOGLE_MAPS_API_KEY`; det är praktiskt
lokalt men rekommenderas inte i produktion eftersom Maps-nyckeln även används av
webbläsaren.

## Beräkningsgränser

Backend hämtar först vägkörtid från användarens position till alla giltiga
kandidater. En deterministisk shortlist begränsar därefter den fulla riktade
matrisen till högst 24 butiker, alltså högst `(1 + 24) × 24 = 600` element.
Shortlisten väger samman prioritetspoäng, poäng per direkt restid, en begränsad
geografisk klustertäthet och en billig ungefärlig körordning. De geografiska
signalerna används bara i förvalet och är beräkningsmässigt begränsade så att
även hela standardunderlaget kan hanteras snabbt.

Endpointen accepterar högst 2 400 kandidatrader, så direktscanningen och den
fulla matrisen tillsammans ryms inom standardkvoten 3 000 element per minut.
Fågelvägsavstånd används endast för shortlistens klusterrankning och aldrig för
att verifiera åttatimmarsvillkoret.

Upp till 15 kandidater löses exakt. Större mängder använder en deterministiskt
begränsad beam-sökning. Global optimalitet markeras bara som bevisad när ingen
shortlist behövdes och den exakta lösaren användes. Alla resultat verifieras
separat med heltalssekunder från Routes API innan de returneras.

Kvoten gäller hela Google Cloud-projektet. Om flera säljare ofta skapar
ofiltererade rutter samtidigt bör Routes API-kvoten höjas eller arbetsflödet
styras mot region-/ansvarigfilter innan beräkningen.
