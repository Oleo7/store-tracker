# Polarbär Meta Ads och Instagram

Den här grenen lägger till serverbaserade läsningar, begränsade Ads-skrivningar och
en backend för Instagram-tävlingar i den befintliga Store Tracker-tjänsten.
Den är byggd för Business Portfolio `polarbar.se` (`1281349120346231`), Ads-konto
`act_1689435278682373`, Meta-appen `1553369652626144`, Instagram-kontot
`17841475991503244` och Facebook-sidan `868369943031594`. Ett annat
`META_AD_ACCOUNT_ID` ger `account_not_allowed`.

## Drift och behörighet

Alla nya `/meta/*`-endpoints kräver Store Tracker-inloggning. Preview, apply,
giveaway-snapshot och dragning kräver dessutom `user_is_admin`. Skrivningar
accepterar bara JSON och förkastar cross-site browseranrop. Graph-anropen sker
server-side över HTTPS med `Authorization: Bearer`; token finns aldrig i URL,
frontend eller API-svar. Inga raw Graph-, delete- eller bulk-write-endpoints finns.

`INSTAGRAM_ACCESS_TOKEN` används för Instagram. Ads-läsningar väljer
`META_ADS_ACCESS_TOKEN` när den finns och kan annars använda Instagram-token.
Ads-skrivningar kräver en separat `META_ADS_ACCESS_TOKEN`, beviljade
`ads_read` och `ads_management`, åtkomst till rätt Ads-konto samt
`META_ADS_WRITE_ENABLED=true`. Capability-kontrollen ger bara metadata.
Sätt aldrig token i källkod eller i ett terminalkommando som kan loggas.

Render Blueprint har skrivflaggan `false`. Behåll den så tills live-token,
kontobehörighet, produktionens read-anrop, säkra PAUSED-testobjekt och audit
har verifierats. Budget-write kräver dessutom användarbeslutade positiva
`META_ADS_MAX_DAILY_BUDGET_MINOR` respektive
`META_ADS_MAX_LIFETIME_BUDGET_MINOR`. Gränserna avser Metas minor units,
inte kronor. Servern hämtar kontovalutan och blockerar valutor som ännu inte
har en uttryckligt stödd minor-unit-faktor. Ingen gräns har hittats på.

## API

| Läsning | Endpoint |
| --- | --- |
| Konto | `GET /meta/ads/account` |
| Kampanjer / ad sets / ads / creatives | `GET /meta/ads/campaigns`, `/adsets`, `/ads`, `/creatives` |
| Enskilt objekt | `GET /meta/ads/<plural>/<id>` |
| Targeting | `GET /meta/ads/adsets/<id>/targeting` |
| Insights | `GET /meta/ads/insights` |
| Capability | `GET /meta/capabilities` (admin) |
| Instagram media / kommentarer | `GET /meta/instagram/media`, `/media/<id>`, `/media/<id>/comments` |

Ads-listor har `limit` 1–100 och läser högst fem Graph-sidor per anrop.
Insights stöder `level=account|campaign|adset|ad`, `since`/`until`,
`date_preset`, `time_increment`, JSON-listan `breakdowns`, JSON-listan
`filtering` och `after`-cursor. Resultatet inkluderar de råa actions och
action_values; köp och ROAS har ingen förvald definition.

| Preview | Förväntad JSON |
| --- | --- |
| `POST /meta/ads/targeting/preview` | `{"object_id":"...","mutation":{"mode":"set_age_range","age_min":25,"age_max":60}}` |
| `POST /meta/ads/budget/preview` | `{"object_type":"adset","object_id":"...","budget_type":"daily_budget","amount_minor":70000}` |
| `POST /meta/ads/status/preview` | `{"object_type":"ad","object_id":"...","status":"PAUSED"}` |
| `POST /meta/ads/campaigns/preview-create` | `{"name":"...","objective":"OUTCOME_TRAFFIC"}` |
| `POST /meta/ads/adsets/preview-create` | Schema i `meta_ads.py`; kräver kampanj, mål, targeting och budget eller kampanjbudget |
| `POST /meta/ads/creatives/preview-create` | Befintligt Page-post-ID för Polarbärs Page |
| `POST /meta/ads/ads/preview-create` | `{"adset_id":"...","creative_id":"...","name":"..."}` |

Targeting-mutationer: `replace_geo_locations`, `add_custom_locations`,
`remove_custom_locations`, `set_age_range`, `set_platforms`,
`replace_interests`. Preview läser hela targeting-objektet, bevarar övriga
fält och visar `before`, `proposed_after` och `diff`. Geografi får inte bli
tom. Nya campaign, ad set och ad skapas alltid som `PAUSED`. COST_CAP,
osäkra objective/optimization-kombinationer och obekräftade creative-modeller
blockeras tills de kan kontrolleras mot livekontot.

Varje preview sparas i Google Sheets `meta_ads_changes` och ger ett UUID,
SHA-256-fingeravtryck samt 30 minuters giltighet. För att genomföra den:

```http
POST /meta/ads/changes/<change_id>/apply
Content-Type: application/json

{"confirm":true}
```

Visa först diffen för användaren och invänta ett uttryckligt godkännande.
Apply läser aktuell Meta-state, jämför fingeravtryck, markerar preview som
förbrukad före POST, skickar bara det typade fältet, läser tillbaka objektet
och skriver resultat till `meta_ads_audit`. `STALE_STATE` och utgångna
previews stoppas. Vid oklar Meta-respons eller read-back-fel får samma
change ID aldrig användas igen. Den befintliga en-instans/en-worker-Render-
topologin behövs eftersom appliceringens lås är processlokalt.

## Instagram-tävling

`POST /meta/giveaways/snapshots` med `{"media_id":"..."}` hämtar högst
1 000 kommentarer på upp till tio sidor från Polarbärs eget inlägg och
sparar snapshot + SHA-256-hash i Google Sheets. `POST /meta/giveaways/draws`
tar `snapshot_id`, `rules`, `confirm:true` och valfri `seed`. Reglerna stöder
`required_keyword`, `excluded_usernames`, `start_at`, `end_at` och
`winner_count` 1–10. Ett användarnamn får en lott även vid flera kommentarer.
En slumpmässigt genererad seed används om ingen anges. Vinnarna rangordnas
med SHA-256 av seed, snapshot-hash och normaliserat användarnamn. Snapshot,
regler, seed, vinnare och tid sparas så dragningen går att reproducera.
Graph-paginering ger en insamlad ögonblicksbild, inte en transaktionell
frysning av Instagram-kommentarer. Följare eller vän-taggar kan inte
verifieras med denna backend.

## Återstående livekontroller

Den aktuella Codex-browser-sessionen kan inte öppna kampanjer för
`1689435278682373` trots att användaren manuellt verifierat dem i sin
vanliga session. Konto `221082042` får inte användas. Ingen Ads-token med
`ads_management` har verifierats eller lagts i Render. Utför därför ingen
produktionsaktivering förrän rätt Facebook-session/token når det angivna
kontot. Kontrollera sedan read-endpoints och valuta, konfigurera eventuella
budgettak, testa skapande av ett dedikerat PAUSED-objekt utan spend,
kontrollera audit/read-back och slå först därefter på skrivflaggan.

Ingen verklig targeting- eller budgetändring på en aktiv annons får göras
utan ett utpekat objekt, visad preview och separat användargodkännande.
