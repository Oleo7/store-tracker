# Brevo mejlförslag V1 – drift

## CRM_DATABASE

Store Tracker läser användare från fliken `users` med kolumnerna `user_name`,
`name`, `role`, `email`, `phone`, `password` och `active`. Endast användare med
`active=Y` kan logga in.

Fliken `settings` ska innehålla:

- `reminder_product_sheet_url`
- `reactivation_product_sheet_url`
- `new_customer_product_sheet_url`
- `reminder_stockfiller_url`
- `sku_10001` till `sku_10006`

De tre produktbladslänkarna används för Påminnelse, Återaktivering respektive
Nykund. Alla typer använder samma Stockfiller-länk. Om någon av de två nya
produktbladslänkarna saknas används `reminder_product_sheet_url` tillfälligt och
Store Tracker visar en varning i utkastet.

Produktradernas visningsnamn hämtas alltid från `sku_`-raderna. De fyra fasta
startmixarna använder `sku_10003`, `sku_10005`, `sku_10002` och `sku_10006` i
den ordningen. Ändras ett produktnamn i `settings` används det nya namnet i nya
utkast och i produktväljaren utan kodändring.

Listfiltret `Mejl-förslag` visar endast kunder som har minst en giltig och ej
blockerad mottagaradress. Relationstypen påverkas inte av kontakt- eller
utskickscooldown; sådana regler hanteras fortfarande som varningar eller stopp
när utkastet öppnas och skickas.

Vid första användningen skapar appen flikarna `email_messages`,
`email_recipients` och `email_events`. Den kompletterar även
`sales_activities` med kolumnen `email_id` om den saknas.
`email_messages.email_type` lagrar `reminder`, `reactivation` eller
`new_customer` för varje skickat mejlförslag.

## Miljövariabler

Samtliga variabler från `.env.example` ska finnas i driftmiljön. Börja alltid
med `EMAIL_SEND_MODE=test`. Testutskick omdirigeras till
`EMAIL_TEST_RECIPIENT` och visas inte i ordinarie kundtidslinje.

## Brevo-webhook

Skapa webhooken först när den aktuella koden är driftsatt på Render. I Brevo:

1. Öppna kontomenyn och välj `Integrations > Webhooks`.
2. Välj `Add webhook > Outbound webhook`.
3. Ange produktionsadressen nedan och välj `No authentication` eftersom den
   hemliga URL-delen redan används som autentisering.
4. Välj `Send one at a time` och kategorin `Transactional email`.
5. Aktivera `Sent`, `Delivered`, `Opened`, `Clicked`, `Soft bounced`,
   `Hard bounced`, `Invalid email`, `Blocked`, `Spam`, `Unsubscribed`,
   `Deferred` och `Error`.

Brevos transaktionswebhook ska peka på:

```text
https://<store-tracker-host>/api/brevo/webhook/<BREVO_WEBHOOK_SECRET>
```

Aktivera händelser för leverans, öppning, klick, hard bounce, blockering, spam
och avregistrering. Tekniska händelser sparas i `email_events`, medan endast
godkända affärshändelser visas på kundkortet.

## Livesättning

Byt till `EMAIL_SEND_MODE=live` först när:

1. båda avsändardomänerna visar **Branded** i Brevo,
2. testmejl från samtliga avsändardomäner har godkänts,
3. öppnings- och klickhändelser når kundens tidslinje,
4. Reply-To och den personliga signaturen har verifierats.
