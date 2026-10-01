from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import sys
from unittest import TestCase
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import app as app_module
import geocoding
from route_proposal import Coordinate
from tests.test_planning import ConstantRoadProvider, default_spreadsheet
from tests.test_route_optimization import NOW, workday_successful_response


GPS = {"latitude": 57.70887, "longitude": 11.97456, "accuracy": 12}
HOME = Coordinate(56.045, 12.695)
PRIVATE_ADDRESS = "Testhemvägen 123"
PRIVATE_TOWN = "Testhemorten"
DAY = (NOW + timedelta(days=1)).date().isoformat()


class PlanningRouteOriginTests(TestCase):
    def setUp(self):
        app_module.app.config.update(TESTING=True, SECRET_KEY="route-origin-test")
        self.sheet = default_spreadsheet()
        self.users = self.sheet.worksheet(app_module.USERS_SHEET)
        self.users.values[0].extend(["home_adress", "home_town"])
        for row in self.users.values[1:]:
            row.extend([PRIVATE_ADDRESS, PRIVATE_TOWN])
        self.set_user("sofia", user_name="johan", name="Johan")
        customers = self.sheet.worksheet("customers_enriched")
        owner_index = customers.values[0].index("sales_person")
        customers.values[2][owner_index] = "Johan"
        self.client = app_module.app.test_client()
        for context in (
            patch.dict(os.environ, {"ROUTE_ENGINE": "route_optimization",
                                   "ROUTE_OPTIMIZATION_PROJECT": "test-project",
                                   "ROUTE_OPTIMIZATION_GOOGLE_CREDENTIALS": "test-placeholder",
                                   "GOOGLE_MAPS_API_KEY": "fake-geocoding-key"}),
            patch.object(app_module, "get_spreadsheet_with_retry", return_value=self.sheet),
            patch.object(app_module, "stockholm_now", return_value=NOW),
            patch.object(app_module, "stockholm_today", return_value=NOW.date()),
        ):
            context.start()
            self.addCleanup(context.stop)
        geocoding._cache.clear()
        self.addCleanup(geocoding._cache.clear)
        self.login("olle")
        self.bodies = []

        def optimize(*, project, body, timeout_seconds):
            self.bodies.append(body)
            items = [{"customer_id": item["label"].split(":", 1)[1]}
                     for item in body["model"]["shipments"]]
            start = datetime.fromisoformat(body["model"]["globalStartTime"].replace("Z", "+00:00"))
            response = workday_successful_response(items, start=start)
            response["routes"][0]["vehicleLabel"] = body["model"]["vehicles"][0]["label"]
            return response, 200

        self.provider = Mock()
        self.provider.optimize.side_effect = optimize
        for context in (
            patch.object(app_module, "route_optimization_provider", return_value=self.provider),
            patch.object(app_module, "get_authoritative_priority_snapshot", side_effect=self.snapshot),
        ):
            context.start()
            self.addCleanup(context.stop)

    def set_user(self, key, **changes):
        row = next(row for row in self.users.values[1:]
                   if row[self.users.values[0].index("user_name")] == key)
        for name, value in changes.items():
            row[self.users.values[0].index(name)] = value
        app_module.invalidate_sheet_for_write(self.users)

    def login(self, key):
        user = next(row for row in self.users.dict_rows() if row["user_name"] == key)
        with self.client.session_transaction() as session:
            session["user"] = app_module.public_user(user)

    def snapshot(self, *_args, **_kwargs):
        customers = app_module.get_customer_rows(self.sheet)
        return {"customers": customers, "contact_rows": [],
                "priorities": [{"row": item["row"], "customer_id": item["customer_id"], "priority_score": 80}
                               for item in customers]}

    def preview(self, **changes):
        payload = {"route_date": DAY, "route_mode": "automatic", "client_request_id": "home-preview"}
        payload.update(changes)
        return self.client.post("/planning/route-preview", json=payload)

    def assert_origin(self, response, coordinate, source):
        self.assertEqual(response.status_code, 200, response.get_json())
        vehicle = self.bodies[-1]["model"]["vehicles"][0]
        expected = {"latitude": coordinate.latitude, "longitude": coordinate.longitude}
        self.assertEqual(vehicle["startLocation"], expected)
        self.assertEqual(vehicle["endLocation"], expected)
        preview = response.get_json()
        self.assertEqual(preview["start"], expected)
        self.assertEqual(preview["origin_source"], source)
        signed = app_module.planning_preview_serializer().loads(preview["preview_token"])
        self.assertEqual(signed["start"], expected)
        self.assertEqual(signed["origin_source"], source)
        return preview

    def assert_local_failure(self, response, code, status=422):
        self.assertEqual(response.status_code, status, response.get_json())
        self.assertEqual(response.get_json()["error"], code)
        self.provider.optimize.assert_not_called()
        self.assertNotIn(app_module.ROUTE_OPTIMIZATION_RUNS_SHEET, self.sheet.added_sheets)
        self.assertEqual(self.sheet.worksheet(app_module.PLANNED_ACTIVITIES_SHEET).dict_rows(), [])

    def assert_private(self, value):
        text = json.dumps(value, ensure_ascii=False)
        for private in ("home_adress", "home_town", PRIVATE_ADDRESS, PRIVATE_TOWN):
            self.assertNotIn(private, text)

    def test_seller_and_admin_own_calendar_keep_browser_gps(self):
        with patch.object(app_module, "geocode_address") as geocode:
            self.assert_origin(self.preview(start=GPS, origin_source="selected_owner_home"),
                               Coordinate(GPS["latitude"], GPS["longitude"]),
                               "current_position")
            self.set_user("olle", admin="Y")
            self.login("olle")
            # A sales user who is also admin still plans their own calendar from GPS.
            self.assert_origin(self.preview(user_name="OLLE", start=GPS),
                               Coordinate(GPS["latitude"], GPS["longitude"]), "current_position")
            geocode.assert_not_called()

    def test_admin_selected_owner_home_is_generic_and_ignores_forged_start(self):
        self.login("admin")
        with patch.object(app_module, "geocode_address", return_value=HOME) as geocode:
            for owner in ("johan", "olle"):
                for start in (None, GPS, "invalid coordinates"):
                    with self.subTest(owner=owner, start=start):
                        args = {"user_name": owner, "client_request_id": f"home-{owner}"}
                        if start is not None:
                            args["start"] = start
                        preview = self.assert_origin(self.preview(**args), HOME, "selected_owner_home")
                        self.assertNotIn("din position", preview["gps_notice"])
            geocode.assert_called_with(f"{PRIVATE_ADDRESS}, {PRIVATE_TOWN}, Sweden", cache=True)
        self.assertEqual(self.provider.optimize.call_count, 2)

    def test_non_admin_other_calendar_remains_forbidden_even_without_gps(self):
        with patch.object(app_module, "geocode_address") as geocode:
            self.assert_local_failure(self.preview(user_name="johan"), "planning_owner_forbidden", 403)
            geocode.assert_not_called()

    def test_own_calendar_still_requires_valid_gps(self):
        for start in (None, {"latitude": 999, "longitude": 12}):
            with self.subTest(start=start):
                self.assert_local_failure(self.preview(start=start), "invalid_start", 400)

    def test_incomplete_home_address_fails_before_quota_and_provider(self):
        self.login("admin")
        for address, town in (("", PRIVATE_TOWN), (PRIVATE_ADDRESS, ""), ("  ", " ")):
            with self.subTest(address=address, town=town), patch.object(
                app_module, "execute_route_optimization",
            ) as execute, patch.object(app_module, "geocode_address") as geocode:
                self.set_user("johan", home_adress=address, home_town=town)
                self.assert_local_failure(self.preview(user_name="johan", start=GPS),
                                          "route_owner_home_address_missing")
                execute.assert_not_called()
                geocode.assert_not_called()

    def test_old_users_sheet_still_reads_but_has_no_home_origin(self):
        self.users.values = [row[:-2] for row in self.users.values]
        app_module.invalidate_sheet_for_write(self.users)
        before = [list(row) for row in self.users.values]
        rows = app_module.get_user_rows(self.sheet)
        self.assertEqual(rows[0]["home_adress"], "")
        self.assertEqual(rows[0]["home_town"], "")
        self.assertEqual(self.users.values, before)
        self.login("admin")
        self.assert_local_failure(self.preview(user_name="johan"), "route_owner_home_address_missing")

    def test_geocoding_failures_are_private_local_and_never_reserve_quota(self):
        self.login("admin")
        for failure in (Mock(json=Mock(return_value={"status": "ZERO_RESULTS"})),
                        Mock(json=Mock(return_value={"status": "REQUEST_DENIED"})),
                        RuntimeError(f"private URL {PRIVATE_ADDRESS} fake-geocoding-key")):
            with self.subTest(failure=type(failure).__name__), patch.object(
                geocoding.requests, "get", side_effect=[failure],
            ), patch.object(app_module, "execute_route_optimization") as execute, patch.object(
                app_module.app.logger, "exception",
            ) as logs:
                response = self.preview(user_name="johan", start=GPS)
                self.assert_local_failure(response, "route_owner_home_geocode_failed")
                self.assert_private(response.get_json())
                execute.assert_not_called()
                logs.assert_not_called()
        with patch.dict(os.environ, {"GOOGLE_MAPS_API_KEY": ""}), patch.object(
            geocoding.requests, "get",
        ) as get:
            self.assert_local_failure(self.preview(user_name="johan"), "route_owner_home_geocode_failed")
            get.assert_not_called()

    def test_home_preview_recovery_and_idempotent_apply_do_not_expose_address(self):
        self.login("admin")
        result = Mock(json=Mock(return_value={"status": "OK", "results": [{
            "geometry": {"location": {"lat": HOME.latitude, "lng": HOME.longitude}},
        }]}))
        with patch.object(geocoding.requests, "get", return_value=result) as get:
            preview = self.assert_origin(self.preview(user_name="johan"), HOME, "selected_owner_home")
            replay = self.assert_origin(self.preview(user_name="johan", start=GPS), HOME, "selected_owner_home")
            self.assertEqual(preview["route_optimization_fingerprint"], replay["route_optimization_fingerprint"])
            self.assertEqual(self.provider.optimize.call_count, 1)
            get.assert_called_once()
            status = self.client.get("/planning/route-preview-status?client_request_id=home-preview")
            self.assertEqual(status.get_json()["state"], "completed")
            for _ in range(2):
                applied = self.client.post("/planning/route-apply", json={
                    "user_name": "johan", "preview_token": preview["preview_token"],
                    "client_request_id": "home-apply",
                })
                self.assertEqual(applied.status_code, 200, applied.get_json())
            self.assertTrue(applied.get_json()["duplicate"])
        self.assertEqual(len(self.sheet.worksheet(app_module.PLANNED_ACTIVITIES_SHEET).dict_rows()), 1)
        for value in (preview, app_module.planning_preview_serializer().loads(preview["preview_token"]),
                      applied.get_json(), status.get_json(), self.client.get("/session").get_json()):
            self.assert_private(value)
        for user in app_module.get_user_rows(self.sheet):
            self.assert_private(app_module.public_user(user))
        week = self.client.get(f"/planning/activities?start={DAY}&end={DAY}&user_name=johan")
        self.assertEqual(week.status_code, 200, week.get_json())
        self.assert_private(week.get_json())

    def test_home_coordinates_remain_part_of_route_fingerprint(self):
        self.login("admin")
        with patch.object(app_module, "geocode_address", return_value=HOME):
            first = self.assert_origin(self.preview(user_name="johan"), HOME, "selected_owner_home")
        moved = Coordinate(HOME.latitude + 0.01, HOME.longitude)
        with patch.object(app_module, "geocode_address", return_value=moved):
            second = self.assert_origin(self.preview(user_name="johan", client_request_id="moved-home"),
                                        moved, "selected_owner_home")
        self.assertNotEqual(first["route_optimization_fingerprint"], second["route_optimization_fingerprint"])

    def test_legacy_uses_the_same_home_coordinate_without_client_gps(self):
        self.login("admin")
        captured = []

        class Provider(ConstantRoadProvider):
            def get_matrix_seconds(self, origins, destinations, **kwargs):
                captured.append((origins, destinations))
                return super().get_matrix_seconds(origins, destinations, **kwargs)

        with patch.dict(os.environ, {"ROUTE_ENGINE": "legacy"}), patch.object(
            app_module, "geocode_address", return_value=HOME,
        ), patch.object(app_module, "get_route_travel_time_provider", return_value=Provider()):
            response = self.preview(user_name="johan", start=GPS)
        self.assertEqual(response.status_code, 200, response.get_json())
        self.assertEqual(response.get_json()["start"], {"latitude": HOME.latitude, "longitude": HOME.longitude})
        self.assertEqual(response.get_json()["origin_source"], "selected_owner_home")
        self.assertEqual(captured[-1][0][0], HOME)
        self.assertEqual(captured[-1][1][0], HOME)


class GeocodingTests(TestCase):
    def setUp(self):
        geocoding._cache.clear()
        self.addCleanup(geocoding._cache.clear)
        environment = patch.dict(os.environ, {"GOOGLE_MAPS_API_KEY": "fake-geocoding-key"})
        environment.start()
        self.addCleanup(environment.stop)

    def test_normalized_success_cache_expires_and_missing_key_never_uses_cache(self):
        response = Mock(json=Mock(return_value={"status": "OK", "results": [{
            "geometry": {"location": {"lat": HOME.latitude, "lng": HOME.longitude}},
        }]}))
        with patch.object(geocoding.requests, "get", return_value=response) as get, patch.object(
            geocoding.time, "monotonic", return_value=0,
        ) as clock:
            self.assertEqual(geocoding.geocode_address("  Testvägen  1, Ort, Sweden ", cache=True), HOME)
            self.assertEqual(geocoding.geocode_address("testvägen 1, ort, sweden", cache=True), HOME)
            get.assert_called_once_with("https://maps.googleapis.com/maps/api/geocode/json",
                                        params={"address": "Testvägen 1, Ort, Sweden",
                                                "key": "fake-geocoding-key", "language": "sv"}, timeout=10)
            clock.return_value = geocoding.CACHE_TTL_SECONDS + 1
            self.assertEqual(geocoding.geocode_address("Testvägen 1, Ort, Sweden", cache=True), HOME)
            self.assertEqual(get.call_count, 2)
            with patch.dict(os.environ, {"GOOGLE_MAPS_API_KEY": ""}):
                self.assertIsNone(geocoding.geocode_address("Testvägen 1, Ort, Sweden", cache=True))
            self.assertEqual(get.call_count, 2)

    def test_invalid_geocoding_shapes_coordinates_and_http_errors_are_not_cached(self):
        values = [{}, {"status": "OK", "results": []}, {"status": "OK", "results": [{}]}]
        for lat, lng in ((float("nan"), 12), (91, 12), (56, 181), (True, 12), (56, None)):
            values.append({"status": "OK", "results": [{"geometry": {"location": {"lat": lat, "lng": lng}}}]})
        for payload in values:
            with self.subTest(payload=payload), patch.object(geocoding.requests, "get", return_value=Mock(
                json=Mock(return_value=payload),
            )):
                self.assertIsNone(geocoding.geocode_address("Invalid address", cache=True))
                self.assertFalse(geocoding._cache)
        with patch.object(geocoding.requests, "get", return_value=Mock(
            raise_for_status=Mock(side_effect=RuntimeError("HTTP error with private URL")),
        )):
            self.assertIsNone(geocoding.geocode_address("Invalid address", cache=True))

    def test_customer_address_update_reuses_helper_and_keeps_rounding(self):
        sheet = default_spreadsheet()
        customers = sheet.worksheet("customers_enriched")
        customers.values[0].extend(["address_google", "address_number_google", "postal_code_google"])
        client = app_module.app.test_client()
        with client.session_transaction() as session:
            session["user"] = app_module.public_user(sheet.worksheet(app_module.USERS_SHEET).dict_rows()[0])
        with patch.object(app_module, "get_spreadsheet_with_retry", return_value=sheet), patch.object(
            app_module, "geocode_address", return_value=Coordinate(56.123456789, 12.987654321),
        ) as geocode:
            response = client.patch("/customers/2/contact", json={
                "address_google": "Kundvägen", "address_number_google": "1",
                "postal_code_google": "12345", "city_google": "Kundorten",
            })
        self.assertEqual(response.status_code, 200, response.get_json())
        geocode.assert_called_once_with("Kundvägen 1, 12345 Kundorten, Sweden")
        row = customers.dict_rows()[0]
        self.assertEqual(row["latitude_google"], 56.1234568)
        self.assertEqual(row["longitude_google"], 12.9876543)
