"""A customer's allowed promise cannot authorize another contact scope."""
from copy import deepcopy
from unittest.mock import patch

import app as app_module
from test_planning import PlanningApiTestCase, NOW


CID = "11111111-1111-4111-8111-111111111111"
OLDER = "followup:older-promise"
NEWER = "followup:new-agreement"


class ObligationContactScopeTests(PlanningApiTestCase):
    def sources(self):
        self.append_contact_row(customer_id=CID, contact_id="older-promise", date_time="2026-07-12 09:00",
                                result="Positiv", follow_up_date="2026-07-26", follow_up_purpose="customer_followup", follow_up_agreement_confirmed="Y")
        # Negative dialogue can include one explicit new agreement. It does not
        # grant permission for every earlier promise still in the CRM.
        self.append_contact_row(customer_id=CID, contact_id="new-agreement", date_time="2026-07-20 09:00",
                                result="Negativ", follow_up_date="2026-07-25", follow_up_purpose="customer_followup", follow_up_agreement_confirmed="Y")

    def snapshot(self):
        return app_module.get_authoritative_priority_snapshot(self.spreadsheet, today=NOW.date(), as_of=NOW)

    def attention(self, suffix=""):
        response = self.client.get("/planning/attention?user_name=olle" + suffix)
        self.assertEqual(response.status_code, 200, response.get_json())
        return response.get_json()


    def test_primary_and_secondary_checks_share_features_and_do_not_mutate_snapshot(self):
        self.sources();snapshot=self.snapshot();before=deepcopy(snapshot)
        with patch.object(app_module,'build_order_features',wraps=app_module.build_order_features) as orders, patch.object(app_module,'build_contact_features',wraps=app_module.build_contact_features) as contacts:
            payload=self.attention()
            self.assertEqual(payload['total'],1)
            self.assertTrue(payload['items'][0]['can_contact_now'])
            self.assertLessEqual(orders.call_count,1)
            self.assertLessEqual(contacts.call_count,1)
        self.assertEqual(snapshot,before)

    def test_attention_only_calculates_the_requested_page(self):
        self.sources()
        with patch.object(app_module,'obligation_contact_decision',wraps=app_module.obligation_contact_decision) as decision:
            result=self.attention('&limit=1')
            self.assertEqual(result['total'],1)
            self.assertEqual(len(result['items']),1)
            self.assertEqual(decision.call_count,0)  # Recording facts needs no per-promise scorer.
