"""P0-3 authority-layer contracts: extraction purity and worker mappings.

Covers three things: (1) every name moved into authority.py is re-exported
from protocol as the same object so existing references keep working;
(2) the per-worker response mappings are exact and never produce a
persistent/policy-amendment variant — persistent authorization is disabled
policy, always downgraded to a per-request deny with an alert; (3) Authority
exposes the PermissionBroker surface (audit/verify_model) by delegation.
"""
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from zcode_supervisor import authority, protocol


class ReexportIdentityTests(unittest.TestCase):
    MOVED = [
        'Authority', 'inventory', 'goal_state_from_snapshot',
        'workflow_jobs_from_snapshot', 'runtime_owned', 'own_event',
        'authorized_event', 'orchestration_tool_surface', 'observed_model',
        'check_model_snapshot', 'model_execution_evidence',
        '_workflow_input_equivalent', '_edit_input_equivalent',
        'execution_evidence',
    ]

    def test_every_moved_name_is_reexported_as_the_same_object(self):
        for name in self.MOVED:
            with self.subTest(name=name):
                self.assertIs(getattr(protocol, name), getattr(authority, name))

    def test_moved_names_are_defined_in_the_authority_module(self):
        for name in self.MOVED:
            with self.subTest(name=name):
                self.assertIn(name, vars(authority))

    def test_protocol_keeps_the_symbols_and_patch_targets_it_owns(self):
        for name in ('hashlib', 'asyncio', 'process_alive', 'send_notification',
                     'ProtocolClient', 'execute', 'probe', 'validate_packet',
                     'checked_locations', 'redact_transport', 'normalized_absolute',
                     'normalized_relative', 'short_session', 'outcome_label'):
            with self.subTest(name=name):
                self.assertTrue(hasattr(protocol, name))


class WorkerResponseMappingTests(unittest.TestCase):
    def test_zcode_passthrough_allow_and_deny(self):
        self.assertEqual(authority.worker_response(
            'zcode', {'decision': 'allow', 'reason': 'ok'}),
            {'decision': 'allow', 'reason': 'ok'})
        self.assertEqual(authority.worker_response(
            'zcode', {'decision': 'deny', 'reason': 'nope'}),
            {'decision': 'deny', 'reason': 'nope'})

    def test_codex_v1_allow_and_deny_shapes(self):
        self.assertEqual(authority.worker_response(
            'codex-v1', {'decision': 'allow', 'reason': 'ok'}),
            {'decision': 'approved'})
        self.assertEqual(authority.worker_response(
            'codex-v1', {'decision': 'deny', 'reason': 'nope'}),
            {'decision': {'denied': {'rejection': 'nope'}}})

    def test_codex_v2_allow_and_deny_shapes(self):
        self.assertEqual(authority.worker_response(
            'codex-v2', {'decision': 'allow', 'reason': 'ok'}),
            {'decision': 'accept'})
        self.assertEqual(authority.worker_response(
            'codex-v2', {'decision': 'deny', 'reason': 'nope'}),
            {'decision': 'decline'})

    def test_to_zcode_response_fails_closed_on_malformed_decision(self):
        self.assertEqual(authority.to_zcode_response({}),
                         {'decision': 'deny', 'reason': None})

    def test_unknown_worker_is_rejected(self):
        for worker in ('codex', 'claude', '', None):
            with self.subTest(worker=worker):
                with self.assertRaises(ValueError):
                    authority.worker_response(worker, {'decision': 'allow', 'reason': 'x'})


class PersistentAuthorizationRefusalTests(unittest.TestCase):
    WORKERS = ('zcode', 'codex-v1', 'codex-v2')

    def test_persistent_variant_table_is_exactly_the_disabled_policy(self):
        self.assertEqual(authority.PERSISTENT_VARIANTS, frozenset({
            'approved_for_session', 'approved_mcp_policy_amendment',
            'approved_execpolicy_amendment', 'network_policy_amendment',
            'acceptForSession', 'acceptWithExecpolicyAmendment',
            'applyNetworkPolicyAmendment', 'escalate', 'modify',
            'permissionUpdates', 'addRules'}))

    def deny_shaped(self, worker, response):
        if worker == 'zcode':
            return response['decision'] == 'deny'
        if worker == 'codex-v1':
            return (isinstance(response['decision'], dict)
                    and 'denied' in response['decision'])
        return response['decision'] == 'decline'

    def test_every_persistent_variant_is_denied_with_alert_for_every_worker(self):
        for variant in sorted(authority.PERSISTENT_VARIANTS):
            for worker in self.WORKERS:
                with self.subTest(worker=worker, variant=variant):
                    response = authority.worker_response(
                        worker, {'decision': 'allow', 'reason': 'x',
                                 'requested_variant': variant})
                    self.assertTrue(self.deny_shaped(worker, response), response)
                    self.assertEqual(
                        response['alert'],
                        'persistent authorization refused: ' + variant)
                    # The variant name may appear ONLY inside the refusal
                    # alert: never granted, echoed, or negotiated elsewhere.
                    granted = {k: v for k, v in response.items() if k != 'alert'}
                    self.assertNotIn(variant, json.dumps(granted))

    def test_non_persistent_requested_variant_passes_through(self):
        decision = {'decision': 'allow', 'reason': 'ok', 'requested_variant': 'once'}
        self.assertEqual(authority.reject_persistent_request('once', decision), decision)
        self.assertEqual(authority.worker_response('zcode', decision),
                         {'decision': 'allow', 'reason': 'ok'})

    def test_absent_requested_variant_never_invents_a_refusal(self):
        self.assertEqual(authority.worker_response(
            'codex-v2', {'decision': 'deny', 'reason': 'x'}), {'decision': 'decline'})
        self.assertNotIn('alert', authority.worker_response(
            'codex-v1', {'decision': 'deny', 'reason': 'x'}))


class AuthorityBrokerSurfaceTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.workspace = self.root / 'workspace'
        self.workspace.mkdir()
        (self.workspace / 'frozen.txt').write_text('frozen input\n')
        self.packet_path = self.root / 'packet.json'
        self.packet = {'workspace': str(self.workspace),
                       'allowed_files': ['result.txt'],
                       'commands': [], 'trusted_hashes': {}}
        self.packet_path.write_text(json.dumps(self.packet))
        self.authority = authority.Authority(
            self.packet, self.packet_path, authority.inventory(self.workspace))
        self.authority.session_id = 'owned-session'
        self.authority.turn_id = 'owned-turn'

    def test_audit_delegates_to_execution_evidence(self):
        seen = []

        def fake_evidence(events, broker):
            seen.append((events, broker))
            return (['receipt'], ['problem'])

        with patch.object(authority, 'execution_evidence', fake_evidence):
            self.assertEqual(self.authority.audit(['event']),
                             (['receipt'], ['problem']))
        self.assertEqual(seen, [(['event'], self.authority)])

    def test_audit_matches_execution_evidence_on_real_events(self):
        self.assertEqual(self.authority.audit([]), ([], []))
        event = {'type': 'tool.updated', 'sessionId': 'owned-session',
                 'turnId': 'owned-turn',
                 'payload': {'kind': 'scheduled', 'toolCallId': 'call-1',
                             'toolName': 'Bash', 'input': {}}}
        self.assertEqual(self.authority.audit([event]),
                         authority.execution_evidence([event], self.authority))

    def test_verify_model_delegates_to_check_model_snapshot(self):
        seen = []

        def fake_check(snapshot, requested):
            seen.append((snapshot, requested))
            return requested

        requested = {'providerId': 'p', 'modelId': 'm'}
        snapshot = {'settings': {'model': {'current': dict(requested)}}}
        with patch.object(authority, 'check_model_snapshot', fake_check):
            self.assertEqual(self.authority.verify_model(snapshot, requested),
                             requested)
        self.assertEqual(seen, [(snapshot, requested)])

    def test_verify_model_keeps_check_model_snapshot_semantics(self):
        requested = {'providerId': 'p', 'modelId': 'm'}
        snapshot = {'settings': {'model': {'current': dict(requested)}}}
        self.assertEqual(self.authority.verify_model(snapshot, requested), requested)
        with self.assertRaises(ValueError):
            self.authority.verify_model({}, requested)

    def test_authority_satisfies_the_permission_broker_protocol(self):
        self.assertIsInstance(self.authority, authority.PermissionBroker)


if __name__ == '__main__':
    unittest.main()
