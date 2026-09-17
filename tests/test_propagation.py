"""Deterministic planning and workflow recovery tests; no live database writes."""

from collections import Counter
from contextlib import ExitStack
from copy import deepcopy
from dataclasses import replace
import json
from pathlib import Path
import random
import tempfile
import unittest
from unittest.mock import Mock, patch

from hamilton_workflows import propagation as calc, propagation_files as files, propagation_store as store
from hamilton_workflows.config import Settings
from test_champions import load_workflow


workflow = load_workflow('ContinueOnGoingExperiment/ContinueOngoingExperiment_platechain.py')


def snapshot(size=84, fraction=0.25):
    wells = [f'{row}{column}' for column in range(1, 13) for row in ('ABCDEFG' if size == 84 else 'ABCDEFGH')][:size]
    return {
        'context': {'experiment_id': 1, 'root_plate_id': 1, 'source_plate_id': 1, 'source_barcode': 'source',
                    'run_id': 'run', 'run_timestamp': '2026-09-15T10:00:00'},
        'parameters': dict(zip(calc.PARAMETER_NAMES, (700.0, 0.03, fraction, 150.0))),
        'sources': [{'culture_id': calc.culture_id(1, w), 'well': w, 'iteration': 1,
                     'timestamp': '2026-09-14T10:00:00', 'raw_gfp': 100.0, 'raw_rfp': 200.0} for w in wells],
        'destinations': [{'plate_id': 2, 'well': w, 'barcode': 'child', 'cytomat_positions': [4]} for w in wells],
    }


def plan_from(data):
    return calc.calculate_plan(data, 'source', 'spill', 'layout')


class PlanningTests(unittest.TestCase):
    def test_exact_splits_and_round_order_for_both_populations(self):
        for size in (84, 96):
            for fraction in (1, 0.5, 0.25):
                with self.subTest(size=size, fraction=fraction):
                    data = snapshot(size, fraction)
                    random.Random(9).shuffle(data['sources'])
                    random.Random(12).shuffle(data['destinations'])
                    plan = plan_from(data)
                    rows = plan['transfers']
                    count = int(size * fraction)
                    parents = sorted(snapshot(size, fraction)['sources'], key=lambda p: calc.well_key(p['well']))[:count]
                    self.assertEqual([r['source_well'] for r in rows], [p['well'] for p in parents] * int(1/fraction))
                    self.assertEqual([r['destination_well'] for r in rows], [d['well'] for d in snapshot(size)['destinations']])
                    self.assertEqual(set(Counter(r['parent_culture_id'] for r in rows).values()), {int(1/fraction)})
                    self.assertEqual(len({r['child_culture_id'] for r in rows}), size)
                    self.assertEqual(sum(p['mature'] for p in plan['parent_history']), count)
                    self.assertTrue(all(r['od'] > 0 for r in rows))
                    self.assertTrue(all(r['culture_volume'] <= 700 / 3 * fraction for r in rows))

    def test_selection_uses_od_then_numeric_well_order(self):
        data = snapshot(96, 0.25)
        for source in data['sources']:
            if source['well'] in ('H12', 'A10'):
                source['raw_gfp'] = 10000
        plan = plan_from(data)
        selected = {p['well'] for p in plan['parent_history'] if p['mature']}
        self.assertIn('H12', selected)
        self.assertIn('A10', selected)
        self.assertEqual(sorted(['A10', 'B1', 'A2', 'A1'], key=calc.well_key), ['A1', 'B1', 'A2', 'A10'])

    def test_restricted_and_occupied_wells_are_not_reintroduced(self):
        data = snapshot(4, 0.5)
        data['destinations'] = [dict(d, well=w) for d, w in zip(data['destinations'], ('C2', 'G3', 'A10', 'H12'))]
        self.assertEqual([r['destination_well'] for r in plan_from(data)['transfers']], ['C2', 'G3', 'A10', 'H12'])

    def test_invalid_inputs_fail(self):
        cases = []
        def case(label, mutate):
            data = snapshot()
            mutate(data)
            cases.append((label, data))
        for fraction in (0.75, 0.125, 0, -1, float('nan'), float('inf')):
            case(str(fraction), lambda d, f=fraction: d['parameters'].update(TopFractionToPropagate=f))
        case('uneven', lambda d: d['sources'].pop())
        case('capacity', lambda d: d['destinations'].pop())
        case('multiple plates', lambda d: d['destinations'][-1].update(plate_id=3))
        case('no position', lambda d: [r.update(cytomat_positions=[]) for r in d['destinations']])
        case('duplicate positions', lambda d: [r.update(cytomat_positions=[4, 5]) for r in d['destinations']])
        case('duplicate child', lambda d: d['destinations'].append(d['destinations'][0]))
        case('duplicate parent', lambda d: d['sources'].__setitem__(1, d['sources'][0]))
        case('bad well', lambda d: d['sources'][0].update(well='I1'))
        case('missing reading', lambda d: d['sources'][0].update(raw_gfp=None))
        case('nonfinite reading', lambda d: d['sources'][0].update(raw_rfp=float('inf')))
        case('target zero', lambda d: d['parameters'].update(TargetWellVolume=0))
        case('sample target', lambda d: d['parameters'].update(V_OD_Sample=700))
        case('withdrawal exceeds remaining', lambda d: d['parameters'].update(V_OD_Sample=699))
        case('unsafe run name', lambda d: d['context'].update(run_id='../run'))
        for label, data in cases:
            with self.subTest(label=label), self.assertRaises(ValueError):
                plan_from(data)

    def test_baseline_and_volume_formula(self):
        self.assertEqual(calc.corrected_readings(-1, 0), calc.corrected_readings(10, 10))
        data = snapshot(4, 0.5)
        data['sources'][0].update(raw_gfp=10000, raw_rfp=20000)
        plan = plan_from(data)
        for parent in plan['parent_history']:
            self.assertAlmostEqual(parent['dilution_factor'], 700/550)
            self.assertAlmostEqual(parent['od'], parent['od_gfp'] + parent['od_rfp'])
        for row in plan['transfers']:
            parent = next(p for p in plan['parent_history'] if p['culture_id'] == row['parent_culture_id'])
            volume = min(0.03*700/parent['od'], 700/3/2)
            self.assertAlmostEqual(row['culture_volume'], volume)
            self.assertAlmostEqual(row['media_volume'], 700-volume)
            self.assertAlmostEqual(row['od'], parent['od']*volume/700)

    def test_serialization_rejects_tampering_and_unknown_version(self):
        plan = plan_from(snapshot())
        self.assertEqual(calc.deserialize_plan(calc.serialize_plan(plan)), plan)
        damaged = deepcopy(plan)
        damaged['transfers'][0]['source_well'] = 'H12'
        with self.assertRaisesRegex(ValueError, 'snapshot'):
            calc.deserialize_plan(json.dumps(damaged))
        damaged['version'] = 99
        with self.assertRaisesRegex(ValueError, 'version'):
            calc.deserialize_plan(json.dumps(damaged))


class WorkflowTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.settings = replace(Settings(), task_dir=self.root / 'tasks', layout_path='layout')
        self.data = snapshot(4, 0.5)
        self.plan = plan_from(self.data)
        self.conn = Mock()
        self.log = Mock()
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.stack.enter_context(patch.object(workflow.database, 'connect', return_value=self.conn))
        self.context = self.stack.enter_context(patch.object(store, 'load_context', return_value=self.data['context']))
        self.locks = self.stack.enter_context(patch.object(store, 'acquire_locks'))
        self.load = self.stack.enter_context(patch.object(store, 'load_snapshot', return_value=self.data))
        self.persist = self.stack.enter_context(patch.object(store, 'persist_plan'))

    def run_workflow(self, **kwargs):
        return workflow.propagate('source', 'source', 'spill', self.settings, self.log, run_id='run', **kwargs)

    def test_export_alignment_and_transaction_order(self):
        def commit():
            self.assertEqual(list(self.settings.task_dir.glob('run_*.txt')), [])
            self.assertEqual(len(list(self.settings.task_dir.glob('.champions-stage-*/*.txt'))), 5)
            self.persist.assert_called_once()
        self.conn.commit.side_effect = commit
        result = self.run_workflow()
        self.assertEqual(result, self.plan)
        self.conn.commit.assert_called_once()
        self.assertEqual(len(list(self.settings.task_dir.iterdir())), 5)
        for suffix, key in [('CytomatPos', 'cytomat_position'), ('CultureVol', 'culture_volume'), ('MediaVol', 'media_volume')]:
            path = self.settings.task_dir / f'run_{suffix}.txt'
            self.assertEqual(path.read_text().splitlines(), [str(r[key]) for r in result['transfers']])
            self.assertFalse(path.read_bytes().endswith(b'\n'))
        for suffix, key in [('SpatialOverPlate_Positions', 'source_well'), ('SpillOverPlate_Positions', 'destination_well')]:
            path = self.settings.task_dir / f'run_{suffix}.txt'
            self.assertNotIn(b'\r', path.read_bytes())
            self.assertEqual([line.split(',')[-1] for line in path.read_text().splitlines()[1:]],
                             [r[key] for r in result['transfers']])

    def test_dry_run_has_no_commit_or_task_files(self):
        with patch('builtins.print') as output:
            self.run_workflow(dry_run=True)
        self.assertEqual(json.loads(output.call_args.args[0]), self.plan)
        self.persist.assert_not_called()
        self.conn.commit.assert_not_called()
        self.assertFalse(self.settings.task_dir.exists())

    def test_failed_persistence_rolls_back_and_removes_staging(self):
        self.persist.side_effect = RuntimeError('write failed')
        with self.assertRaisesRegex(RuntimeError, 'write failed'):
            self.run_workflow()
        self.conn.commit.assert_not_called()
        self.assertGreater(self.conn.rollback.call_count, 0)
        self.assertEqual(list(self.settings.task_dir.iterdir()), [])

    def test_staging_failure_precedes_persistence(self):
        with patch.object(files, 'write_propagation_files', side_effect=OSError('disk full')):
            with self.assertRaisesRegex(OSError, 'disk full'):
                self.run_workflow()
        self.persist.assert_not_called()
        self.conn.commit.assert_not_called()
        self.assertEqual(list(self.settings.task_dir.iterdir()), [])

    def test_partial_publication_requires_manual_reconciliation(self):
        original = files.os.replace
        calls = []
        def failing_replace(source, target):
            calls.append(target)
            if len(calls) == 3:
                raise PermissionError('file locked')
            original(source, target)
        with patch.object(files.os, 'replace', side_effect=failing_replace):
            with self.assertRaisesRegex(RuntimeError, 'Manual reconciliation'):
                self.run_workflow()
        self.conn.commit.assert_called_once()
        self.assertEqual(len(list(self.settings.task_dir.iterdir())), 2)
        self.load.side_effect = ValueError('Source already processed; automatic replay is disabled')
        self.persist.reset_mock()
        with self.assertRaisesRegex(ValueError, 'already processed'):
            self.run_workflow()
        self.persist.assert_not_called()
        self.assertEqual(len(list(self.settings.task_dir.iterdir())), 2)

    def test_recheck_conflict_and_commit_failure_do_not_publish(self):
        self.conn.commit.side_effect = RuntimeError('commit failed')
        with self.assertRaisesRegex(RuntimeError, 'commit failed'):
            self.run_workflow()
        self.assertEqual(list(self.settings.task_dir.iterdir()), [])
        self.conn.commit.side_effect = None
        self.persist.side_effect = ValueError('occupancy changed')
        with self.assertRaisesRegex(ValueError, 'occupancy changed'):
            self.run_workflow()
        self.assertEqual(list(self.settings.task_dir.iterdir()), [])

    def test_processed_source_stops_before_export(self):
        self.load.side_effect = ValueError('Source already processed')
        with self.assertRaisesRegex(ValueError, 'already processed'):
            self.run_workflow()
        self.persist.assert_not_called()
        self.assertFalse(self.settings.task_dir.exists())


class StoreValidationTests(unittest.TestCase):
    def test_readings_require_exactly_one_value_per_eligible_well(self):
        data = snapshot(4)
        rows = [{'well': p['well'], 'reading': 100} for p in data['sources']]
        for bad_rows in (rows[:-1], rows + [rows[0]]):
            with patch.object(store, 'query', return_value=bad_rows), self.assertRaisesRegex(ValueError, 'exactly one'):
                store.load_readings(Mock(), data['context'], data['sources'])

    def test_lock_conflict_stops(self):
        cursor = Mock()
        cursor.fetchone.return_value = (-1,)
        with self.assertRaisesRegex(RuntimeError, 'lock'):
            store.acquire_locks(cursor, snapshot()['context'])

    def test_snapshot_change_precedes_inserts(self):
        plan = plan_from(snapshot())
        cursor = Mock()
        with patch.object(store, 'load_context', return_value=plan['snapshot']['context']), \
             patch.object(store, 'load_snapshot', return_value={}):
            with self.assertRaisesRegex(ValueError, 'changed'):
                store.persist_plan(cursor, plan)
        cursor.executemany.assert_not_called()

    def test_modified_transfer_is_rejected_before_queries_or_writes(self):
        plan = plan_from(snapshot())
        plan['transfers'][0]['source_well'] = 'H12'
        cursor = Mock()
        with self.assertRaisesRegex(ValueError, 'snapshot'):
            store.persist_plan(cursor, plan)
        cursor.execute.assert_not_called()
        cursor.executemany.assert_not_called()


if __name__ == '__main__':
    unittest.main()
