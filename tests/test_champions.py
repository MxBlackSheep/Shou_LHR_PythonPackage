"""Workflow contracts using scripted SQL responses and temporary robot paths."""

from contextlib import ExitStack
from dataclasses import replace
import importlib.util
from pathlib import Path
import os
import runpy
import tempfile
import unittest
from unittest.mock import Mock, patch

import pandas as pd
import pyodbc

from hamilton_workflows import bcp, database
from hamilton_workflows.config import Settings


ROOT = Path(__file__).resolve().parents[1]


def load_workflow(relative_path):
    """Import the actual VENUS script so tests exercise its local workflow code."""
    path = ROOT / 'VENUS Python Modules' / 'Champions_FL' / relative_path
    spec = importlib.util.spec_from_file_location(path.stem, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


setup = load_workflow('BeginNewExperiment/StartNewExperiment_1.py')
commence = load_workflow('BeginNewExperiment/StartNewExperiment_2.py')
initial_readings = load_workflow('BeginNewExperiment/StartNewExperiment_SimulationFlourscent.py')
condition_check = load_workflow('ContinueOnGoingExperiment/ContinueOnGoingExperiment_ConditionCheck.py')
expansion = load_workflow('ContinueOnGoingExperiment/ContinueOnGoingExperiment_PurgeRetirePlate.py')
propagation = load_workflow('ContinueOnGoingExperiment/ContinueOngoingExperiment_platechain.py')
simulated_readings = load_workflow('ContinueOnGoingExperiment/ContinueOnGoingExperiment_SimulationFlourscent.py')
readings = load_workflow('SendGFPAndRFPToDB.py')


class ScriptedCursor:
    """Reject unexpected SQL; each step supplies a query fragment and result sets."""
    def __init__(self, steps):
        self.steps = list(steps)
        self.calls = []

    def execute(self, sql, *params):
        self.calls.append((sql, params))
        expected, self.sets = self.steps.pop(0)
        if expected not in ' '.join(sql.split()):
            raise AssertionError(f'Expected {expected}; received {sql}')
        self.sets = list(self.sets)
        self.current = self.sets.pop(0) if self.sets else []
        return self

    def executemany(self, sql, rows):
        return self.execute(sql, rows)

    def fetchone(self):
        rows = self.fetchall()
        return rows[0] if rows else None

    def fetchall(self):
        if isinstance(self.current, Exception):
            raise self.current
        return self.current

    def nextset(self):
        if not self.sets:
            return False
        self.current = self.sets.pop(0)
        return True


class ChampionsContracts(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.settings = replace(Settings(), task_dir=self.root / 'tasks', gen5_dir=self.root / 'raw',
                                log_dir=self.root / 'logs', layout_path='layout')
        self.log = Mock()

    def connect_script(self, steps):
        cursor = ScriptedCursor(steps)
        conn = Mock()
        conn.cursor.return_value = cursor
        patcher = patch.object(database, 'connect', return_value=conn)
        patcher.start()
        self.addCleanup(patcher.stop)
        return cursor, conn

    def test_all_eight_scripts_own_their_entry_points_and_import_without_side_effects(self):
        paths = sorted((ROOT / 'VENUS Python Modules/Champions_FL').rglob('*.py'))
        self.assertEqual(len(paths), 8)
        with ExitStack() as stack:
            for target in ('pyodbc.connect', 'tkinter.Tk', 'subprocess.run', 'os.makedirs',
                           'pathlib.Path.mkdir', 'argparse.ArgumentParser.parse_args'):
                stack.enter_context(patch(target, side_effect=AssertionError(f'Import side effect: {target}')))
            for path in paths:
                namespace = runpy.run_path(str(path), run_name='import_test')
                self.assertTrue(callable(namespace['main']))
                self.assertEqual(Path(namespace['main'].__code__.co_filename), path)

    def test_cli_arguments_and_help_require_no_database(self):
        with patch.object(database, 'connect', side_effect=AssertionError('DB touched')):
            for entry in (readings.main, simulated_readings.main, propagation.main, setup.main):
                with patch('sys.stdout'), patch('sys.stderr'):
                    with self.assertRaises(SystemExit) as error:
                        entry([])
                    self.assertEqual(error.exception.code, 2)
                    with self.assertRaises(SystemExit) as help_exit:
                        entry(['--help'])
                    self.assertEqual(help_exit.exception.code, 0)

    def test_commence_ignores_barcode_arguments_and_checks_sql_error(self):
        cursor, conn = self.connect_script([
            ('SELECT TOP 1 PlateID', [[(12,)]]),
            ('EXEC SpatialEvo_CommenceExperimentFl', [pyodbc.ProgrammingError('no rows')]),
        ])
        with patch.object(commence.Settings, 'from_env', return_value=self.settings):
            self.assertEqual(commence.main(['first-barcode', 'second-barcode']), 0)
        conn.commit.assert_called_once()
        conn.close.assert_called_once()
        self.assertFalse(cursor.steps)

    def test_commence_database_error_rolls_back_and_returns_failure(self):
        _, conn = self.connect_script([
            ('SELECT TOP 1 PlateID', [[(12,)]]),
            ('EXEC SpatialEvo_CommenceExperimentFl', [[('DATABASE ERROR: failed',)]]),
        ])
        with patch.object(commence.Settings, 'from_env', return_value=self.settings):
            self.assertEqual(commence.main([]), 1)
        conn.commit.assert_not_called()
        conn.rollback.assert_called_once()
        conn.close.assert_called_once()

    def test_condition_check_filters_both_chain_sets_and_file_has_no_final_newline(self):
        cursor, conn = self.connect_script([
            ('SELECT TOP 1 RunGUID', [[('run',)]]),
            ('EXEC dbo.Evo_RetrievePlateChain', [[('parent', 1)], [('empty', 2), ('child', 3)]]),
            ('EXEC dbo.Competition_SelectCultures', [[(1,)]]),
            ('EXEC dbo.Competition_SelectCultures', [[]]),
            ('EXEC dbo.Competition_SelectCultures', [[(2,)]]),
        ])
        condition_check.check_conditions(self.settings, self.log)
        self.assertEqual((self.settings.task_dir / 'run_PlateChainChecked.txt').read_bytes(), b'parent,1\r\nchild,3')
        self.assertFalse(cursor.steps)
        conn.commit.assert_not_called()
        conn.close.assert_called_once()

    def test_empty_chain_fails_without_writing_outputs(self):
        _, conn = self.connect_script([
            ('SELECT TOP 1 RunGUID', [[('run',)]]), ('EXEC dbo.Evo_RetrievePlateChain', [[], []]),
        ])
        with self.assertRaisesRegex(RuntimeError, 'No plates'):
            condition_check.check_conditions(self.settings, self.log)
        self.assertFalse(self.settings.task_dir.exists())
        conn.close.assert_called_once()

    def test_setup_rejects_unsupported_fraction_before_connecting(self):
        for fraction in (None, 0.75, 0.125, float('nan')):
            with patch.object(database, 'connect') as connect:
                with self.assertRaises(ValueError):
                    setup.create_experiment('user', '', '1', {'TopFractionToPropagate': fraction},
                                            'parent', 'expansion', 'file', self.settings, self.log)
                connect.assert_not_called()

    def test_upload_real_and_test_mode_payloads(self):
        for test_mode in (False, True):
            with self.subTest(test_mode=test_mode):
                _, conn = self.connect_script([
                    ('SELECT TOP 1 RunGUID', [[('run',)]]), ('SELECT PlateID FROM Plates', [[(9,)]]),
                ])
                self.settings.gen5_dir.mkdir(exist_ok=True)
                for channel in ('FlEx482Em510', 'FlEx587Em611'):
                    (self.settings.gen5_dir / f'{channel}_data').write_text('header\nheader\nB1\t23.\n')
                with patch.object(bcp, 'run_bcp') as upload:
                    readings.upload_readings('barcode', self.settings, self.log, test_mode=test_mode)
                self.assertEqual([call.args[0] for call in upload.call_args_list], ['ImportFlEx482Em510', 'ImportFlEx587Em611'])
                data = (self.settings.task_dir / 'run_FlEx482Em510.txt').read_bytes()
                if test_mode:
                    self.assertEqual(len(data.splitlines()), 96)
                    self.assertTrue((self.settings.gen5_dir / 'FlEx482Em510_data_test').exists())
                else:
                    self.assertEqual(data, b'9\tB1\t23.0\trun\r\n')
                self.assertEqual((self.settings.gen5_dir / 'FlEx482Em510_data').read_text(), 'header\nheader\nB1\t23.\n')
                conn.close.assert_called_once()

    def test_initial_simulation_filters_wells_before_both_inserts(self):
        cursor, conn = self.connect_script([
            ('SELECT TOP 1 RunGUID', [[('run',)]]), ('SELECT TOP 1 PlateID', [[(9,)]]),
            ('INSERT INTO ImportFlEx482Em510', []), ('INSERT INTO ImportFlEx587Em611', []),
        ])
        with patch.object(pd, 'read_sql', return_value=pd.DataFrame({'WellID': ['B1', 'H12']})):
            initial_readings.seed_initial_readings(self.settings, self.log)
        for _, params in cursor.calls[-2:]:
            self.assertEqual([row[1] for row in params[0]], ['B1', 'H12'])
            self.assertEqual([row[0] for row in params[0]], [9, 9])
        conn.commit.assert_called_once()

    def test_ongoing_simulation_generates_both_channels_without_gen5_inputs(self):
        cursor, conn = self.connect_script([
            ('SELECT TOP 1 RunGUID', [[('run',)]]), ('SELECT PlateID FROM Plates', [[(9,)]]),
        ])
        with patch.object(bcp, 'run_bcp') as upload:
            simulated_readings.upload_simulated_readings('barcode', self.settings, self.log)
        self.assertFalse(self.settings.gen5_dir.exists())
        self.assertFalse(cursor.steps)
        self.assertEqual([call.args[0] for call in upload.call_args_list],
                         ['ImportFlEx482Em510', 'ImportFlEx587Em611'])
        for channel in ('FlEx482Em510', 'FlEx587Em611'):
            frame = pd.read_csv(self.settings.task_dir / f'run_{channel}.txt', sep='\t', header=None)
            self.assertEqual(frame.shape, (96, 4))
            self.assertEqual(frame.iloc[0, 1], 'A1')
            self.assertEqual(frame.iloc[-1, 1], 'H12')
            self.assertTrue(frame[2].between(0, 550).all())
            self.assertEqual(frame[0].unique().tolist(), [9])
            self.assertEqual(frame[3].unique().tolist(), ['run'])
        conn.close.assert_called_once()

    def test_setup_keeps_parameters_barcodes_assignment_and_output_contract(self):
        cursor, conn = self.connect_script([
            ('INSERT INTO Experiments', []), ('SELECT ExperimentID', [[(5,)]]),
            ('INSERT INTO ExperimentParameters', []), ('INSERT INTO ExperimentParameters', []),
            ('EXEC SpatialEvo_NewExperiment', [[(12, 3)]]),
            ('SELECT dbo.QueryCytomatPosition', [[(4,)]]), ('SELECT TOP 1 RunGUID', [[('run',)]]),
            ('INSERT INTO ImportPlatePattern', []),
        ])
        previous = Path.cwd()
        try:
            os.chdir(self.root)
            with patch.object(pd, 'read_excel', return_value=pd.DataFrame({'Destination': ['A1'], 'Source': ['T'], 'Vol': [1]})):
                result = setup.create_experiment('user', 'note', '1', {'TargetWellVolume': 700, 'TopFractionToPropagate': 1},
                    'parent', 'expansion', 'file', self.settings, self.log)
        finally:
            os.chdir(previous)
        self.assertEqual(result, (12, 3))
        self.assertEqual(cursor.calls[4][1], ('user', 'parent', 'expansion'))
        self.assertEqual(cursor.calls[5][1], (('expansion',),))
        self.assertEqual(cursor.calls[-1][1], ([(12, 'A1', 'run', 'MediaCtrl')],))
        for suffix, contents in [('PlateID', b'12'), ('CytomatPos', b'3'), ('ExpansionCytomatPos', b'4')]:
            self.assertEqual((self.settings.task_dir / f'run_{suffix}.txt').read_bytes(), contents)
        self.assertTrue((self.root / 'output.txt').exists())
        conn.commit.assert_called_once()
        conn.close.assert_called_once()

    def test_setup_sql_error_rolls_back_without_success_outputs(self):
        _, conn = self.connect_script([
            ('INSERT INTO Experiments', []), ('SELECT ExperimentID', [[(5,)]]),
            ('INSERT INTO ExperimentParameters', []),
            ('EXEC SpatialEvo_NewExperiment', [[('DATABASE ERROR: conflict',)]]),
        ])
        with self.assertRaisesRegex(RuntimeError, 'DATABASE ERROR'):
            setup.create_experiment('user', '', '1', {'TopFractionToPropagate': 1}, 'parent', 'expansion', 'file', self.settings, self.log)
        conn.commit.assert_not_called()
        conn.rollback.assert_called_once()
        conn.close.assert_called_once()
        self.assertFalse(self.settings.task_dir.exists())

    def test_expansion_result_format_and_fallback_barcode(self):
        for result, expected in ([('retired', None, 4)], b'retired,,4\r\n'), ([], b'BC1234'):
            cursor, conn = self.connect_script([
                ('SELECT TOP 1 RunGUID', [[('run',)]]),
                ('SELECT AncestPlatesInExperiments.PlateID', [[(1,)]]),
                ('SELECT DISTINCT Plates.PlateID', [[(2,)]]),
                ('SELECT BarCode FROM Plates WHERE PlateID IN', [[('parent',), ('child',)]]),
                ('EXEC dbo.AddExpansionPlateToActiveExperiment', [result]),
            ])
            with patch.object(expansion, 'generate_unique_barcode', return_value='BC1234'):
                expansion.add_expansion_plate(self.settings, self.log)
            self.assertEqual((self.settings.task_dir / 'run_AddPlate.txt').read_bytes(), expected)
            self.assertFalse(cursor.steps)
            conn.commit.assert_called_once()


if __name__ == '__main__':
    unittest.main()
