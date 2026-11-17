"""Registration contracts: real Excel parsing and scripted SQL, without a window."""

import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd

from hamilton_workflows import database
from hamilton_workflows.config import Settings
from test_champions import ROOT, ScriptedCursor, load_workflow


with patch.object(sys, 'path', [str(ROOT / 'VENUS Python Modules/Champions_FL/BeginNewExperiment'), *sys.path]):
    registration = load_workflow('BeginNewExperiment/RegisterNewExperiment.py')


class ExperimentEntryContracts(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.excel_file = Path(self.temp.name) / 'layout.xlsx'
        pd.DataFrame({'Destination': [' A1 ', 'B1', None],
                      'Source': [' T ', 'strain', None], 'Vol': [100, 100, None]}).to_excel(
            self.excel_file, index=False, startrow=1)
        self.parameters = {'TopFractionToPropagate': 1, 'TargetWellVolume': 700}

    def connect_script(self, steps):
        cursor = ScriptedCursor(steps)
        conn = Mock()
        conn.cursor.return_value = cursor
        patcher = patch.object(database, 'connect', return_value=conn)
        patcher.start()
        self.addCleanup(patcher.stop)
        return cursor, conn

    def steps(self):
        return [('INSERT INTO Experiments', []), ('SELECT ExperimentID', [[(5,)]]),
                ('INSERT INTO ExperimentParameters', []), ('INSERT INTO ExperimentParameters', []),
                ('INSERT INTO ExperimentPlatePattern', [])]

    def create(self, schedule):
        return registration.create_experiment('user', 'note', schedule, self.parameters,
                                              self.excel_file, Settings(), Mock())

    def test_registration_persists_base_layout_and_schedules_only_when_chosen(self):
        for schedule in ('0', '1'):
            with self.subTest(schedule=schedule):
                steps = self.steps()
                if schedule == '1':
                    steps.append(('EXEC dbo.ScheduleExperimentForImmediateExecution', []))
                cursor, conn = self.connect_script(steps)
                with patch.object(database, 'latest_run_id', side_effect=AssertionError('Run lookup')):
                    self.assertEqual(self.create(schedule), 5)
                self.assertFalse(cursor.steps)
                self.assertEqual(cursor.calls[0][1], (('user', 'note', schedule),))
                self.assertEqual(cursor.calls[4][1], ([(5, 'A1', 'MediaCtrl'), (5, 'B1', 'Cells')],))
                if schedule == '1':
                    self.assertEqual(cursor.calls[-1][1], (('user',),))
                conn.commit.assert_called_once()
                conn.rollback.assert_not_called()
                conn.close.assert_called_once()
                self.assertEqual(sorted(p.name for p in self.excel_file.parent.iterdir()), ['layout.xlsx'])

    def test_validation_precedes_database_writes(self):
        cursor = Mock()
        for fraction in (None, 0.75, float('nan')):
            with self.assertRaises(ValueError):
                registration.insert_experiment(cursor, 'user', '', '0',
                    {'TopFractionToPropagate': fraction}, self.excel_file)
        pd.DataFrame({'Destination': ['A1']}).to_excel(self.excel_file, index=False, startrow=1)
        with self.assertRaisesRegex(ValueError, 'missing required columns'):
            registration.insert_experiment(cursor, 'user', '', '0', self.parameters, self.excel_file)
        cursor.execute.assert_not_called()

    def test_pattern_or_scheduling_failure_rolls_back_everything(self):
        for failing_call in (5, 6):
            conn = Mock()
            cursor = conn.cursor.return_value
            cursor.fetchone.return_value = (5,)
            # Count executemany with the other SQL writes.
            calls = Mock(side_effect=[None] * (failing_call - 1) + [RuntimeError('SQL failure')])
            cursor.execute.side_effect = calls
            cursor.executemany.side_effect = calls
            with patch.object(database, 'connect', return_value=conn):
                with self.assertRaisesRegex(RuntimeError, 'SQL failure'):
                    self.create('1')
            conn.commit.assert_not_called()
            conn.rollback.assert_called_once()
            conn.close.assert_called_once()


if __name__ == '__main__':
    unittest.main()
