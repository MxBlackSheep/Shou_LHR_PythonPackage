"""Registration contracts: real Excel parsing and scripted SQL, without a window."""

import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd
import pyodbc

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
        return [('SELECT ExperimentID FROM Experiments WITH (UPDLOCK, HOLDLOCK)', [[]]),
                ('OUTPUT INSERTED.ExperimentID', [[(5,)]]),
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
                    steps.append(('EXEC dbo.ScheduleExperimentForImmediateExecution',
                                  [pyodbc.ProgrammingError('No results'), [('Scheduled',)]]))
                steps.append(('SELECT @@TRANCOUNT', [[(1,)]]))
                cursor, conn = self.connect_script(steps)
                with patch.object(database, 'latest_run_id', side_effect=AssertionError('Run lookup')):
                    self.assertEqual(self.create(schedule), 5)
                self.assertFalse(cursor.steps)
                self.assertEqual(cursor.calls[0][1], (('user',),))
                self.assertEqual(cursor.calls[1][1], (('user', 'note', schedule),))
                self.assertEqual(cursor.calls[4][1], ([(5, 'A1', 'MediaCtrl'), (5, 'B1', 'Cells')],))
                if schedule == '1':
                    self.assertEqual(cursor.calls[-2][1], (('user',),))
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
            steps = self.steps()
            steps.append(('EXEC dbo.ScheduleExperimentForImmediateExecution', []))
            cursor, conn = self.connect_script(steps)
            original_execute = cursor.execute
            def fail(sql, *params):
                if len(cursor.calls) + 1 == failing_call:
                    raise RuntimeError('SQL failure')
                return original_execute(sql, *params)
            with patch.object(cursor, 'execute', side_effect=fail):
                with self.assertRaisesRegex(RuntimeError, 'SQL failure'):
                    self.create('1')
            conn.commit.assert_not_called()
            conn.rollback.assert_called_once()
            conn.close.assert_called_once()

    def test_scheduling_error_result_rolls_back_without_committing(self):
        steps = self.steps() + [('EXEC dbo.ScheduleExperimentForImmediateExecution',
                                [pyodbc.ProgrammingError('No results'),
                                 [('DATABASE ERROR: scheduling failed',)]])]
        cursor, conn = self.connect_script(steps)
        with self.assertRaisesRegex(RuntimeError, 'DATABASE ERROR: scheduling failed'):
            self.create('1')
        self.assertFalse(cursor.steps)
        conn.commit.assert_not_called()
        conn.rollback.assert_called_once()

    def test_missing_transaction_prevents_commit_for_both_scheduling_choices(self):
        for schedule in ('0', '1'):
            with self.subTest(schedule=schedule):
                steps = self.steps()
                if schedule == '1':
                    steps.append(('EXEC dbo.ScheduleExperimentForImmediateExecution', [[]]))
                steps.append(('SELECT @@TRANCOUNT', [[(0,)]]))
                cursor, conn = self.connect_script(steps)
                with self.assertRaisesRegex(RuntimeError, 'transaction'):
                    self.create(schedule)
                self.assertFalse(cursor.steps)
                conn.commit.assert_not_called()
                conn.rollback.assert_called_once()

    def test_duplicate_id_is_refused_before_any_insert(self):
        cursor, conn = self.connect_script([
            ('SELECT ExperimentID FROM Experiments WITH (UPDLOCK, HOLDLOCK)', [[(99,)]])])
        with self.assertRaisesRegex(registration.RegistrationError,
                                    'An experiment with this ID already exists.'):
            self.create('0')
        self.assertFalse(cursor.steps)
        conn.commit.assert_not_called()
        conn.rollback.assert_called_once()

    def test_form_displays_plain_errors_before_exiting(self):
        form = registration.InputForm.__new__(registration.InputForm)
        for name, value in [('user_id', 'user'), ('note', ''), ('schedule', '1'),
                            ('excel_path', str(self.excel_file))]:
            setattr(form, name, Mock(get=Mock(return_value=value)))
        form.param_entries = {'TopFractionToPropagate': Mock(get=Mock(return_value='1'))}
        form.settings, form.log, form.root = Settings(), Mock(), Mock()
        errors = [
            (registration.RegistrationError('An experiment with this ID already exists.'),
             'An experiment with this ID already exists.'),
            (RuntimeError('DATABASE ERROR: dbo.internal SQL details'),
             'The experiment was not saved. The database operation failed.'),
            (ValueError('internal parser details'),
             'The experiment was not saved. Check the experiment details and Excel layout.'),
        ]
        for error, message in errors:
            with self.subTest(error=error), \
                    patch.object(registration, 'create_experiment', side_effect=error), \
                    patch.object(registration.messagebox, 'showerror') as showerror, \
                    patch.object(registration.messagebox, 'showinfo') as showinfo:
                with self.assertRaises(SystemExit) as exited:
                    form.validate_and_submit()
                self.assertEqual(exited.exception.code, 1)
                showerror.assert_called_once_with('Error', message)
                form.log.assert_called_with(f'Validation or DB error: {error}')
                showinfo.assert_not_called()


if __name__ == '__main__':
    unittest.main()
