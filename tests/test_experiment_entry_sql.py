"""Opt-in registration check; Windows sign-in, EvoYeast_check only, always rollback.

Run with HAMILTON_EXPERIMENT_ENTRY_SQL_TESTS=1. No installation database is read.
"""

import os
from pathlib import Path
import tempfile
import unittest
import uuid

import pandas as pd
import pyodbc

from test_experiment_entry import registration


def connect_check_database(database_name):
    if database_name != 'EvoYeast_check':
        raise ValueError('Registration check requires EvoYeast_check')
    return pyodbc.connect(
        'DRIVER={ODBC Driver 11 for SQL Server};SERVER=.\\HAMILTON;'
        f'DATABASE={database_name};Trusted_Connection=yes;',
        autocommit=False, timeout=10,
    )


@unittest.skipUnless(os.environ.get('HAMILTON_EXPERIMENT_ENTRY_SQL_TESTS') == '1',
                     'opt-in EvoYeast_check rollback check')
class ExperimentEntrySqlCheck(unittest.TestCase):
    def test_registration_and_scheduling_roll_back(self):
        conn = connect_check_database('EvoYeast_check')
        try:
            cursor = conn.cursor()
            self.assertEqual(cursor.execute('SELECT DB_NAME()').fetchone()[0], 'EvoYeast_check')
            original_flags = list(cursor.execute(
                'SELECT ExperimentID, ScheduledToRun FROM dbo.Experiments ORDER BY ExperimentID'))
            ids = []
            with tempfile.TemporaryDirectory() as directory:
                excel_file = Path(directory) / 'layout.xlsx'
                pd.DataFrame({'Destination': ['A1', 'B1'], 'Source': ['T', 'strain'],
                              'Vol': [100, 100]}).to_excel(excel_file, index=False, startrow=1)
                parameters = {'TopFractionToPropagate': 1, 'TargetWellVolume': 700}
                for index, schedule in enumerate(('0', '0', '1')):
                    user_id = 'entry_' + uuid.uuid4().hex[:16]
                    experiment_id = registration.insert_experiment(
                        cursor, user_id, 'registration rollback check', schedule, parameters, excel_file)
                    ids.append(experiment_id)
                    row = cursor.execute(
                        'SELECT UserDefinedID, Note, ScheduledToRun FROM dbo.Experiments WHERE ExperimentID=?',
                        experiment_id).fetchone()
                    self.assertEqual(tuple(row), (user_id, 'registration rollback check', int(schedule)))
                    actual_parameters = dict(cursor.execute(
                        'SELECT ParameterName, ParamValueTxt FROM dbo.ExperimentParameters WHERE ExperimentID=?',
                        experiment_id).fetchall())
                    self.assertEqual({k: float(v) for k, v in actual_parameters.items()}, parameters)
                    self.assertEqual([tuple(r) for r in cursor.execute(
                        'SELECT WellID, WellAssign FROM dbo.ExperimentPlatePattern WHERE ExperimentID=? ORDER BY WellID',
                        experiment_id)], [('A1', 'MediaCtrl'), ('B1', 'Cells')])
                    flags = list(cursor.execute(
                        'SELECT ExperimentID, ScheduledToRun FROM dbo.Experiments ORDER BY ExperimentID'))
                    if schedule == '0':
                        self.assertEqual([r for r in flags if r[0] not in ids], original_flags)
                        if index == 1:
                            self.assertEqual(dict(flags)[ids[0]], 1)
                    else:
                        self.assertEqual([r[0] for r in flags if r[1]], [experiment_id])
                    if index == 0:
                        # Guarantee there is another scheduled experiment to clear.
                        cursor.execute('UPDATE dbo.Experiments SET ScheduledToRun=1 WHERE ExperimentID=?', experiment_id)
                    self.assertGreater(cursor.execute('SELECT @@TRANCOUNT').fetchone()[0], 0)
            conn.rollback()
            self.assertEqual(list(cursor.execute(
                'SELECT ExperimentID, ScheduledToRun FROM dbo.Experiments ORDER BY ExperimentID')), original_flags)
            for experiment_id in ids:
                for table in ('Experiments', 'ExperimentParameters', 'ExperimentPlatePattern'):
                    self.assertEqual(cursor.execute(
                        f'SELECT COUNT(*) FROM dbo.{table} WHERE ExperimentID=?', experiment_id).fetchone()[0], 0)
        finally:
            try:
                conn.rollback()
            finally:
                conn.close()


class CheckDatabaseGuard(unittest.TestCase):
    def test_other_database_names_are_rejected_before_connecting(self):
        from unittest.mock import patch
        with patch.object(pyodbc, 'connect') as connect:
            for name in ('EvoYeast', 'master', 'EvoYeast_check;DATABASE=EvoYeast'):
                with self.assertRaises(ValueError):
                    connect_check_database(name)
            connect.assert_not_called()


if __name__ == '__main__':
    unittest.main()
