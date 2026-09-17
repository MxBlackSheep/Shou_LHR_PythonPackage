"""Opt-in integration tests in a newly created, disposable SQL database.

Set HAMILTON_PROPAGATION_SQL_TESTS=1 to run. Reads procedure definitions and a
Hamilton run timestamp from the configured installation, but writes ONLY to a
fresh CodexChampionsTest_<uuid> database. Always drops that database afterward.
"""

from dataclasses import replace
from datetime import timedelta
import os
from pathlib import Path
import re
import subprocess
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
import uuid

import pyodbc

from hamilton_workflows import database, propagation as calc, propagation_store as store
from hamilton_workflows.config import Settings
from test_propagation import workflow, snapshot


@unittest.skipUnless(os.environ.get('HAMILTON_PROPAGATION_SQL_TESTS') == '1', 'opt-in disposable SQL database')
class SqlIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.installation = Settings.from_env()
        cls.db_name = 'CodexChampionsTest_' + uuid.uuid4().hex
        cls.admin = cls.connect_to('master', autocommit=True)
        cls.admin.execute(f'CREATE DATABASE [{cls.db_name}]')
        print(f'Created disposable integration database: {cls.db_name}')
        cls.addClassCleanup(cls.drop_database)
        cls.settings = replace(cls.installation, database=cls.db_name)
        with database.connection(cls.installation) as source:
            cls.run_id = database.latest_run_id(source.cursor())
            cls.started = source.execute('SELECT StartTime FROM HamiltonVectorDB.dbo.HxRun WHERE RunGUID=?', cls.run_id).fetchone()[0]
            cls.functions = []
            for name in ('Descendants', 'OccupiedWellsInPlatesChain', 'UnoccupiedWellsInPlatesChain'):
                definition = source.execute('SELECT OBJECT_DEFINITION(OBJECT_ID(?))', 'dbo.' + name).fetchone()[0]
                if not definition:
                    raise RuntimeError(f'Missing read-only reference function {name}')
                cls.functions.append(definition)
        conn = cls.connect_to(cls.db_name)
        try:
            conn.execute('''
                CREATE TABLE dbo.Experiments (ExperimentID int PRIMARY KEY,ScheduledToRun bit);
                CREATE TABLE dbo.AncestPlatesInExperiments (ExperimentID int,PlateID int);
                CREATE TABLE dbo.Plates (PlateID int PRIMARY KEY,BarCode varchar(255),Discarded bit);
                CREATE TABLE dbo.Cultures (CultureID int PRIMARY KEY,PlateID int,WellID varchar(10));
                CREATE TABLE dbo.CulturesHistory (CultureID int NOT NULL,Iteration int,OD float,DilutionFactor float,
                  Status_mature bit,Status_frozen bit,TimeStamp datetime,FlEx482Em510 float,FlEx587Em611 float,
                  OD_FlEx482Em510 float,OD_FlEx587Em611 float);
                CREATE TABLE dbo.Propagation (ParentCultureID int,ChldCultureID int NOT NULL);
                CREATE TABLE dbo.Cytomat (CytomatPos int PRIMARY KEY,PlateID int);
                CREATE TABLE dbo.ExperimentParameters (ExperimentID int,ParameterName varchar(50),ParamValueTxt varchar(1024));
                CREATE TABLE dbo.ImportFlEx482Em510 (PlateID int,WellID varchar(10),RunID varchar(255),FlEx482Em510 float);
                CREATE TABLE dbo.ImportFlEx587Em611 (PlateID int,WellID varchar(10),RunID varchar(255),FlEx587Em611 float);
            ''')
            for definition in cls.functions:
                conn.execute(definition)
            conn.commit()
        finally:
            conn.close()

    @classmethod
    def connect_to(cls, name, autocommit=False):
        s = cls.installation
        return pyodbc.connect(f'DRIVER={{{s.odbc_driver}}};SERVER={s.server};DATABASE={name};'
                              f'UID={s.username};PWD={s.password};Encrypt=no;TrustServerCertificate=yes;',
                              autocommit=autocommit, timeout=10)

    @classmethod
    def drop_database(cls):
        # Only the generated database is eligible for teardown. Never accept a
        # configured database name or user-supplied path as a deletion target.
        if not re.fullmatch(r'CodexChampionsTest_[0-9a-f]{32}', cls.db_name):
            raise RuntimeError('Refusing to drop a non-test database')
        cls.admin.execute(f'ALTER DATABASE [{cls.db_name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE')
        cls.admin.execute(f'DROP DATABASE [{cls.db_name}]')
        cls.admin.close()

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.current_settings = replace(self.settings, task_dir=Path(self.temporary.name) / 'tasks', layout_path='layout')
        self.reset_fixture()

    def reset_fixture(self, size=84, fraction=0.25):
        conn = self.connect_to(self.db_name)
        try:
            for table in ('CulturesHistory','Propagation','Cultures','Cytomat',
                          'ImportFlEx482Em510','ImportFlEx587Em611','ExperimentParameters',
                          'AncestPlatesInExperiments','Plates','Experiments'):
                conn.execute(f'DELETE FROM dbo.{table}')
            conn.execute('INSERT dbo.Experiments VALUES (1,1)')
            conn.execute('INSERT dbo.AncestPlatesInExperiments VALUES (1,1)')
            conn.execute("INSERT dbo.Plates VALUES (1,'source',0),(2,'child',0)")
            conn.execute('INSERT dbo.Cytomat VALUES (4,2)')
            conn.execute('INSERT dbo.Cultures VALUES (100000,1,NULL),(200000,2,NULL)')
            conn.execute('INSERT dbo.Propagation VALUES (100000,200000)')
            data = snapshot(size, fraction)
            cur = conn.cursor()
            cur.executemany('INSERT dbo.ExperimentParameters VALUES (1,?,?)',
                            [(name, str(value)) for name, value in data['parameters'].items()])
            cur.executemany('INSERT dbo.Cultures VALUES (?,1,?)', [(s['culture_id'],s['well']) for s in data['sources']])
            cur.executemany(store.HISTORY_SQL, [(s['culture_id'],1,0,1,0,0,self.started-timedelta(days=1),10,10,0,0)
                                                for s in data['sources']])
            for channel, raw in [('FlEx482Em510','raw_gfp'),('FlEx587Em611','raw_rfp')]:
                cur.executemany(f'INSERT dbo.Import{channel} VALUES (1,?,?,?)',
                                [(s['well'],self.run_id,s[raw]) for s in data['sources']])
            conn.commit()
        finally:
            conn.close()

    def execute(self, sql, *params):
        conn = self.connect_to(self.db_name)
        try:
            cur = conn.execute(sql, *params)
            result = cur.fetchall() if cur.description else []
            conn.commit()
            return result
        finally:
            conn.close()

    def run_workflow(self, **kwargs):
        return workflow.propagate('source','source','spill',self.current_settings,lambda _: None,
                                  run_id=self.run_id, **kwargs)

    def counts(self):
        return tuple(self.execute(f'SELECT COUNT(*) FROM dbo.{table}')[0][0]
                     for table in ('Cultures','CulturesHistory','Propagation'))

    def test_exact_splits_without_journal_and_repeat_is_rejected(self):
        for size in (84, 96):
            for fraction in (1,0.5,0.25):
                with self.subTest(size=size, fraction=fraction):
                    self.reset_fixture(size, fraction)
                    self.assertIsNone(self.execute("SELECT OBJECT_ID('dbo.ChampionsPropagationPlans', 'U')")[0][0])
                    self.run_workflow()
                    self.assertEqual(self.counts(), (2*size+2,3*size,size+1))
                    before = {p.name:p.read_bytes() for p in self.current_settings.task_dir.glob('*.txt')}
                    with self.assertRaisesRegex(ValueError, 'already processed'):
                        self.run_workflow()
                    self.assertEqual(self.counts(), (2*size+2,3*size,size+1))
                    self.assertEqual(before, {p.name:p.read_bytes() for p in self.current_settings.task_dir.glob('*.txt')})
                    self.assertEqual(self.execute('SELECT COUNT(*) FROM dbo.CulturesHistory WHERE Iteration=1 AND CultureID>200000 AND OD<=0')[0][0], 0)

    def test_read_only_sql_calibration_parity(self):
        for gfp,rfp in ((1,2),(10,10),(100,200),(10000,20),(20,10000)):
            row = self.execute('''
                DECLARE @g float=?,@r float=?,@target float=700,@inoc float=0.03,@split float=4;
                SET @g=CASE WHEN @g<10 THEN 10 ELSE @g END;
                SET @r=CASE WHEN @r<10 THEN 10 ELSE @r END;
                DECLARE @og float=(2262.3*@g-773.1*@r)/(4669.1*2262.3+773.1*305.0);
                DECLARE @orr float=(4669.1*@r-305.0*@g)/(4669.1*2262.3+773.1*305.0);
                DECLARE @v float=@inoc*@target/(@og+@orr),@cap float=@target/3.0/@split;
                SET @v=CASE WHEN @v>@cap THEN @cap ELSE @v END;
                SELECT @og,@orr,@v,@target-@v,(@og+@orr)*@v/@target;
            ''', gfp,rfp)[0]
            expected = calc.corrected_readings(gfp,rfp)
            volume = min(0.03*700/expected['od'],700/3/4)
            for actual,wanted in zip(row,(expected['od_gfp'],expected['od_rfp'],volume,700-volume,expected['od']*volume/700)):
                self.assertAlmostEqual(actual,wanted,places=10)

    def test_database_failure_rolls_back_all_records(self):
        before = self.counts()
        original = store.persist_plan
        def fail(cursor, plan):
            original(cursor, plan)
            raise RuntimeError('injected after all writes')
        with patch.object(store,'persist_plan',side_effect=fail), self.assertRaisesRegex(RuntimeError,'injected'):
            self.run_workflow()
        self.assertEqual(self.counts(),before)
        self.assertEqual(list(self.current_settings.task_dir.iterdir()),[])
        self.run_workflow()
        self.assertEqual(self.counts(), (170,252,85))

    def test_duplicate_reading_and_occupied_capacity_fail_without_writes(self):
        before = self.counts()
        self.execute('INSERT dbo.ImportFlEx482Em510 SELECT * FROM dbo.ImportFlEx482Em510 WHERE WellID=\'A1\'')
        with self.assertRaisesRegex(ValueError,'exactly one'):
            self.run_workflow()
        self.assertEqual(self.counts(),before)
        self.reset_fixture()
        self.execute("INSERT dbo.Cultures VALUES (200101,2,'A1')")
        before = self.counts()
        with self.assertRaisesRegex(ValueError,'capacity'):
            self.run_workflow()
        self.assertEqual(self.counts(),before)

    def test_dry_run_does_not_persist(self):
        before = self.counts()
        with patch('builtins.print'):
            plan = self.run_workflow(dry_run=True)
        self.assertEqual(len(plan['transfers']),84)
        self.assertEqual(self.counts(),before)
        self.assertFalse(self.current_settings.task_dir.exists())

    def test_partial_publication_commits_once_and_blocks_repeat(self):
        from hamilton_workflows import propagation_files as files
        original = files.os.replace
        calls = []
        def fail(source, target):
            calls.append(target)
            if len(calls) == 3:
                raise PermissionError('locked output')
            original(source, target)
        with patch.object(files.os, 'replace', side_effect=fail):
            with self.assertRaisesRegex(RuntimeError, 'Manual reconciliation'):
                self.run_workflow()
        self.assertEqual(self.counts(), (170,252,85))
        outputs = {p.name:p.read_bytes() for p in self.current_settings.task_dir.glob('*.txt')}
        self.assertEqual(len(outputs), 2)
        with self.assertRaisesRegex(ValueError, 'already processed'):
            self.run_workflow()
        self.assertEqual(self.counts(), (170,252,85))
        self.assertEqual(outputs, {p.name:p.read_bytes() for p in self.current_settings.task_dir.glob('*.txt')})

    def test_multiple_plates_and_invalid_rates_stop_before_writes(self):
        self.execute("INSERT dbo.Plates VALUES (3,'third',0)")
        self.execute('INSERT dbo.Cultures VALUES (300000,3,NULL)')
        self.execute('INSERT dbo.Propagation VALUES (100000,300000)')
        self.execute('INSERT dbo.Cytomat VALUES (5,3)')
        self.execute("INSERT dbo.Cultures VALUES (200101,2,'A1')")
        before = self.counts()
        with self.assertRaisesRegex(ValueError, 'multiple destination plates'):
            self.run_workflow()
        self.assertEqual(self.counts(), before)
        self.execute("UPDATE dbo.ExperimentParameters SET ParamValueTxt='0.75' WHERE ParameterName='TopFractionToPropagate'")
        with self.assertRaisesRegex(ValueError, '0.25, 0.5 or 1'):
            self.run_workflow()
        self.assertEqual(self.counts(), before)

    def test_two_callers_serialize_and_only_one_can_propagate(self):
        locked = threading.Event()
        proceed = threading.Event()
        original = store.persist_plan
        results, errors = [], []
        def pause(cursor, plan):
            locked.set()
            if not proceed.wait(10):
                raise RuntimeError('test did not release first planner')
            return original(cursor,plan)
        def worker():
            try:
                results.append(self.run_workflow())
            except Exception as error:
                errors.append(error)
        with patch.object(store,'persist_plan',side_effect=pause):
            first = threading.Thread(target=worker)
            second = threading.Thread(target=worker)
            first.start()
            self.assertTrue(locked.wait(10))
            second.start()
            time.sleep(0.2)
            proceed.set()
            first.join(25)
            second.join(25)
            self.assertFalse(first.is_alive() or second.is_alive())
        self.assertEqual(len(errors),1)
        self.assertIn('already processed',str(errors[0]))
        self.assertEqual(len(results),1)
        self.assertEqual(self.counts(),(170,252,85))

    @unittest.skipUnless(os.environ.get('HAMILTON_PROPAGATION_EXE'), 'optional packaged executable smoke test')
    def test_packaged_executable_dry_run_propagation_and_repeat(self):
        executable = Path(os.environ['HAMILTON_PROPAGATION_EXE']).resolve(strict=True)
        before = self.counts()
        env = os.environ.copy()
        for field in self.current_settings.__dataclass_fields__:
            env['HAMILTON_' + field.upper()] = str(getattr(self.current_settings, field))
        log_dir = Path(self.temporary.name) / 'logs'
        env['HAMILTON_LOG_DIR'] = str(log_dir)
        result = subprocess.run([str(executable),'source','source','spill','--run-id',self.run_id,'--dry-run'],
                                env=env,capture_output=True,timeout=30,creationflags=subprocess.CREATE_NO_WINDOW)
        self.assertEqual(result.returncode,0, result.stderr.decode(errors='replace'))
        self.assertEqual(self.counts(),before)
        self.assertFalse(self.current_settings.task_dir.exists())
        self.assertIn('Dry-run plan', next(log_dir.glob('*.log')).read_text())
        command = [str(executable),'source','source','spill','--run-id',self.run_id]
        result = subprocess.run(command,env=env,capture_output=True,timeout=30,creationflags=subprocess.CREATE_NO_WINDOW)
        self.assertEqual(result.returncode,0)
        self.assertEqual(self.counts(),(170,252,85))
        result = subprocess.run(command,env=env,capture_output=True,timeout=30,creationflags=subprocess.CREATE_NO_WINDOW)
        self.assertEqual(result.returncode,1)
        self.assertEqual(self.counts(),(170,252,85))


if __name__ == '__main__':
    unittest.main()
