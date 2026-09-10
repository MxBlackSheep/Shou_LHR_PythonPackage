"""Isolated contracts: these tests never contact SQL Server or robot hardware."""

from io import BytesIO
from pathlib import Path
import random
import tempfile
import unittest
from unittest.mock import Mock, patch

import pandas as pd
import pyodbc

from hamilton_workflows import bcp, database, fluorescence, plate_patterns, task_files
from hamilton_workflows.config import Settings
from hamilton_workflows.runtime import RunLogger, run_logged


class SharedContracts(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def test_column_major_well_order(self):
        wells = plate_patterns.wells_96()
        self.assertEqual(len(set(wells)), 96)
        self.assertEqual(wells[:10], ['A1', 'B1', 'C1', 'D1', 'E1', 'F1', 'G1', 'H1', 'A2', 'B2'])
        self.assertEqual(wells[-1], 'H12')

    def test_raw_parser_preserves_legacy_values_and_order(self):
        path = self.root / 'raw'
        path.write_text(
            '482,510\nWell\t482,510 - Mean\n\nA1\t5.\nH12\t1,234 RFU\n'
            'B1\t-\nC1\tRFU12.5\nD1\tbad\nE1\t-2.5\nF1\t1e3\nignored\n', encoding='utf-8'
        )
        frame = fluorescence.parse_raw_data(path, 7, 'run')
        self.assertEqual(list(frame.columns), ['PlateID', 'WellID', 'Value', 'RunID'])
        self.assertEqual(frame.WellID.tolist(), ['A1', 'H12', 'B1', 'C1', 'D1', 'E1', 'F1'])
        # The existing parser consumes the last token and coerces unparseable values to zero.
        self.assertEqual(frame.Value.tolist(), [5, 0, 0, 12.5, 0, -2.5, 1000])
        self.assertEqual(frame.PlateID.tolist(), [7] * 7)

    def test_invalid_raw_files_fail_without_exiting_the_process(self):
        with self.assertRaises(FileNotFoundError):
            fluorescence.parse_raw_data(self.root / 'missing', 1, 'run')
        for contents in ('header\n', 'header\nheader\nignored\n'):
            path = self.root / 'invalid'
            path.write_text(contents)
            with self.assertRaises(ValueError):
                fluorescence.parse_raw_data(path, 1, 'run')

    def test_random_gen5_roundtrip_and_original_untouched(self):
        for name in ('raw', 'raw.txt'):
            original = self.root / name
            original.write_text('original')
            target = task_files.add_test_suffix_preserve_ext(original)
            fluorescence.generate_random_raw_file(target, '587,611', rng=random.Random(12345))
            frame = fluorescence.parse_raw_data(target, 2, 'run')
            self.assertEqual(frame.WellID.tolist(), plate_patterns.wells_96())
            self.assertTrue(frame.Value.between(0, 2200).all())
            self.assertEqual(original.read_text(), 'original')
            self.assertEqual(Path(target).read_text().splitlines()[:2], ['587,611', 'Well\t587,611 - Mean'])

    def test_simulated_channel_schema_and_range(self):
        frames = fluorescence.generate_two_dfs(9, 'run', rng=random.Random(12345))
        for frame, channel in zip(frames, ('FlEx482Em510', 'FlEx587Em611')):
            self.assertEqual(list(frame.columns), ['PlateID', 'WellID', channel, 'RunID'])
            self.assertEqual(frame.WellID.tolist(), plate_patterns.wells_96())
            self.assertTrue(frame[channel].between(0, 550).all())
        self.assertFalse(frames[0].iloc[:, 2].equals(frames[1].iloc[:, 2]))

    def test_excel_assignment_uses_second_row_header_and_filters_blanks(self):
        source = pd.DataFrame({
            'Destination': [' A1 ', 'B2', None, 'C1'],
            'Source': [' T ', 'culture', 'culture', None], 'Vol': [1, 2, 3, 4],
        })
        workbook = BytesIO()
        source.to_excel(workbook, index=False, startrow=1)
        workbook.seek(0)
        actual = plate_patterns.process_excel_to_well_assignment(workbook, 'run', 17)
        self.assertEqual(list(actual.columns), ['plateid', 'wellID', 'runid', 'WellAssign'])
        self.assertEqual(list(actual.itertuples(index=False, name=None)), [
            (17, 'A1', 'run', 'MediaCtrl'), (17, 'B2', 'run', 'Cells')
        ])

    def test_excel_missing_columns_has_clear_error(self):
        with patch.object(pd, 'read_excel', return_value=pd.DataFrame({'Source': ['T']})):
            with self.assertRaisesRegex(ValueError, 'Destination, Source, Vol'):
                plate_patterns.process_excel_to_well_assignment('unused', 'run', 1)

    def test_task_and_sequence_file_bytes(self):
        path = task_files.task_path(self.root / 'tasks', 'run', 'CultureVol')
        task_files.write_plain_lines(path, [1, 2.5])
        self.assertEqual(path.read_bytes(), b'1\r\n2.5')
        task_files.write_plain_lines(path, [1], trailing_newline=True)
        self.assertEqual(path.read_bytes(), b'1\r\n')
        task_files.write_hamilton_sequence(path, ['A1', 'B2'], 'plate', 'seq', 'layout')
        self.assertEqual(path.read_bytes(),
            b'Id,Layout,Sequence,Labware,Position\n1,layout,seq,plate,A1\n2,layout,seq,plate,B2\n')

    def test_tsv_and_bcp_payload_column_order(self):
        path = self.root / 'payload'
        task_files.write_tsv_rows(path, [[7, 'run', 'B1', 1]])
        self.assertEqual(path.read_bytes(), b'7\trun\tB1\t1\r\n')
        bcp.write_bcp_file(pd.DataFrame([[7, 'B1', 2.5, 'run']]), path)
        self.assertEqual(path.read_bytes(), b'7\tB1\t2.5\trun\r\n')

    def test_bcp_command_and_nonzero_exit(self):
        settings = Settings()
        with patch.object(bcp.subprocess, 'run', return_value=Mock(returncode=0, stdout='ok')) as execute:
            bcp.run_bcp('ImportFlEx482Em510', self.root / 'input file.txt', settings)
            self.assertEqual(execute.call_args.args[0], [
                'bcp', 'EvoYeast.dbo.ImportFlEx482Em510', 'in', str(self.root / 'input file.txt'),
                '-T', '-c', '-S', r'HAMILTON-PC\HAMILTON',
            ])
            self.assertEqual(execute.call_args.kwargs['creationflags'], bcp.subprocess.CREATE_NO_WINDOW)
            self.assertNotIn('shell', execute.call_args.kwargs)
            execute.return_value = Mock(returncode=1, stdout='details', stderr='failed')
            with self.assertRaisesRegex(RuntimeError, 'failed'):
                bcp.run_bcp('ImportFlEx482Em510', 'file', settings)

    def test_connection_never_implicitly_commits_and_always_closes(self):
        conn = Mock()
        with patch.object(database, 'connect', return_value=conn):
            with database.connection(Settings()) as actual:
                self.assertIs(actual, conn)
        conn.commit.assert_not_called()
        conn.close.assert_called_once()
        for error in (RuntimeError('error'), SystemExit(1)):
            conn.reset_mock()
            with patch.object(database, 'connect', return_value=conn):
                with self.assertRaises(type(error)):
                    with database.connection(Settings()):
                        raise error
            conn.rollback.assert_called_once()
            conn.close.assert_called_once()

    def test_queries_are_parameterized_and_missing_ids_fail(self):
        cursor = Mock()
        cursor.fetchone.return_value = (19,)
        self.assertEqual(database.plate_id_from_barcode(cursor, "BC'001"), 19)
        cursor.execute.assert_called_with('SELECT PlateID FROM Plates WHERE BarCode = ?', ("BC'001",))
        for row in (None, (None,), ('',)):
            cursor.fetchone.return_value = row
            with self.assertRaises(RuntimeError):
                database.latest_run_id(cursor)

    def test_result_sets_skip_no_row_sets_but_propagate_data_errors(self):
        cursor = Mock()
        cursor.fetchall.side_effect = [[(1,)], pyodbc.ProgrammingError('no rows'), [(2,)]]
        cursor.nextset.side_effect = [True, True, False]
        self.assertEqual(database.fetch_all_result_sets(cursor), [(1,), (2,)])
        cursor.fetchall.side_effect = pyodbc.DataError('bad data')
        with self.assertRaises(pyodbc.DataError):
            database.fetch_all_result_sets(cursor)

    def test_settings_override_and_lazy_log(self):
        with patch.dict('os.environ', {'HAMILTON_TASK_DIR': str(self.root), 'HAMILTON_SERVER': 'test-server'}):
            settings = Settings.from_env()
        self.assertEqual(settings.task_dir, self.root)
        self.assertEqual(settings.server, 'test-server')
        self.assertNotIn(settings.password, repr(settings))
        logger = RunLogger(self.root / 'logs', 'workflow')
        self.assertFalse(logger.path.parent.exists())
        logger('hello')
        self.assertIn('] hello\n', logger.path.read_text())
        self.assertEqual(run_logged(lambda: None, logger), 0)
        self.assertEqual(run_logged(lambda: 1 / 0, logger), 1)


if __name__ == '__main__':
    unittest.main()
