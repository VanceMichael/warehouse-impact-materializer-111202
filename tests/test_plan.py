import tempfile
import unittest
from pathlib import Path

from warehouse.catalog import connect, load_bundle
from warehouse.engine import compare, full_results, rebuild_full, run
from warehouse.plan import (
    PlanError,
    ensure_tables,
    forward_labels,
    input_labels,
    load_plan,
    shift_day,
    topological_order,
    week_label,
)

ROOT = Path(__file__).resolve().parents[1]
PLAN = ROOT / 'examples/plans/demand.toml'


def base_bundle():
    return {
        'demand': [
            {'record': 'r1', 'day': '2026-09-28', 'item': 's1', 'units': 3},
            {'record': 'r2', 'day': '2026-10-01', 'item': 's1', 'units': 4},
            {'record': 'r3', 'day': '2026-10-04', 'item': 's1', 'units': 2},
        ]
    }


class PlanTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.plan = load_plan(PLAN)
        self.connection = connect(Path(self.temporary.name) / 'w.sqlite3')
        ensure_tables(self.plan, self.connection)

    def tearDown(self):
        self.connection.close()
        self.temporary.cleanup()

    def test_topology_and_nodes(self):
        order = [node.name for node in topological_order(self.plan)]
        self.assertEqual(order[0], 'daily')
        self.assertLess(order.index('daily'), order.index('weekly'))
        names = {node.name: node for node in self.plan.nodes}
        self.assertEqual(names['rolling_7d'].mapping.kind, 'day_offset')
        self.assertEqual(names['rolling_7d'].mapping.window_days, 6)
        self.assertEqual(names['weekly'].mapping.kind, 'day_to_week')

    def test_date_helpers_use_calendar_not_strings(self):
        self.assertEqual(shift_day('2026-12-31', 1), '2027-01-01')
        self.assertEqual(week_label('2027-01-01'), '2026-W53')
        self.assertEqual(input_labels(self.plan.by_name()['rolling_7d'], '2026-10-04')[:2], ['2026-10-04', '2026-10-03'])
        affected = forward_labels(self.plan.by_name()['weekly'], ['2027-01-01'])
        self.assertEqual(affected, {'2026-W53'})

    def test_cross_year_week_window_is_provable(self):
        node = self.plan.by_name()['rolling_7d']
        self.assertEqual(len(forward_labels(node, ['2026-12-31'])), 7)
        self.assertIn('2027-01-06', forward_labels(node, ['2026-12-31']))
        weekly = self.plan.by_name()['weekly']
        self.assertEqual(forward_labels(weekly, ['2026-12-28']), {'2026-W53'})

    def test_cycle_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / 'cycle.toml'
            path.write_text(
                (PLAN.parent / 'demand.toml').read_text(encoding='utf-8').replace(
                    'depends_on = ["daily"]', 'depends_on = ["weekly"]'
                ),
                encoding='utf-8',
            )
            with self.assertRaises(PlanError):
                load_plan(path)

    def test_undeclared_reference_is_rejected(self):
        text = PLAN.read_text(encoding='utf-8').replace(
            'target = [{ column = "week", partition = true }, "item"]',
            'target = [{ column = "week", partition = true }, "missing"]',
        )
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / 'bad.toml'
            path.write_text(text, encoding='utf-8')
            with self.assertRaises(PlanError):
                load_plan(path)

    def test_unprovable_window_is_rejected(self):
        text = PLAN.read_text(encoding='utf-8').replace(
            'window_days = 6', 'window_days = "six"'
        )
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / 'bad.toml'
            path.write_text(text, encoding='utf-8')
            with self.assertRaises(PlanError):
                load_plan(path)

    def test_unique_key_mismatch_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / 'key.toml'
            text = PLAN.read_text(encoding='utf-8')
            section = text[text.index('[tables.weekly_units]'):]
            changed = text.replace(
                section, section.replace('key = ["week", "item"]', 'key = ["week", "week_units"]', 1), 1
            )
            path.write_text(changed, encoding='utf-8')
            with self.assertRaises(PlanError):
                load_plan(path)

    def test_source_table_must_be_declared(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / 'source.toml'
            text = PLAN.read_text(encoding='utf-8')
            start = text.index('[tables.demand]')
            end = text.index('[tables.daily_units]')
            path.write_text(text[:start] + text[end:], encoding='utf-8')
            with self.assertRaises(PlanError):
                load_plan(path)

    def test_actual_table_drift_is_rejected(self):
        self.connection.execute('DROP TABLE daily_units')
        self.connection.execute(
            'CREATE TABLE daily_units (day TEXT, item TEXT, demand_units INTEGER, extra TEXT)'
        )
        with self.assertRaises(PlanError):
            ensure_tables(self.plan, self.connection)

    def test_declared_window_mismatch_is_rejected(self):
        text = PLAN.read_text(encoding='utf-8').replace('window_days = 6', 'window_days = 6.5')
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / 'bad.toml'
            path.write_text(text, encoding='utf-8')
            with self.assertRaises(PlanError):
                load_plan(path)

    def test_window_on_non_window_mapping_is_rejected(self):
        text = PLAN.read_text(encoding='utf-8').replace(
            'kind = "identity"', 'kind = "identity", window_days = 3'
        )
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / 'bad.toml'
            path.write_text(text, encoding='utf-8')
            with self.assertRaises(PlanError):
                load_plan(path)


class MaterializationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / 'w.sqlite3'
        self.connection = connect(self.path)
        self.plan = load_plan(PLAN)
        ensure_tables(self.plan, self.connection)

    def tearDown(self):
        self.connection.close()
        self.temporary.cleanup()

    def load(self, bundle=None, batch='daily'):
        load_bundle(self.connection, bundle or base_bundle(), batch)

    def assert_matches_full(self):
        self.assertTrue(compare(self.plan, self.connection, full_results(self.plan, self.connection))['equal'])

    def test_preview_lists_partitions_before_execution(self):
        self.load()
        preview = run(self.plan, self.connection, apply=False)
        self.assertFalse(preview.applied)
        self.assertEqual(preview.nodes[0].dirty['2026-10-01'].action, 'recalculate')
        self.assertEqual(self.connection.execute('SELECT COUNT(*) FROM daily_units').fetchone()[0], 0)
        self.assertEqual(self.connection.execute('SELECT COUNT(*) FROM node_partitions').fetchone()[0], 0)

    def test_first_run_and_noop(self):
        self.load()
        first = run(self.plan, self.connection)
        self.assertTrue(first.applied)
        self.assertEqual(self.connection.execute('SELECT COUNT(*) FROM daily_units').fetchone()[0], 3)
        second = run(self.plan, self.connection, apply=False)
        self.assertEqual([node.dirty for node in second.nodes], [{}, {}, {}])
        self.assertEqual(self.connection.execute('SELECT COUNT(*) FROM stage_partitions').fetchone()[0], 0)

    def test_single_day_rolls_forward(self):
        self.load()
        run(self.plan, self.connection)
        self.connection.execute("UPDATE demand SET units=6 WHERE record='r2'")
        preview = run(self.plan, self.connection, apply=False)
        daily = {node.node.name: node for node in preview.nodes}['daily']
        self.assertEqual(set(daily.dirty), {'2026-10-01'})
        rolling = {node.node.name: node for node in preview.nodes}['rolling_7d']
        self.assertEqual(
            set(rolling.dirty),
            {'2026-10-01', '2026-10-02', '2026-10-03', '2026-10-04', '2026-10-05', '2026-10-06', '2026-10-07'},
        )
        run(self.plan, self.connection)
        rows = [dict(row) for row in self.connection.execute('SELECT * FROM rolling_7d ORDER BY day')]
        self.assertEqual(rows[3]['window_units'], 3 + 6)
        self.assertEqual(rows[6]['window_units'], 3 + 6 + 2)
        self.assert_matches_full()

    def test_delete_removes_partitions(self):
        self.load()
        run(self.plan, self.connection)
        self.connection.execute("DELETE FROM demand WHERE record='r2'")
        preview = run(self.plan, self.connection, apply=False)
        daily = {node.node.name: node for node in preview.nodes}['daily']
        self.assertEqual(daily.dirty['2026-10-01'].action, 'delete')
        run(self.plan, self.connection)
        self.assertEqual(
            [row[0] for row in self.connection.execute('SELECT day FROM daily_units ORDER BY day')],
            ['2026-09-28', '2026-10-04'],
        )
        self.assert_matches_full()

    def test_dimension_drift_triggers_adjacent_weeks(self):
        self.load({'demand': [
            {'record': 'a', 'day': '2026-12-25', 'item': 's1', 'units': 1},
            {'record': 'b', 'day': '2027-01-01', 'item': 's1', 'units': 2},
        ]}, 'cross')
        run(self.plan, self.connection)
        weekly = [dict(row) for row in self.connection.execute('SELECT * FROM weekly_units ORDER BY week')]
        self.assertEqual([row['week'] for row in weekly], ['2026-W52', '2026-W53'])
        self.connection.execute("DELETE FROM demand WHERE record='a'")
        preview = run(self.plan, self.connection, apply=False)
        nodes = {node.node.name: node for node in preview.nodes}
        self.assertEqual(set(nodes['weekly'].dirty), {'2026-W52'})
        run(self.plan, self.connection)
        weekly = [dict(row) for row in self.connection.execute('SELECT * FROM weekly_units ORDER BY week')]
        self.assertEqual([row['week'] for row in weekly], ['2026-W53'])
        self.assert_matches_full()

    def test_rule_change_invalidates_and_propagates(self):
        self.load()
        run(self.plan, self.connection)
        text = PLAN.read_text(encoding='utf-8').replace(
            'name = "daily"\ntable = "daily_units"',
            'name = "daily"\ntable = "daily_units"',
        ).replace(
            'rule = { op = "sum", value = "units", output = "demand_units" }',
            'rule = { op = "max", value = "units", output = "demand_units" }',
        )
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / 'max.toml'
            path.write_text(text, encoding='utf-8')
            changed = load_plan(path)
        preview = run(changed, self.connection, apply=False)
        nodes = {node.node.name: node for node in preview.nodes}
        self.assertTrue(nodes['daily'].rule_changed)
        self.assertIn('2026-10-01', nodes['daily'].dirty)
        self.assertIn('rule', {cause.kind for cause in nodes['daily'].dirty['2026-10-01'].causes})
        self.assertIn('upstream_rule', {cause.kind for cause in nodes['rolling_7d'].dirty['2026-10-01'].causes})
        self.assertEqual({cause.kind for cause in nodes['weekly'].dirty['2026-W40'].causes}, {'upstream_rule'})
        self.assertIn('rule', nodes['weekly'].summary())
        run(changed, self.connection)
        self.assertEqual(
            dict(self.connection.execute("SELECT demand_units FROM daily_units WHERE day='2026-09-28'").fetchone())['demand_units'],
            3,
        )
        self.assert_matches_full()

    def test_record_moves_across_week_boundary(self):
        self.load({'demand': [
            {'record': 'a', 'day': '2026-12-27', 'item': 's1', 'units': 1},
        ]}, 'move')
        run(self.plan, self.connection)
        self.connection.execute("UPDATE demand SET day='2026-12-28' WHERE record='a'")
        preview = run(self.plan, self.connection, apply=False)
        nodes = {node.node.name: node for node in preview.nodes}
        self.assertEqual(set(nodes['weekly'].dirty), {'2026-W52', '2026-W53'})
        self.assertEqual(nodes['weekly'].dirty['2026-W52'].action, 'delete')
        self.assertEqual(nodes['weekly'].dirty['2026-W53'].action, 'recalculate')
        self.assertEqual(nodes['weekly'].summary()['source'], ['upstream_content'])
        run(self.plan, self.connection)
        weekly = [dict(row) for row in self.connection.execute('SELECT * FROM weekly_units ORDER BY week')]
        self.assertEqual([row['week'] for row in weekly], ['2026-W53'])
        self.assert_matches_full()

    def test_dimension_change_reported_as_source_not_rule(self):
        self.load()
        run(self.plan, self.connection)
        self.connection.execute("UPDATE demand SET item='s2' WHERE record='r2'")
        preview = run(self.plan, self.connection, apply=False)
        nodes = {node.node.name: node for node in preview.nodes}
        kinds = {cause.kind for cause in nodes['rolling_7d'].dirty['2026-10-01'].causes}
        self.assertEqual(kinds, {'upstream_content'})
        self.assertFalse(nodes['daily'].rule_changed)
        self.assertTrue(all(cause.kind != 'rule' for cause in nodes['daily'].dirty['2026-10-01'].causes))

    def test_irrelevant_partitions_not_scanned(self):
        self.load()
        run(self.plan, self.connection)
        self.connection.execute("UPDATE demand SET units=9 WHERE record='r1'")
        preview = run(self.plan, self.connection, apply=False)
        nodes = {node.node.name: node for node in preview.nodes}
        self.assertEqual(nodes['daily'].scanned, set())
        affected = set(nodes['rolling_7d'].dirty)
        self.assertEqual(affected, {
            '2026-09-28', '2026-09-29', '2026-09-30', '2026-10-01', '2026-10-02', '2026-10-03', '2026-10-04',
        })
        self.assertNotIn('2026-10-10', nodes['rolling_7d'].scanned)
        self.assertFalse(set(nodes['rolling_7d'].scanned) - {shift_day(label, -step) for label in affected for step in range(7)})

    def test_interrupt_keeps_published_and_recovers(self):
        self.load()
        run(self.plan, self.connection)

        def interrupt(name):
            if name == 'rolling_7d':
                raise RuntimeError('模拟中断')

        self.connection.execute("UPDATE demand SET units=8 WHERE record='r2'")
        with self.assertRaises(RuntimeError):
            run(self.plan, self.connection, stage_hook=interrupt)
        self.assertEqual(
            dict(self.connection.execute("SELECT demand_units FROM daily_units WHERE day='2026-10-01'").fetchone())['demand_units'],
            4,
        )
        self.assertTrue(self.connection.execute('SELECT COUNT(*) FROM stage_partitions').fetchone()[0] > 0)
        report = run(self.plan, self.connection, recover=True)
        self.assertEqual(report.nodes[0].reuse_staged, 1)
        self.assertEqual(report.nodes[1].reuse_staged, 7)
        self.assert_matches_full()

    def test_staged_results_not_reused_after_input_change(self):
        self.load()
        run(self.plan, self.connection)

        def interrupt(name):
            if name == 'rolling_7d':
                raise RuntimeError('模拟中断')

        self.connection.execute("UPDATE demand SET units=8 WHERE record='r2'")
        with self.assertRaises(RuntimeError):
            run(self.plan, self.connection, stage_hook=interrupt)
        self.connection.execute("UPDATE demand SET units=9 WHERE record='r2'")
        report = run(self.plan, self.connection, recover=True)
        nodes = {node.node.name: node for node in report.nodes}
        self.assertEqual(nodes['daily'].reuse_staged, 0)
        self.assertEqual(
            dict(self.connection.execute("SELECT demand_units FROM daily_units WHERE day='2026-10-01'").fetchone())['demand_units'],
            9,
        )
        self.assert_matches_full()

    def test_publish_is_atomic(self):
        self.load({'demand': base_bundle()['demand'] + [
            {'record': 'r4', 'day': '2026-10-07', 'item': 's1', 'units': 5},
            {'record': 'r5', 'day': '2026-10-08', 'item': 's1', 'units': 6},
        ]}, 'wide')
        run(self.plan, self.connection)
        before = [dict(row) for row in self.connection.execute('SELECT * FROM rolling_7d ORDER BY day')]
        self.connection.execute("UPDATE demand SET units=50 WHERE record='r4'")

        def fail_before_commit(_):
            raise RuntimeError('提交前中断')

        with self.assertRaises(RuntimeError):
            run(self.plan, self.connection, publish_hook=fail_before_commit)
        after = [dict(row) for row in self.connection.execute('SELECT * FROM rolling_7d ORDER BY day')]
        self.assertEqual(before, after)

    def test_restore_skips_unchanged_both_levels(self):
        self.load()
        run(self.plan, self.connection)
        self.connection.execute("UPDATE demand SET units=7 WHERE record='r2'")
        report = run(self.plan, self.connection, apply=True)
        nodes = {node.node.name: node for node in report.nodes}
        self.assertEqual(
            set(nodes['rolling_7d'].dirty),
            {'2026-10-01', '2026-10-02', '2026-10-03', '2026-10-04', '2026-10-05', '2026-10-06', '2026-10-07'},
        )
        self.assertEqual(nodes['rolling_7d'].reuse_published, 0)
        self.assertEqual(nodes['rolling_7d'].verified, 6)
        self.assertEqual(nodes['weekly'].verified, 0)
        self.assert_matches_full()

    def test_corrupted_published_partition_is_not_skipped(self):
        self.load()
        run(self.plan, self.connection)
        self.connection.execute("UPDATE daily_units SET demand_units=99 WHERE day='2026-09-28'")
        report = run(self.plan, self.connection, apply=False)
        nodes = {node.node.name: node for node in report.nodes}
        self.assertIn('2026-09-28', nodes['daily'].dirty)
        self.assertEqual(nodes['daily'].dirty['2026-09-28'].action, 'recalculate')
        run(self.plan, self.connection)
        self.assert_matches_full()

    def test_rebuild_full_matches_incremental(self):
        self.load()
        run(self.plan, self.connection)
        self.connection.execute("UPDATE demand SET units=11 WHERE record='r1'")
        run(self.plan, self.connection)
        rebuild_full(self.plan, self.connection)
        self.assert_matches_full()
        noop = run(self.plan, self.connection, apply=False)
        self.assertEqual([node.dirty for node in noop.nodes], [{}, {}, {}])


if __name__ == '__main__':
    unittest.main()
