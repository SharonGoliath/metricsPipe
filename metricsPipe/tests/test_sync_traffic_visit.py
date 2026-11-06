# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2026.                            (c) 2026.
#  Government of Canada                 Gouvernement du Canada
#  National Research Council            Conseil national de recherches
#  Ottawa, Canada, K1A 0R6              Ottawa, Canada, K1A 0R6
#  All rights reserved                  Tous droits réservés
#
#  NRC disclaims any warranties,        Le CNRC dénie toute garantie
#  expressed, implied, or               énoncée, implicite ou légale,
#  statutory, of any kind with          de quelque nature que ce
#  respect to the software,             soit, concernant le logiciel,
#  including without limitation         y compris sans restriction
#  any warranty of merchantability      toute garantie de valeur
#  or fitness for a particular          marchande ou de pertinence
#  purpose. NRC shall not be            pour un usage particulier.
#  liable in any event for any          Le CNRC ne pourra en aucun cas
#  damages, whether direct or           être tenu responsable de tout
#  indirect, special or general,        dommage, direct ou indirect,
#  consequential or incidental,         particulier ou général,
#  arising from the use of the          accessoire ou fortuit, résultant
#  software.  Neither the name          de l'utilisation du logiciel. Ni
#  of the National Research             le nom du Conseil National de
#  Council of Canada nor the            Recherches du Canada ni les noms
#  names of its contributors may        de ses  participants ne peuvent
#  be used to endorse or promote        être utilisés pour approuver ou
#  products derived from this           promouvoir les produits dérivés
#  software without specific prior      de ce logiciel sans autorisation
#  written permission.                  préalable et particulière
#                                       par écrit.
#
#  This file is part of the             Ce fichier fait partie du projet
#  OpenCADC project.                    OpenCADC.
#
#  OpenCADC is free software:           OpenCADC est un logiciel libre ;
#  you can redistribute it and/or       vous pouvez le redistribuer ou le
#  modify it under the terms of         modifier suivant les termes de
#  the GNU Affero General Public        la “GNU Affero General Public
#  License as published by the          License” telle que publiée
#  Free Software Foundation,            par la Free Software Foundation
#  either version 3 of the              : soit la version 3 de cette
#  License, or (at your option)         licence, soit (à votre gré)
#  any later version.                   toute version ultérieure.
#
#  OpenCADC is distributed in the       OpenCADC est distribué
#  hope that it will be useful,         dans l’espoir qu’il vous
#  but WITHOUT ANY WARRANTY;            sera utile, mais SANS AUCUNE
#  without even the implied             GARANTIE : sans même la garantie
#  warranty of MERCHANTABILITY          implicite de COMMERCIALISABILITÉ
#  or FITNESS FOR A PARTICULAR          ni d’ADÉQUATION À UN OBJECTIF
#  PURPOSE.  See the GNU Affero         PARTICULIER. Consultez la Licence
#  General Public License for           Générale Publique GNU Affero
#  more details.                        pour plus de détails.
#
#  You should have received             Vous devriez avoir reçu une
#  a copy of the GNU Affero             copie de la Licence Générale
#  General Public License along         Publique GNU Affero avec
#  with OpenCADC.  If not, see          OpenCADC ; si ce n’est
#  <http://www.gnu.org/licenses/>.      pas le cas, consultez :
#                                       <http://www.gnu.org/licenses/>.
#
#  $Revision: 4 $
#
# ***********************************************************************
#

import csv
import gzip
import importlib.util
import json
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Load the visitor from its file. Importing the metricsPipe package also
# imports log_visit, and that import needs pyarrow.
_VISITOR_PATH = Path(__file__).resolve().parents[1] / "sync_traffic_visit.py"
_SPEC = importlib.util.spec_from_file_location("sync_traffic_visit", _VISITOR_PATH)
_SYNC = importlib.util.module_from_spec(_SPEC)
sys.modules["sync_traffic_visit"] = _SYNC
_SPEC.loader.exec_module(_SYNC)

SyncTrafficError = _SYNC.SyncTrafficError
evaluate = _SYNC.evaluate
merge_path = _SYNC.merge_path
parse_lines = _SYNC.parse_lines
visit = _SYNC.visit
week_start = _SYNC.week_start


EPOCH = datetime(2024, 1, 1, tzinfo=timezone.utc)  # Monday


def at_week(index, days=0, **delta):
    return EPOCH + timedelta(days=7 * index + days, **delta)


def make(
    when,
    ip="10.0.0.1",
    auth="anon",
    success=True,
    duration=10,
    job="job",
    method="POST",
    path="/argus/sync",
    service="argus",
    phase="end",
    include_auth=True,
    include_duration=None,
):
    body = {
        "@timestamp": when.strftime("%Y-%m-%d %H:%M:%S.%f"),
        "service": {"name": service},
        "phase": phase,
        "ip": ip,
        "jobID": job,
        "method": method,
        "path": path,
        "success": success,
    }
    if include_auth:
        if auth is not None:
            body["authMethod"] = auth
    if include_duration is None:
        include_duration = success
    if include_duration:
        body["duration"] = duration
    return json.dumps(body)


def span_lines(n_full, skip=(), fill_ip="10.0.0.1"):
    lines = []
    for index in range(n_full):
        if index in skip:
            continue
        lines.append(
            make(at_week(index), ip=fill_ip, job=f"fill-{index}", duration=10)
        )
    lines.append(make(at_week(n_full), ip="10.255.255.255", job="edge", duration=1))
    return lines


def rows_for(result, week_index, auth):
    return [
        row
        for row in result.rows
        if row.week_index == week_index and row.auth_method == auth
    ]


class ParseTests(unittest.TestCase):
    def test_sample_line(self):
        raw = (
            '{"@timestamp":"2026-10-28 02:42:22.274","service":{"name":"argus"},'
            '"thread":{"name":"request-proc-2"},"log":{"level":"info"},"phase":"end",'
            '"ip":"132.246.195.66","jobID":"e97mky6hhksdfczl","method":"POST",'
            '"authMethod":"anon","path":"/argus/sync","success":true,"duration":1300}'
        )
        parsed = parse_lines([raw])
        self.assertEqual(len(parsed), 1)
        query = parsed[0]
        self.assertEqual(query.ip, "132.246.195.66")
        self.assertEqual(query.auth_method, "anon")
        self.assertTrue(query.success)
        self.assertEqual(query.duration, 1300)
        self.assertEqual(
            week_start(query.timestamp),
            datetime(2026, 10, 26, tzinfo=timezone.utc),
        )

    def test_filters_and_duplicate_job_lines(self):
        lines = span_lines(21)
        lines.extend(
            [
                make(at_week(3), method="GET", job="get"),
                make(at_week(3), path="/argus/sync/", job="slash"),
                make(at_week(3), phase="start", job="start"),
                make(at_week(3), service="other", job="other"),
                make(at_week(3), success="true", job="string-success", include_duration=False),
                '{"phase":"end","method":"POST","path":"/argus/sync","service":{"name":"argus"}}',
                make(at_week(3), job="same", ip="10.1.0.9"),
                make(at_week(3), job="same", ip="10.1.0.9"),
            ]
        )
        result = evaluate(parse_lines(lines))
        week = rows_for(result, 4, "anon")[0]
        # fill query plus two lines that share a jobID
        self.assertEqual(week.query_volume, 3)
        self.assertEqual(week.client_diversity, 2)

    def test_unspecified_auth_is_a_stratum(self):
        lines = span_lines(21)
        lines.append(make(at_week(0), auth=None, include_auth=False, job="missing", ip="10.2.0.1"))
        lines.append(make(at_week(0), auth="  ", job="blank", ip="10.2.0.2"))
        lines.append(
            make(
                at_week(0),
                auth=None,
                include_auth=False,
                success=False,
                include_duration=False,
                job="missing-fail",
                ip="10.2.0.3",
            )
        )
        result = evaluate(parse_lines(lines))
        week = rows_for(result, 1, "unspecified")[0]
        self.assertEqual(week.query_volume, 2)
        self.assertEqual(week.failure_volume, 1)

    def test_skips_a_line_that_is_not_json(self):
        parsed = parse_lines(["{not json", make(at_week(0), job="kept")])
        self.assertEqual(len(parsed), 1)
        self.assertEqual(parsed[0].line_no, 2)

    def test_rejects_bad_duration_and_missing_ip(self):
        with self.assertRaises(SyncTrafficError):
            parse_lines([make(at_week(0), include_duration=False)])
        with self.assertRaises(SyncTrafficError):
            parse_lines(
                [
                    json.dumps(
                        {
                            "@timestamp": "2024-01-01 00:00:00",
                            "service": {"name": "argus"},
                            "phase": "end",
                            "method": "POST",
                            "path": "/argus/sync",
                            "success": True,
                            "duration": 1,
                        }
                    )
                ]
            )


class WindowTests(unittest.TestCase):
    def test_stops_below_20_complete_weeks(self):
        result = evaluate(parse_lines(span_lines(10)))
        self.assertTrue(result.stopped)
        self.assertEqual(len(result.weeks), 10)
        self.assertIn("10 complete weeks", result.stop_reason)
        self.assertIn("needs 20", result.stop_reason)

    def test_holes_can_drop_the_window_below_20(self):
        result = evaluate(parse_lines(span_lines(25, skip=set(range(10, 20)))))
        self.assertTrue(result.stopped)
        self.assertEqual(len(result.holes), 10)
        self.assertEqual(len(result.weeks), 15)

    def test_partial_edge_is_outside_and_a_failure_only_week_stays(self):
        lines = []
        # Wednesday of week 0, so that week is partial.
        lines.append(make(at_week(0, days=2), ip="10.9.0.1", job="early"))
        for index in range(1, 22):
            if index == 5:
                lines.append(
                    make(
                        at_week(5),
                        success=False,
                        include_duration=False,
                        ip="10.9.0.2",
                        job="fail-only",
                    )
                )
            else:
                lines.append(make(at_week(index), job=f"fill-{index}"))
        lines.append(make(at_week(22), ip="10.255.255.255", job="edge"))
        # early address returns in calendar week 10
        lines.append(make(at_week(10, hours=3), ip="10.9.0.1", job="early-again"))
        result = evaluate(parse_lines(lines))
        self.assertFalse(result.stopped)
        self.assertEqual(result.weeks[0], at_week(1))
        self.assertEqual(len(result.weeks), 21)
        failure_week = rows_for(result, 5, "anon")[0]
        self.assertEqual(failure_week.query_volume, 0)
        self.assertEqual(failure_week.failure_volume, 1)
        self.assertIsNone(failure_week.intensity)
        # calendar week 10 is window week 10 (weeks start at calendar week 1)
        presence = rows_for(result, 10, "anon")[0]
        self.assertEqual(presence.new_address_presence, 1)
        self.assertEqual(presence.new_address_count, 0)
        self.assertFalse(any(row.ip == "10.9.0.1" for row in result.arrivals))

    def test_latest_78_does_not_reach_back_across_earlier_weeks(self):
        lines = span_lines(80)
        lines.append(
            make(
                at_week(0, hours=1),
                success=False,
                include_duration=False,
                ip="10.8.0.1",
                job="ancient",
            )
        )
        # 100 more failures would dominate a cutoff if week 0 were baseline
        for extra in range(99):
            lines.append(
                make(
                    at_week(0, hours=2),
                    success=False,
                    include_duration=False,
                    ip="10.8.0.1",
                    job=f"ancient-{extra}",
                )
            )
        lines.append(
            make(
                at_week(10),
                success=False,
                include_duration=False,
                ip="10.8.0.2",
                job="later",
            )
        )
        result = evaluate(parse_lines(lines))
        self.assertFalse(result.stopped)
        self.assertEqual(result.weeks[0], at_week(2))
        self.assertEqual(len(result.weeks), 78)
        cutoff = next(row for row in result.cutoffs if row.auth_method == "anon")
        self.assertEqual(cutoff.failure_concentration, 0)
        # calendar week 10 is window week 9
        scored = rows_for(result, 9, "anon")[0]
        self.assertEqual(scored.high_failure_addresses, ("10.8.0.2",))

    def test_sustained_failure_crosses_a_hole(self):
        lines = span_lines(24, skip={10})
        for calendar in (8, 9, 11, 12):
            lines.append(
                make(
                    at_week(calendar, hours=4),
                    success=False,
                    include_duration=False,
                    ip="10.4.0.1",
                    job=f"fail-{calendar}",
                )
            )
        result = evaluate(parse_lines(lines))
        self.assertNotIn(at_week(10), result.weeks)
        self.assertIn(at_week(10), result.holes)
        # numbered weeks: calendar 8, 9, 11, 12 are four successive window weeks
        self.assertEqual(result.weeks[8], at_week(8))
        self.assertEqual(result.weeks[9], at_week(9))
        self.assertEqual(result.weeks[10], at_week(11))
        self.assertEqual(result.weeks[11], at_week(12))
        self.assertFalse(rows_for(result, 11, "anon")[0].sustained_failure)
        sustained = rows_for(result, 12, "anon")[0]
        self.assertTrue(sustained.sustained_failure)
        self.assertEqual(sustained.sustained_failure_addresses, ("10.4.0.1",))
        self.assertIn("high failure concentration", sustained.agent_like_signatures)


class SignatureTests(unittest.TestCase):
    def test_intensity_cutoff_spike_and_sustained_run(self):
        lines = span_lines(21)
        for calendar in (8, 9, 10, 11):
            lines.append(make(at_week(calendar, hours=1), job=f"a-{calendar}", duration=10))
            lines.append(make(at_week(calendar, hours=2), job=f"b-{calendar}", duration=10))
        result = evaluate(parse_lines(lines))
        cutoff = next(row for row in result.cutoffs if row.auth_method == "anon")
        self.assertEqual(cutoff.intensity, 1)
        week_9 = rows_for(result, 9, "anon")[0]
        week_11 = rows_for(result, 11, "anon")[0]
        week_12 = rows_for(result, 12, "anon")[0]
        self.assertEqual(week_9.intensity, 3)
        self.assertTrue(week_9.high_intensity)
        self.assertFalse(week_11.sustained_intensity)
        self.assertTrue(week_12.sustained_intensity)
        self.assertEqual(week_12.agent_like_signatures, ("high intensity",))
        # a later week equal to the baseline maximum is not high
        week_13 = rows_for(result, 13, "anon")[0]
        self.assertEqual(week_13.intensity, 1)
        self.assertFalse(week_13.high_intensity)
        self.assertIsNone(rows_for(result, 8, "anon")[0].high_intensity)

    def test_undefined_intensity_is_not_scored_when_baseline_never_defines_it(self):
        lines = span_lines(21)
        lines.append(
            make(
                at_week(0, hours=5),
                auth="cert",
                success=False,
                include_duration=False,
                ip="10.3.0.1",
                job="cert-fail",
            )
        )
        lines.append(
            make(at_week(10), auth="cert", ip="10.3.0.1", job="cert-ok", duration=5)
        )
        result = evaluate(parse_lines(lines))
        cert_cutoff = next(row for row in result.cutoffs if row.auth_method == "cert")
        self.assertIsNone(cert_cutoff.intensity)
        self.assertIsNone(rows_for(result, 11, "cert")[0].high_intensity)
        self.assertEqual(rows_for(result, 1, "cert")[0].query_volume, 0)

    def test_failure_cutoff_of_zero_and_same_address(self):
        lines = span_lines(21)
        for calendar, count in ((0, 2), (8, 2), (9, 3)):
            for nth in range(count):
                lines.append(
                    make(
                        at_week(calendar, hours=nth + 1),
                        success=False,
                        include_duration=False,
                        ip="10.6.0.1",
                        job=f"noisy-{calendar}-{nth}",
                    )
                )
        lines.append(
            make(
                at_week(9, hours=6),
                success=False,
                include_duration=False,
                ip="10.6.0.2",
                job="other",
            )
        )
        result = evaluate(parse_lines(lines))
        cutoff = next(row for row in result.cutoffs if row.auth_method == "anon")
        self.assertEqual(cutoff.failure_concentration, 2)
        equal_week = rows_for(result, 9, "anon")[0]
        self.assertFalse(equal_week.high_failure)
        above = rows_for(result, 10, "anon")[0]
        self.assertEqual(above.high_failure_addresses, ("10.6.0.1",))
        self.assertNotIn("10.6.0.2", above.high_failure_addresses)

    def test_quiet_stratum_week_breaks_a_failure_run(self):
        lines = span_lines(21)
        for calendar in (8, 9, 10, 12, 13, 14, 15):
            lines.append(
                make(
                    at_week(calendar, hours=3),
                    auth="cert",
                    success=False,
                    include_duration=False,
                    ip="10.7.0.1",
                    job=f"cert-{calendar}",
                )
            )
        result = evaluate(parse_lines(lines))
        self.assertEqual(rows_for(result, 12, "cert")[0].query_volume, 0)
        self.assertEqual(rows_for(result, 12, "cert")[0].failure_volume, 0)
        self.assertFalse(rows_for(result, 11, "cert")[0].sustained_failure)
        self.assertFalse(rows_for(result, 12, "cert")[0].sustained_failure)
        sustained = rows_for(result, 16, "cert")[0]
        self.assertEqual(sustained.sustained_failure_addresses, ("10.7.0.1",))
        self.assertFalse(rows_for(result, 15, "cert")[0].sustained_failure)

    def test_new_addresses_count_once_and_presence_keeps_later_weeks(self):
        lines = span_lines(24)
        # Seen under cert during the baseline, so a later anon query is not a new address.
        lines.append(make(at_week(0, hours=2), auth="cert", ip="10.5.0.9", job="known"))
        lines.append(make(at_week(20, hours=2), auth="anon", ip="10.5.0.9", job="known-later"))
        # Two arrivals in the last calibration week. The signature is not scored there,
        # and their count becomes the cutoff the later weeks have to beat.
        for nth in (1, 2):
            lines.append(
                make(at_week(19, hours=nth), ip=f"10.5.1.{nth}", job=f"cal-{nth}", duration=7)
            )
        fresh = "10.5.0.1"
        # Same timestamp: the cert line is first in the file, so it is the arrival.
        lines.append(make(at_week(20), ip=fresh, auth="cert", job="tie-cert", duration=10))
        lines.append(make(at_week(20), ip=fresh, auth="anon", job="tie-anon", duration=20))
        lines.append(
            make(
                at_week(20, minutes=5),
                ip=fresh,
                auth="cert",
                success=False,
                include_duration=False,
                job="tie-fail",
            )
        )
        lines.append(make(at_week(21, hours=1), ip=fresh, auth="anon", job="again", duration=4))
        for calendar in range(20, 24):
            for nth in range(3):
                lines.append(
                    make(
                        at_week(calendar, hours=3 + nth),
                        ip=f"10.5.{calendar}.{nth}",
                        job=f"wave-{calendar}-{nth}",
                        duration=3,
                    )
                )
        result = evaluate(parse_lines(lines))

        cutoff = next(row for row in result.cutoffs if row.auth_method == "anon")
        self.assertEqual(cutoff.new_address_count, 2)
        calibration_week = rows_for(result, 20, "anon")[0]
        self.assertEqual(calibration_week.new_address_count, 2)
        self.assertIsNone(calibration_week.many_new)

        arrival = next(row for row in result.arrivals if row.ip == fresh)
        self.assertEqual(arrival.auth_method, "cert")
        self.assertEqual(arrival.week_index, 21)
        self.assertEqual(arrival.query_volume, 1)
        self.assertEqual(arrival.failure_volume, 1)
        self.assertEqual(arrival.service_load, 10)

        cert_21 = rows_for(result, 21, "cert")[0]
        self.assertEqual(cert_21.new_address_count, 1)
        self.assertEqual(cert_21.new_address_presence, 1)
        self.assertTrue(cert_21.many_new)
        self.assertFalse(cert_21.sustained_many_new)
        anon_21 = rows_for(result, 21, "anon")[0]
        self.assertEqual(anon_21.new_address_count, 3)
        self.assertEqual(anon_21.new_address_presence, 4)
        self.assertTrue(anon_21.many_new)
        self.assertFalse(anon_21.sustained_many_new)
        self.assertEqual(rows_for(result, 22, "anon")[0].new_address_presence, 4)
        self.assertEqual(rows_for(result, 22, "anon")[0].new_address_count, 3)
        self.assertFalse(any(row.ip == "10.5.0.9" for row in result.arrivals))

        week_24 = rows_for(result, 24, "anon")[0]
        self.assertTrue(week_24.sustained_many_new)
        self.assertIn("many new source addresses", week_24.agent_like_signatures)
        self.assertFalse(rows_for(result, 23, "anon")[0].sustained_many_new)

    def test_arrival_shares_and_duration_shape(self):
        lines = span_lines(21, skip={0, 20})
        lines.append(make(at_week(0), job="shape-10", duration=10))
        for duration in (20, 30, 40):
            lines.append(
                make(at_week(0, minutes=duration), job=f"shape-{duration}", duration=duration)
            )
        lines.append(make(at_week(20), ip="10.0.0.1", job="resident", duration=50))
        for duration in (10, 20, 30, 40):
            lines.append(
                make(
                    at_week(20, minutes=duration),
                    ip="10.5.1.1",
                    job=f"fresh-{duration}",
                    duration=duration,
                )
            )
        result = evaluate(parse_lines(lines))
        baseline = rows_for(result, 1, "anon")[0]
        self.assertEqual(baseline.service_load, 100)
        self.assertEqual(baseline.duration_median, 25)
        self.assertEqual(baseline.duration_p95, 38.5)
        scored = rows_for(result, 21, "anon")[0]
        self.assertEqual(scored.arrival_query_volume, 4)
        self.assertEqual(scored.query_volume, 5)
        self.assertEqual(scored.arrival_share_query_volume, 0.8)
        self.assertAlmostEqual(scored.arrival_share_service_load, 100 / 150)
        self.assertEqual(scored.new_address_count, 1)


class TallyTests(unittest.TestCase):
    def test_writes_reports_and_stops_cleanly(self):
        with tempfile.TemporaryDirectory() as tmp:
            short = Path(tmp) / "short.jsonl"
            short.write_text("\n".join(span_lines(8)) + "\n", encoding="utf-8")
            short_out = Path(tmp) / "short-out"
            (short_out / "weekly.csv").parent.mkdir()
            (short_out / "weekly.csv").write_text("stale\n", encoding="utf-8")
            self.assertEqual(merge_path(str(short_out), str(short)), "merged")
            empty = Path(tmp) / "empty.jsonl"
            empty.write_text("", encoding="utf-8")
            self.assertEqual(merge_path(str(short_out), str(empty)), "merged")
            self.assertEqual(merge_path(str(short_out), str(empty)), "skipped")
            summary = (short_out / "summary.txt").read_text(encoding="utf-8")
            self.assertIn("Stopped:", summary)
            self.assertFalse((short_out / "weekly.csv").exists())

            good = Path(tmp) / "good.jsonl"
            good.write_text("\n".join(span_lines(21)) + "\n", encoding="utf-8")
            good_out = Path(tmp) / "good-out"
            self.assertEqual(merge_path(str(good_out), str(good)), "merged")
            with (good_out / "weekly.csv").open(encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                first = next(reader)
            self.assertEqual(first["week_index"], "1")
            self.assertEqual(first["auth_method"], "anon")
            self.assertEqual(first["high_intensity"], "not_scored")
            self.assertIn("Agent-like ranges", (good_out / "summary.txt").read_text(encoding="utf-8"))

            broken = Path(tmp) / "broken.jsonl"
            broken.write_text("{nope\n", encoding="utf-8")
            broken_out = Path(tmp) / "bad-out"
            self.assertEqual(merge_path(str(broken_out), str(broken)), "merged")
            self.assertIn("Stopped:", (broken_out / "summary.txt").read_text(encoding="utf-8"))

    def test_two_files_accumulate_and_a_repeat_does_not_double_count(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            first = root / "a.jsonl"
            second = root / "b.jsonl"
            text = "\n".join(span_lines(21)) + "\n"
            first.write_text(text, encoding="utf-8")
            second.write_text(text, encoding="utf-8")
            tally = root / "tally"
            merge_path(str(tally), str(first))
            merge_path(str(tally), str(second))
            self.assertEqual(merge_path(str(tally), str(first)), "skipped")
            with (tally / "weekly.csv").open(encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            week = next(row for row in rows if row["week_index"] == "1")
            self.assertEqual(week["query_volume"], "2")
            self.assertEqual(week["client_diversity"], "1")

    def test_changed_file_replaces_its_slice(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "one.jsonl"
            source.write_text("\n".join(span_lines(21)) + "\n", encoding="utf-8")
            tally = root / "tally"
            merge_path(str(tally), str(source))
            extra = make(at_week(0, hours=1), job="added", duration=10)
            with source.open("a", encoding="utf-8") as handle:
                handle.write(extra + "\n")
            self.assertEqual(merge_path(str(tally), str(source)), "merged")
            with (tally / "weekly.csv").open(encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            week = next(row for row in rows if row["week_index"] == "1")
            self.assertEqual(week["query_volume"], "2")

    def test_lexical_path_decides_an_equal_timestamp(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            later_name = root / "b.jsonl"
            earlier_name = root / "a.jsonl"
            fresh = "10.5.0.1"
            when = at_week(20)
            later_name.write_text(
                make(when, ip=fresh, auth="cert", job="tie-cert", duration=10) + "\n",
                encoding="utf-8",
            )
            earlier_name.write_text(
                "\n".join(span_lines(24))
                + "\n"
                + make(when, ip=fresh, auth="anon", job="tie-anon", duration=20)
                + "\n",
                encoding="utf-8",
            )
            tally = root / "tally"
            merge_path(str(tally), str(later_name))
            merge_path(str(tally), str(earlier_name))
            with (tally / "arrivals.csv").open(encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            arrival = next(row for row in rows if row["ip"] == fresh)
            self.assertEqual(arrival["auth_method"], "anon")
            self.assertEqual(arrival["week_index"], "21")

    def test_gzip_is_read_as_text(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "week.jsonl.gz"
            payload = ("\n".join(span_lines(21)) + "\n").encode("utf-8")
            with gzip.open(source, "wb") as handle:
                handle.write(payload)
            tally = root / "tally"
            merge_path(str(tally), str(source))
            with (tally / "weekly.csv").open(encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            week = next(row for row in rows if row["week_index"] == "1")
            self.assertEqual(week["query_volume"], "1")

    def test_a_bad_line_does_not_drop_the_other_queries(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "one.jsonl"
            lines = span_lines(21)
            lines.insert(3, "{nope")
            source.write_text("\n".join(lines) + "\n", encoding="utf-8")
            tally = root / "tally"
            self.assertEqual(merge_path(str(tally), str(source)), "merged")
            with (tally / "weekly.csv").open(encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            week = next(row for row in rows if row["week_index"] == "1")
            self.assertEqual(week["query_volume"], "1")
            summary = (tally / "summary.txt").read_text(encoding="utf-8")
            self.assertIn("complete weeks: 21", summary)

    def test_visit_writes_under_the_working_directory(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "good.jsonl"
            source.write_text("\n".join(span_lines(21)) + "\n", encoding="utf-8")

            class Config:
                def __init__(self, directory):
                    self.working_directory = directory

            class StorageName:
                def __init__(self, names):
                    self.source_names = names

            visit(
                config=Config(str(root / "work")),
                storage_name=StorageName([str(source)]),
            )
            summary = root / "work" / "argus_sync_tally" / "summary.txt"
            self.assertTrue(summary.is_file())
            self.assertIn("complete weeks: 21", summary.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
