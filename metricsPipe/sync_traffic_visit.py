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

"""Argus sync-traffic visitor.

Each visit merges one log file into a slice under
``{working_directory}/argus_sync_tally``. When the file's size and
modification time are unchanged, the slice is left in place. Otherwise the
slice is replaced and the evaluation report is rewritten from every slice.
"""

from __future__ import annotations

import csv
import gzip
import hashlib
import json
import logging
import math
import os
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Iterable


logger = logging.getLogger(__name__)

UNSPECIFIED = 'unspecified'
TALLY_DIR_NAME = 'argus_sync_tally'
MAX_CALENDAR_WEEKS = 78
MIN_COMPLETE_WEEKS = 20
BASELINE_LAST = 8
CALIBRATION_LAST = 20
INTENSITY_SCORE_FROM = 9
NEW_ADDRESS_SCORE_FROM = 21
SUSTAINED_LENGTH = 4

SIG_INTENSITY = 'high intensity'
SIG_NEW = 'many new source addresses'
SIG_FAILURE = 'high failure concentration'

WEEKLY_FIELDS = (
    'week_index',
    'week_start',
    'auth_method',
    'query_volume',
    'failure_volume',
    'service_load',
    'duration_median',
    'duration_p95',
    'client_diversity',
    'intensity',
    'new_address_count',
    'new_address_presence',
    'arrival_query_volume',
    'arrival_failure_volume',
    'arrival_service_load',
    'arrival_share_query_volume',
    'arrival_share_service_load',
    'high_intensity',
    'many_new_source_addresses',
    'high_failure_concentration',
    'high_failure_addresses',
    'sustained_high_intensity',
    'sustained_many_new_source_addresses',
    'sustained_high_failure_concentration',
    'sustained_failure_addresses',
    'agent_like',
    'agent_like_signatures',
)

ARRIVAL_FIELDS = (
    'week_index',
    'week_start',
    'auth_method',
    'ip',
    'arrival_query_volume',
    'arrival_failure_volume',
    'arrival_service_load',
)

CUTOFF_FIELDS = (
    'auth_method',
    'intensity_cutoff',
    'failure_concentration_cutoff',
    'new_address_count_cutoff',
)


class SyncTrafficError(Exception):
    """A log file or tally cannot be evaluated."""


@dataclass(frozen=True)
class SyncQuery:
    line_no: int
    timestamp: datetime
    ip: str
    auth_method: str
    success: bool
    duration: int | float | None


@dataclass(frozen=True)
class FirstCompleted:
    timestamp: datetime
    path: str
    line_no: int
    auth_method: str

    def sort_key(self):
        return (self.timestamp, self.path, self.line_no)


@dataclass
class Cell:
    completed: int = 0
    failed: int = 0
    duration_sum: int | float = 0


@dataclass
class Arrival:
    week_index: int
    week_start: datetime
    auth_method: str
    ip: str
    query_volume: int
    failure_volume: int
    service_load: int | float


@dataclass
class WeekRow:
    week_index: int
    week_start: datetime
    auth_method: str
    query_volume: int
    failure_volume: int
    service_load: int | float
    duration_median: int | float | None
    duration_p95: int | float | None
    client_diversity: int
    intensity: int | float | None
    new_address_count: int
    new_address_presence: int
    arrival_query_volume: int
    arrival_failure_volume: int
    arrival_service_load: int | float
    arrival_share_query_volume: int | float | None
    arrival_share_service_load: int | float | None
    high_intensity: bool | None
    many_new: bool | None
    high_failure: bool | None
    high_failure_addresses: tuple[str, ...]
    sustained_intensity: bool
    sustained_many_new: bool
    sustained_failure: bool
    sustained_failure_addresses: tuple[str, ...]

    @property
    def agent_like_signatures(self) -> tuple[str, ...]:
        names = []
        if self.sustained_intensity:
            names.append(SIG_INTENSITY)
        if self.sustained_many_new:
            names.append(SIG_NEW)
        if self.sustained_failure:
            names.append(SIG_FAILURE)
        return tuple(names)


@dataclass
class Cutoff:
    auth_method: str
    intensity: int | float | None
    failure_concentration: int
    new_address_count: int


@dataclass
class Evaluation:
    queries_read: int = 0
    log_start: datetime | None = None
    log_end: datetime | None = None
    calendar_weeks: int = 0
    considered_weeks: int = 0
    holes: list[datetime] = field(default_factory=list)
    weeks: list[datetime] = field(default_factory=list)
    rows: list[WeekRow] = field(default_factory=list)
    arrivals: list[Arrival] = field(default_factory=list)
    cutoffs: list[Cutoff] = field(default_factory=list)
    stop_reason: str | None = None

    @property
    def stopped(self) -> bool:
        return self.stop_reason is not None


def parse_timestamp(value: object, line_no: int) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise SyncTrafficError(f'line {line_no}: missing @timestamp')
    text = value.strip()
    if text.endswith('Z'):
        text = text[:-1] + '+00:00'
    for fmt in ('%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S'):
        try:
            parsed = datetime.strptime(text, fmt)
        except ValueError:
            continue
        return parsed.replace(tzinfo=timezone.utc)
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise SyncTrafficError(f'line {line_no}: unrecognized @timestamp {value!r}') from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def week_start(ts: datetime) -> datetime:
    utc = ts.astimezone(timezone.utc)
    midnight = datetime(utc.year, utc.month, utc.day, tzinfo=timezone.utc)
    return midnight - timedelta(days=utc.weekday())


def as_duration(value: object, line_no: int) -> int | float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise SyncTrafficError(f'line {line_no}: completed sync query needs a numeric duration')
    if isinstance(value, float) and not math.isfinite(value):
        raise SyncTrafficError(f'line {line_no}: duration is not finite')
    if value < 0:
        raise SyncTrafficError(f'line {line_no}: duration is negative')
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return value


def auth_method_of(raw: object, line_no: int) -> str:
    if raw is None:
        return UNSPECIFIED
    if not isinstance(raw, str):
        raise SyncTrafficError(f'line {line_no}: authMethod is not a string')
    if raw.strip() == '':
        return UNSPECIFIED
    return raw.strip()


def parse_lines(lines: Iterable[str]) -> list[SyncQuery]:
    queries: list[SyncQuery] = []
    for line_no, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            logger.warning('line %s: %s', line_no, exc.msg)
            continue
        if not isinstance(record, dict):
            raise SyncTrafficError(f'line {line_no}: log line is not a JSON object')
        query = sync_query_from(record, line_no)
        if query is not None:
            queries.append(query)
    return queries


def sync_query_from(record: dict, line_no: int) -> SyncQuery | None:
    service = record.get('service')
    service_name = service.get('name') if isinstance(service, dict) else None
    if (
        service_name != 'argus'
        or record.get('method') != 'POST'
        or record.get('path') != '/argus/sync'
        or record.get('phase') != 'end'
    ):
        return None
    success = record.get('success')
    if not isinstance(success, bool):
        return None
    ip = record.get('ip')
    if not isinstance(ip, str) or ip.strip() == '':
        raise SyncTrafficError(f'line {line_no}: sync query needs an ip')
    duration = None
    if success:
        if 'duration' not in record:
            raise SyncTrafficError(f'line {line_no}: completed sync query needs a numeric duration')
        duration = as_duration(record.get('duration'), line_no)
    return SyncQuery(
        line_no=line_no,
        timestamp=parse_timestamp(record.get('@timestamp'), line_no),
        ip=ip.strip(),
        auth_method=auth_method_of(record.get('authMethod'), line_no),
        success=success,
        duration=duration,
    )


def read_log_lines(path: str) -> list[str]:
    opener = gzip.open if path.endswith('.gz') else open
    with opener(path, 'rt', encoding='utf-8') as handle:
        return list(handle)


def full_calendar_weeks(start: datetime, end: datetime) -> list[datetime]:
    first = week_start(start)
    if start > first:
        first += timedelta(days=7)
    weeks = []
    cursor = first
    while cursor + timedelta(days=7) <= end:
        weeks.append(cursor)
        cursor += timedelta(days=7)
    return weeks


def percentile(values: list[int | float], fraction: float) -> int | float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * fraction
    low = math.floor(position)
    high = math.ceil(position)
    if low == high:
        return ordered[low]
    weight = position - low
    return ordered[low] * (1 - weight) + ordered[high] * weight


def sustained_flags(high: list[bool]) -> list[bool]:
    flags = []
    for index in range(len(high)):
        if index < SUSTAINED_LENGTH - 1:
            flags.append(False)
            continue
        flags.append(all(high[index - SUSTAINED_LENGTH + 1 : index + 1]))
    return flags


def sustained_addresses(weekly_high: list[set[str]]) -> list[set[str]]:
    chosen: list[set[str]] = []
    for index in range(len(weekly_high)):
        if index < SUSTAINED_LENGTH - 1:
            chosen.append(set())
            continue
        run = weekly_high[index - SUSTAINED_LENGTH + 1 : index + 1]
        chosen.append(set.intersection(*run))
    return chosen


def _stamp(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).isoformat()


def _parse_stamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def slice_from_queries(path: str, size: int, mtime_ns: int, queries: list[SyncQuery]) -> dict:
    cells: dict[tuple[str, str, str], list] = {}
    durations: dict[tuple[str, str], list] = defaultdict(list)
    first: dict[str, FirstCompleted] = {}
    log_start = None
    log_end = None
    for query in queries:
        if log_start is None or query.timestamp < log_start:
            log_start = query.timestamp
        if log_end is None or query.timestamp > log_end:
            log_end = query.timestamp
        week = format_day(week_start(query.timestamp))
        key = (week, query.auth_method, query.ip)
        cell = cells.setdefault(key, [0, 0, 0])
        if query.success:
            cell[0] += 1
            cell[2] += query.duration
            durations[(week, query.auth_method)].append(query.duration)
            candidate = FirstCompleted(query.timestamp, path, query.line_no, query.auth_method)
            current = first.get(query.ip)
            if current is None or candidate.sort_key() < current.sort_key():
                first[query.ip] = candidate
        else:
            cell[1] += 1
    return {
        'path': path,
        'size': size,
        'mtime_ns': mtime_ns,
        'sync_queries': len(queries),
        'log_start': _stamp(log_start) if log_start else None,
        'log_end': _stamp(log_end) if log_end else None,
        'first': {
            ip: {
                'timestamp': _stamp(item.timestamp),
                'line_no': item.line_no,
                'auth_method': item.auth_method,
            }
            for ip, item in first.items()
        },
        'cells': [
            {
                'week': week,
                'auth_method': auth,
                'ip': ip,
                'completed': completed,
                'failed': failed,
                'duration_sum': duration_sum,
            }
            for (week, auth, ip), (completed, failed, duration_sum) in cells.items()
        ],
        'durations': [
            {'week': week, 'auth_method': auth, 'values': values}
            for (week, auth), values in durations.items()
        ],
    }


def tally_directory(working_directory: str) -> str:
    return os.path.join(working_directory, TALLY_DIR_NAME)


def _slice_dir(tally_dir: str) -> str:
    return os.path.join(tally_dir, 'slices')


def _index_path(tally_dir: str) -> str:
    return os.path.join(_slice_dir(tally_dir), 'index.json')


def _slice_filename(path: str) -> str:
    return hashlib.sha256(path.encode('utf-8')).hexdigest() + '.json'


def _read_json(path: str) -> dict:
    with open(path, encoding='utf-8') as handle:
        return json.load(handle)


def _write_json(path: str, payload: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    temporary = path + '.tmp'
    with open(temporary, 'w', encoding='utf-8') as handle:
        json.dump(payload, handle, separators=(',', ':'))
    os.replace(temporary, path)


def _load_index(tally_dir: str) -> dict:
    path = _index_path(tally_dir)
    if not os.path.exists(path):
        return {'paths': {}}
    return _read_json(path)


def file_stat(path: str) -> tuple[int, int]:
    info = os.stat(path)
    return info.st_size, info.st_mtime_ns


def merge_path(tally_dir: str, path: str) -> str:
    """Merge one log file. Return ``skipped`` or ``merged``.

    A sync-query failure is raised before the stored slice is replaced.
    """
    size, mtime_ns = file_stat(path)
    index = _load_index(tally_dir)
    known = index['paths'].get(path)
    if known and known['size'] == size and known['mtime_ns'] == mtime_ns:
        return 'skipped'
    queries = parse_lines(read_log_lines(path))
    payload = slice_from_queries(path, size, mtime_ns, queries)
    os.makedirs(_slice_dir(tally_dir), exist_ok=True)
    _write_json(os.path.join(_slice_dir(tally_dir), _slice_filename(path)), payload)
    rewrite_report(tally_dir)
    index['paths'][path] = {'file': _slice_filename(path), 'size': size, 'mtime_ns': mtime_ns}
    _write_json(_index_path(tally_dir), index)
    return 'merged'


def load_slices(tally_dir: str) -> list[dict]:
    directory = _slice_dir(tally_dir)
    if not os.path.isdir(directory):
        return []
    payloads = []
    for name in sorted(os.listdir(directory)):
        if not name.endswith('.json') or name == 'index.json':
            continue
        payloads.append(_read_json(os.path.join(directory, name)))
    return payloads


def evaluate_slices(payloads: list[dict]) -> Evaluation:
    usable = [item for item in payloads if item.get('log_start')]
    result = Evaluation(queries_read=sum(item.get('sync_queries', 0) for item in payloads))
    if not usable:
        result.stop_reason = 'No sync queries in the input.'
        return result
    result.log_start = min(_parse_stamp(item['log_start']) for item in usable)
    result.log_end = max(_parse_stamp(item['log_end']) for item in usable)

    cells: dict[tuple[datetime, str, str], Cell] = {}
    durations: dict[tuple[datetime, str], list] = defaultdict(list)
    first: dict[str, FirstCompleted] = {}
    for item in usable:
        for raw in item['cells']:
            start = datetime.strptime(raw['week'], '%Y-%m-%d').replace(tzinfo=timezone.utc)
            key = (start, raw['auth_method'], raw['ip'])
            cell = cells.setdefault(key, Cell())
            cell.completed += raw['completed']
            cell.failed += raw['failed']
            cell.duration_sum += raw['duration_sum']
        for raw in item['durations']:
            start = datetime.strptime(raw['week'], '%Y-%m-%d').replace(tzinfo=timezone.utc)
            durations[(start, raw['auth_method'])].extend(raw['values'])
        for ip, raw in item['first'].items():
            candidate = FirstCompleted(
                _parse_stamp(raw['timestamp']),
                item['path'],
                raw['line_no'],
                raw['auth_method'],
            )
            current = first.get(ip)
            if current is None or candidate.sort_key() < current.sort_key():
                first[ip] = candidate
    return _evaluate_cells(result, cells, durations, first)


def _evaluate_cells(result, cells, durations, first) -> Evaluation:
    calendar = full_calendar_weeks(result.log_start, result.log_end)
    result.calendar_weeks = len(calendar)
    considered = calendar[-MAX_CALENDAR_WEEKS:]
    result.considered_weeks = len(considered)
    occupied = {start for (start, _auth, _ip), cell in cells.items() if cell.completed or cell.failed}
    result.holes = [start for start in considered if start not in occupied]
    window = [start for start in considered if start in occupied]
    result.weeks = window
    if len(window) < MIN_COMPLETE_WEEKS:
        result.stop_reason = (
            f'{len(window)} complete weeks remain in the latest '
            f'{len(considered)} calendar weeks inside the log span. '
            f'The evaluation needs {MIN_COMPLETE_WEEKS}.'
        )
        return result

    baseline = set(window[:BASELINE_LAST])
    calibration = window[BASELINE_LAST:CALIBRATION_LAST]
    baseline_ips = {
        ip
        for (start, _auth, ip), cell in cells.items()
        if start in baseline and cell.completed
    }
    strata = sorted({auth for (start, auth, _ip) in cells if start in set(window)})

    def cells_for(start, stratum):
        return {ip: cell for (week, auth, ip), cell in cells.items() if week == start and auth == stratum}

    series = {}
    intensity = {}
    new_counts = {}
    failure_counts = {}
    for start in window:
        for stratum in strata:
            matched = cells_for(start, stratum)
            completed_n = sum(cell.completed for cell in matched.values())
            failed_n = sum(cell.failed for cell in matched.values())
            duration_values = durations.get((start, stratum), [])
            diversity = sum(1 for cell in matched.values() if cell.completed)
            load = sum(duration_values)
            week_intensity = (completed_n / diversity) if diversity else None
            presence = set()
            arrival_ips = set()
            for ip, cell in matched.items():
                if not cell.completed or ip in baseline_ips:
                    continue
                presence.add(ip)
                opening = first[ip]
                if week_start(opening.timestamp) == start and opening.auth_method == stratum:
                    arrival_ips.add(ip)
            arrival_completed = sum(matched[ip].completed for ip in arrival_ips)
            arrival_failed = sum(matched[ip].failed for ip in arrival_ips)
            arrival_load = sum(matched[ip].duration_sum for ip in arrival_ips)
            per_address_failures = {ip: cell.failed for ip, cell in matched.items() if cell.failed}
            intensity[(start, stratum)] = week_intensity
            new_counts[(start, stratum)] = len(arrival_ips)
            failure_counts[(start, stratum)] = per_address_failures
            series[(start, stratum)] = {
                'query_volume': completed_n,
                'failure_volume': failed_n,
                'service_load': load,
                'duration_median': percentile(duration_values, 0.5),
                'duration_p95': percentile(duration_values, 0.95),
                'client_diversity': diversity,
                'intensity': week_intensity,
                'new_address_count': len(arrival_ips),
                'new_address_presence': len(presence),
                'arrival_query_volume': arrival_completed,
                'arrival_failure_volume': arrival_failed,
                'arrival_service_load': arrival_load,
                'arrival_share_query_volume': (arrival_completed / completed_n) if completed_n else None,
                'arrival_share_service_load': (arrival_load / load) if load else None,
                'arrival_ips': arrival_ips,
                'matched': matched,
            }

    cutoffs = []
    high_intensity = {}
    many_new = {}
    high_failure = {}
    for stratum in strata:
        defined = [
            intensity[(start, stratum)]
            for start in window[:BASELINE_LAST]
            if intensity[(start, stratum)] is not None
        ]
        intensity_cutoff = max(defined) if defined else None
        failure_cutoff = 0
        for start in window[:BASELINE_LAST]:
            for count in failure_counts[(start, stratum)].values():
                failure_cutoff = max(failure_cutoff, count)
        new_cutoff = max(new_counts[(start, stratum)] for start in calibration)
        cutoffs.append(
            Cutoff(
                auth_method=stratum,
                intensity=intensity_cutoff,
                failure_concentration=failure_cutoff,
                new_address_count=new_cutoff,
            )
        )
        intensity_high = []
        new_high = []
        failure_high = []
        for index, start in enumerate(window, start=1):
            if index < INTENSITY_SCORE_FROM or intensity_cutoff is None:
                intensity_high.append(False)
            else:
                value = intensity[(start, stratum)]
                intensity_high.append(value is not None and value > intensity_cutoff)
            if index < NEW_ADDRESS_SCORE_FROM:
                new_high.append(False)
            else:
                new_high.append(new_counts[(start, stratum)] > new_cutoff)
            if index < INTENSITY_SCORE_FROM:
                failure_high.append(set())
            else:
                failure_high.append(
                    {ip for ip, count in failure_counts[(start, stratum)].items() if count > failure_cutoff}
                )
        high_intensity[stratum] = intensity_high
        many_new[stratum] = new_high
        high_failure[stratum] = failure_high

    sustained_intensity = {stratum: sustained_flags(high_intensity[stratum]) for stratum in strata}
    sustained_new = {stratum: sustained_flags(many_new[stratum]) for stratum in strata}
    sustained_failure = {stratum: sustained_addresses(high_failure[stratum]) for stratum in strata}

    rows = []
    arrivals_out = []
    for index, start in enumerate(window):
        for stratum in strata:
            item = series[(start, stratum)]
            failure_set = high_failure[stratum][index]
            sustained_set = sustained_failure[stratum][index]
            cutoff = next(row for row in cutoffs if row.auth_method == stratum)
            scored_intensity = not (index + 1 < INTENSITY_SCORE_FROM or cutoff.intensity is None)
            scored_new = index + 1 >= NEW_ADDRESS_SCORE_FROM
            scored_failure = index + 1 >= INTENSITY_SCORE_FROM
            rows.append(
                WeekRow(
                    week_index=index + 1,
                    week_start=start,
                    auth_method=stratum,
                    query_volume=item['query_volume'],
                    failure_volume=item['failure_volume'],
                    service_load=item['service_load'],
                    duration_median=item['duration_median'],
                    duration_p95=item['duration_p95'],
                    client_diversity=item['client_diversity'],
                    intensity=item['intensity'],
                    new_address_count=item['new_address_count'],
                    new_address_presence=item['new_address_presence'],
                    arrival_query_volume=item['arrival_query_volume'],
                    arrival_failure_volume=item['arrival_failure_volume'],
                    arrival_service_load=item['arrival_service_load'],
                    arrival_share_query_volume=item['arrival_share_query_volume'],
                    arrival_share_service_load=item['arrival_share_service_load'],
                    high_intensity=high_intensity[stratum][index] if scored_intensity else None,
                    many_new=many_new[stratum][index] if scored_new else None,
                    high_failure=bool(failure_set) if scored_failure else None,
                    high_failure_addresses=tuple(sorted(failure_set)) if scored_failure else (),
                    sustained_intensity=sustained_intensity[stratum][index],
                    sustained_many_new=sustained_new[stratum][index],
                    sustained_failure=bool(sustained_set),
                    sustained_failure_addresses=tuple(sorted(sustained_set)),
                )
            )
            for ip in sorted(item['arrival_ips']):
                cell = item['matched'][ip]
                arrivals_out.append(
                    Arrival(
                        week_index=index + 1,
                        week_start=start,
                        auth_method=stratum,
                        ip=ip,
                        query_volume=cell.completed,
                        failure_volume=cell.failed,
                        service_load=cell.duration_sum,
                    )
                )
    result.rows = rows
    result.arrivals = arrivals_out
    result.cutoffs = cutoffs
    return result


def evaluate(queries: list[SyncQuery], path: str = '/log.jsonl') -> Evaluation:
    payload = slice_from_queries(path, 0, 0, queries)
    return evaluate_slices([payload])


def rewrite_report(tally_dir: str) -> Evaluation:
    result = evaluate_slices(load_slices(tally_dir))
    write_result(result, tally_dir)
    return result


def format_number(value: int | float | None) -> str:
    if value is None:
        return ''
    if isinstance(value, float):
        text = f'{value:.6f}'.rstrip('0').rstrip('.')
        return text if text else '0'
    return str(value)


def format_flag(value: bool | None) -> str:
    if value is None:
        return 'not_scored'
    return 'true' if value else 'false'


def format_stamp(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')


def format_day(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).strftime('%Y-%m-%d')


def summary_text(result: Evaluation) -> str:
    lines = ['Argus sync traffic', '']
    if result.log_start is None or result.log_end is None:
        lines.append('log span: none')
    else:
        lines.append(f'log span: {format_stamp(result.log_start)} through {format_stamp(result.log_end)}')
    lines.append(f'sync queries: {result.queries_read}')
    lines.append(f'calendar weeks fully inside the log span: {result.calendar_weeks}')
    lines.append(f'latest calendar weeks considered: {result.considered_weeks}')
    lines.append(f'holes: {len(result.holes)}')
    for hole in result.holes:
        lines.append(f'  {format_day(hole)}')
    lines.append(f'complete weeks: {len(result.weeks)}')
    if result.weeks:
        lines.append(f'week 1: {format_day(result.weeks[0])}')
        lines.append(f'week {len(result.weeks)}: {format_day(result.weeks[-1])}')
    lines.append('')
    if result.stopped:
        lines.append(f'Stopped: {result.stop_reason}')
        return '\n'.join(lines) + '\n'
    lines.append('Cutoffs')
    for cutoff in result.cutoffs:
        intensity = 'not scored' if cutoff.intensity is None else format_number(cutoff.intensity)
        lines.append(
            f'  {cutoff.auth_method}: intensity {intensity}; '
            f'failure concentration {cutoff.failure_concentration}; '
            f'new-address count {cutoff.new_address_count}'
        )
    lines.append('')
    lines.append('Agent-like ranges')
    ranges = agent_like_ranges(result)
    if not ranges:
        lines.append('  none')
    else:
        lines.extend(f'  {line}' for line in ranges)
    lines.append('')
    lines.append('Service load sums the logged duration values. The unit is the unit stored in the logs.')
    return '\n'.join(lines) + '\n'


def agent_like_ranges(result: Evaluation) -> list[str]:
    by_stratum: dict[str, list[WeekRow]] = defaultdict(list)
    for row in result.rows:
        by_stratum[row.auth_method].append(row)
    statements = []
    for stratum, rows in by_stratum.items():
        statements.extend(_flag_ranges(rows, stratum, SIG_INTENSITY, lambda row: row.sustained_intensity))
        statements.extend(_flag_ranges(rows, stratum, SIG_NEW, lambda row: row.sustained_many_new))
        addresses = sorted({ip for row in rows for ip in row.sustained_failure_addresses})
        for ip in addresses:
            statements.extend(
                _flag_ranges(
                    rows,
                    stratum,
                    SIG_FAILURE,
                    lambda row, ip=ip: ip in row.sustained_failure_addresses,
                    ip,
                )
            )
    return statements


def _flag_ranges(rows, stratum, signature, predicate, address: str | None = None):
    ranges = []
    start = None
    for index, row in enumerate(rows):
        if predicate(row) and start is None:
            start = index
        elif not predicate(row) and start is not None:
            ranges.append((start, index - 1))
            start = None
    if start is not None:
        ranges.append((start, len(rows) - 1))
    lines = []
    for begin, end in ranges:
        first = rows[begin]
        last = rows[end]
        span = (
            f'weeks {first.week_index} through {last.week_index} '
            f'({format_day(first.week_start)} through {format_day(last.week_start)})'
        )
        if address is None:
            lines.append(f'{stratum}  {span}  {signature}')
        else:
            lines.append(f'{stratum}  {span}  {signature}  {address}')
    return lines


def write_result(result: Evaluation, out_dir: str) -> None:
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, 'summary.txt'), 'w', encoding='utf-8') as handle:
        handle.write(summary_text(result))
    if result.stopped:
        for name in ('weekly.csv', 'arrivals.csv', 'cutoffs.csv'):
            stale = os.path.join(out_dir, name)
            if os.path.exists(stale):
                os.remove(stale)
        return
    _write_csv(os.path.join(out_dir, 'weekly.csv'), WEEKLY_FIELDS, (_weekly_record(row) for row in result.rows))
    _write_csv(os.path.join(out_dir, 'arrivals.csv'), ARRIVAL_FIELDS, (_arrival_record(row) for row in result.arrivals))
    _write_csv(os.path.join(out_dir, 'cutoffs.csv'), CUTOFF_FIELDS, (_cutoff_record(row) for row in result.cutoffs))


def _write_csv(path: str, fields: tuple[str, ...], records) -> None:
    with open(path, 'w', encoding='utf-8', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator='\n')
        writer.writeheader()
        for record in records:
            writer.writerow(record)


def _weekly_record(row: WeekRow) -> dict[str, str]:
    return {
        'week_index': str(row.week_index),
        'week_start': format_day(row.week_start),
        'auth_method': row.auth_method,
        'query_volume': str(row.query_volume),
        'failure_volume': str(row.failure_volume),
        'service_load': format_number(row.service_load),
        'duration_median': format_number(row.duration_median),
        'duration_p95': format_number(row.duration_p95),
        'client_diversity': str(row.client_diversity),
        'intensity': format_number(row.intensity),
        'new_address_count': str(row.new_address_count),
        'new_address_presence': str(row.new_address_presence),
        'arrival_query_volume': str(row.arrival_query_volume),
        'arrival_failure_volume': str(row.arrival_failure_volume),
        'arrival_service_load': format_number(row.arrival_service_load),
        'arrival_share_query_volume': format_number(row.arrival_share_query_volume),
        'arrival_share_service_load': format_number(row.arrival_share_service_load),
        'high_intensity': format_flag(row.high_intensity),
        'many_new_source_addresses': format_flag(row.many_new),
        'high_failure_concentration': format_flag(row.high_failure),
        'high_failure_addresses': ';'.join(row.high_failure_addresses),
        'sustained_high_intensity': 'true' if row.sustained_intensity else 'false',
        'sustained_many_new_source_addresses': 'true' if row.sustained_many_new else 'false',
        'sustained_high_failure_concentration': 'true' if row.sustained_failure else 'false',
        'sustained_failure_addresses': ';'.join(row.sustained_failure_addresses),
        'agent_like': 'true' if row.agent_like_signatures else 'false',
        'agent_like_signatures': ';'.join(row.agent_like_signatures),
    }


def _arrival_record(row: Arrival) -> dict[str, str]:
    return {
        'week_index': str(row.week_index),
        'week_start': format_day(row.week_start),
        'auth_method': row.auth_method,
        'ip': row.ip,
        'arrival_query_volume': str(row.query_volume),
        'arrival_failure_volume': str(row.failure_volume),
        'arrival_service_load': format_number(row.service_load),
    }


def _cutoff_record(row: Cutoff) -> dict[str, str]:
    return {
        'auth_method': row.auth_method,
        'intensity_cutoff': format_number(row.intensity),
        'failure_concentration_cutoff': str(row.failure_concentration),
        'new_address_count_cutoff': str(row.new_address_count),
    }


class SyncTrafficVisitor:
    """Merges each source log into the Argus sync-traffic tally.

    metricsPipe calls ``visit(**kwargs)`` on this module. The constructor
    reads the same arguments as ``LogVisitor``.
    """

    def __init__(self, **kwargs):
        self.config = kwargs.get('config', None)
        self.storage_name = kwargs.get('storage_name', None)
        self.logger = logging.getLogger(__name__)

    def visit(self):
        self.logger.debug('Begin visit')
        if self.config is None or not self.config.working_directory:
            raise SyncTrafficError('working_directory is required')
        if self.storage_name is None:
            raise SyncTrafficError('storage_name is required')
        directory = tally_directory(self.config.working_directory)
        os.makedirs(directory, exist_ok=True)
        for source_name in self.storage_name.source_names:
            self.logger.info(f'Processing source name: {source_name}')
            merge_path(directory, source_name)
        self.logger.debug('End visit')


def visit(**kwargs):
    return SyncTrafficVisitor(**kwargs).visit()
