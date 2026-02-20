from astropy import units as u
from astropy.time import Time

# import dask.bag as db
# import dask.dataframe as dd
import numpy as np
import pandas as pd
import json
import os

# CFHT would like to know how many people use the CFHT archive each year, from which countries and which products
# they use (processed  data? raw data? which instruments?). this information is needed for CFHT's response to French
# planning processes.
#
# is it better to use the raven logs? Or both?
#
# how many people:
#     - distinct user's size from minoc logs
#         - convert between user id, user name, and user CN
#
# which countries - do this out-of-band:
#     - link to email information from user table, and use the TLD to identify countries
#
# which products from which instruments:
#     - pd.merge with caom2 table query results, counts by calibration level, instrument
#
# how to stitch together each of the minoc logs:
#     for each minoc log:
#          - do the load and filter that is here
#          - do the merge with the caom2 table query results that is here
#          - append the merged with caom2 counts to a file
#          - append the user names to a different file
#
#     assemble all the lines together into one set of metrics for file, instrument types
#     assemble the country information out-of-band

# GoC Service Availability
#
# root@7ba72850f247:/usr/src/app# halog -srv -time 1745798400:1745798800 < haproxy.log-20250428
# srv_name 1xx 2xx 3xx 4xx 5xx other tot_req req_ok pct_ok avg_ct avg_rt
# src_cavern/cavern 0 15 0 0 0 0 15 15 100.0 0 47
# src_portalui/arc 0 13 0 0 0 0 13 13 100.0 0 26
# src_storageui/arc 0 14 0 0 0 0 14 14 100.0 0 134
# uv_ac/horde-uv 0 259 0 0 0 0 259 259 100.0 143 1239
# uv_luskan/horde-uv 0 84 73 0 0 0 157 157 100.0 144 8495
# uv_minoc/ws-uv-01 0 330 0 153 0 0 483 483 100.0 0 947
# uv_minoc/ws-uv-02 0 329 0 155 0 0 484 484 100.0 0 454
# uv_minoc/ws-uv-03 0 325 0 159 0 0 484 484 100.0 0 931
# uv_reg/horde-uv 0 19 0 0 0 0 19 19 100.0 83 108
# uv_youcat/ws-uv-02 0 1 0 0 0 0 1 1 100.0 0 176
# uv_youcat/ws-uv-03 0 1 0 0 0 0 1 1 100.0 1 158
# ws_arc/arc 0 3262 0 1 0 0 3263 3263 100.0 0 1164
# ws_skaha/skaha 0 10 0 0 0 0 10 10 100.0 1 463
# wsuv/<NOSRV> 0 0 0 2 0 0 2 0 0.0 0 0
# 619515 lines in, 14 lines out, 52 parsing errors


EXPLANATIONS = {
    'ac': 'access control',
    'ad_luskan': 'retired storage system',
    'ad_minoc': 'retired storage system',
    'arcade_ssh_ingress': 'arcade',
    'blackbox': 'prometheus test',
    'cadc-dss': '/cadcbin/en/dss/dss_status.py, /cadcbin/en/dss/dss_query_coord.pl',
    'cadc-ws-ams': '/ams/grants, /ams/observations',
    'cadc-ws-argus': '/argus/sync',
    'cadc-ws-caom25argus': 'caom25 /argus/sync',
    'cadc-ws-caom25torkeep': 'caom25 /ams',
    'cadc-ws-caom2ops': '/caom2ops/datalink, /caom2ops/meta/, /caom2ops/pkg, /caom2ops/sync, /caom2ops/soda-echo',
    'cadc-ws-cred': '/cred/generate',
    'cadc-ws-data2': '/data/pub, /data/auth',
    'cadc-ws-reg': 'resource-caps, cadc, maq, oai',
    'cadc-ws-sia': '/sia/v2query',
    'cadc-ws-target-resolver': 'science target resolver',
    'cadc-ws-youcat': 'science catalog database',
    'cadc-www': 'static, /en, /fr, /community pages, telescope pages',
    'cadc-www-access': '/access/control, /access/consent. /access/login',
    'cadc-www-tomcat': 'advanced search, previews, access control ui, cadc static, group management ui, meetings, download ui',
    'cadc_luskan': 'storage system',
    'cadc_minoc': 'storage system',
    'cadcrepo': '/cadc/EL/9/x86_64/repodata/repomd.xml, /cadc/EL/7/x86_64/repodata/repomd.xml',
    'canfar-ws': 'turned off mid-late April',
    'canfar-ws-vault': 'user storage',
    'canfar-ws-youcat': 'CANFAR user RDBMS',
    'canfar-www': 'reg/applications',
    'canfar-www-access': '/access/control, /access/consent',
    'canfar-www-storage': 'arc, vault',
    'canfar-www-tomcat': 'CANFAR science portal, citation, static, access',
    'cvo-pg-pool': 'storage system',
    'doi': 'capabilities, instances',
    'global_baldur': 'storage system',
    'global_luskan': 'storage system',
    'global_raven': 'storage system',
    'meetings-admin': 'meetings_admin/meetings_admin.py',
    'meetings-ui': 'meetings/rssFeed',
    'no-match': 'robots.txt',
    'rcvault': 'release candidate testing',
    'sc2': 'sandbox',
    'sc2caom2metaui': 'sandbox',
    'sc2links': 'sandbox',
    'sc2meta': 'sandbox',
    'sc2repo': 'sandbox',
    'sc2search': 'sandbox',
    'sc2soda': 'sandbox',
    'sc2tap': 'sandbox',
    'src_arc': 'canSRC user storage',
    'src_cavern': 'canSRC user storage',
    'src_echo': 'SRCNet heartbeat?',  # every 30s?
    'src_portalui': 'canSRC portal ui',
    'src_posixmapper': 'canSRC access control',
    'src_preparedata': 'canSRC data retrieval',
    'src_skaha': 'canSRC science portal',
    'src_soda': 'canSRC data retrieval cutouts',
    'src_storageui': 'canSRC storage ui',
    'tardis-pg-pool': 'IDK',
    'stats': 'Kevin\'s CANFAR statistics',
    'transfer': 'retired storage system',
    'uv_ac': 'access control',
    'uv_lsstbutler': 'LSST butler',
    'uv_lsstluskan': 'LSST storage system',
    'uv_lsstminoc': 'LSST storage system',
    'uv_luskan': 'storage system',
    'uv_minoc': 'storage system',
    'uv_reg': 'capability check - health check',
    'uv_youcat': 'CANFAR user RDBMS',
    'vault-pg-pool': 'IDK',
    'ws-cadc': 'no destination <NOSRV>',
    'ws.cadc': 'cred, ams, baldur ????',
    'ws-cadc.canfar': 'minoc redirects',
    'ws_arc': 'CANFAR user storage',
    'ws_cavern': 'user storage',
    'ws_http': 'no destination <NOSRV>',
    'ws_https': 'no destination <NOSRV>',
    'ws_session': 'CANFAR science platform sessions',
    'ws_skaha': 'CANFAR science platform',
    'ws_src_gms': 'canSRC group management',
    'ws_transfer': 'retired storage system',
    'wsuv': 'canSRC storage system (prepare-data)',
    'www-cadc': 'no destination <NOSRV> for /data/pub',
    'www-canfar': 'no destination for reg/applications',
}


def my_load(line):
    bits = line.split()
    # print(f'10 {bits[10]} 12 {bits[12]} 13{bits[13]} 14 {bits[14]}')
    # only track successful retrievals
    if bits[10] == '200' and bits[12] == '-' and bits[13] == '-' and bits[14] == '----' and bits[18] == '"GET':
        ip_address = bits[5].split(':')[0]
        f_name = bits[-2].split('/')[-1].split('?RUNID')[0].split('?SUB')[0]
        if 'fits' in f_name:
            # print(f'ip_address {ip_address} f_name {f_name}')
            return {
                'ip_address': ip_address,
                'f_name': f_name,
            }
    return {}


def guess_at_query_types():
    with open('./service_list.txt') as f:
        service_list = f.readlines()
    print(len(service_list))


def main_cfht(f_name, year):
    df1 = pd.read_csv(f_name)
    # for now, remove the cadc-app and cadc-ws entries from the srv reports - don't double-count
    df1 = df1[df1['srv'] != 'cadc-ws']
    df1 = df1[df1['srv'] != 'cadc-app']
    # df2 = pd.read_csv('./community_aggregate.csv')
    # df = pd.concat([df1, df2], ignore_index=True)
    df = df1
    # print(df.info())

    # Convert start_ts to datetime if not already
    df['start_ts'] = pd.to_datetime(df['start_ts'], unit='s', errors='coerce')

    # Create a two-week bucket column
    epoch_start = pd.Timestamp('1970-01-01')
    delta = pd.Timedelta(days=14)
    df['two_week_bucket'] = (df['start_ts'] - epoch_start) // delta

    pd.set_option('display.max_rows', None)
    # Aggregate by bucket and srv_name
    agg_df = df.groupby(['two_week_bucket', 'srv']).agg(
        total_request_count=('tot_req', 'sum'),
        error_count=('tot_5xx', 'sum'),
    ).sort_values('total_request_count', ascending=False)
    # agg_df = agg_df[agg_df['total_request_count'] >= 2]
    agg_df['success_percentage'] = (
        (agg_df['total_request_count'] - agg_df['error_count']) / agg_df['total_request_count']
    ) * 100
    # print(agg_df)
    sum_agg = df.agg(all_results=('tot_req', 'sum'), all_errors=('tot_5xx', 'sum'))
    # print(other_agg.fillna(0).astype(int))

    other_agg = df.groupby(['srv']).agg(srv_requests=('tot_req', 'sum'), srv_errors=('tot_5xx', 'sum'))
    print(other_agg)
    print('#######')
    other_agg['success_percentage'] = (
        (other_agg['srv_requests'] - other_agg['srv_errors']) / other_agg['srv_requests']
    ) * 100
    other_agg['percentage_of_all_requests'] = (other_agg['srv_requests'] / sum_agg['tot_req']['all_results']) * 100
    # Format request_percentage to two significant digits, no exponent
    other_agg['percentage_of_all_requests'] = other_agg['percentage_of_all_requests'].apply(lambda x: f"{x:.4f}")
    # print(other_agg)
    # Add explanations to other_agg based on srv
    explanations_df = pd.DataFrame(list(EXPLANATIONS.items()), columns=['srv', 'explanation'])
    other_agg_with_expl = other_agg.merge(explanations_df, how='left', left_index=True, right_on='srv')
    # Reorder columns as requested
    cols = ['srv', 'srv_requests', 'srv_errors', 'success_percentage', 'percentage_of_all_requests', 'explanation']
    # Left justify the explanations column for better readability
    other_agg_with_expl['explanation'] = other_agg_with_expl['explanation'].astype(str)
    # Right justify all columns except 'srv' and 'explanation'
    # right_justified = other_agg_with_expl[cols].copy()
    # for col in ['srv_requests', 'srv_errors', 'success_percentage', 'percentage_of_all_requests']:
    #     right_justified[col] = right_justified[col].astype(str).str.rjust(10)
    # print(other_agg_with_expl.to_string(index=False))
    # Calculate and add the 99th percentile QPS per service
    qps_df = pd.read_csv(f_name)
    qps_df['start_ts'] = pd.to_datetime(qps_df['start_ts'], unit='s', errors='coerce')
    qps_df = qps_df.sort_values(['srv', 'start_ts'])
    qps_results = []
    for srv, group in qps_df.groupby('srv'):
        group = group.set_index('start_ts').sort_index()
        hourly = group['tot_req'].resample('1h').last().dropna()
        qps = hourly.diff() / 3600.0
        qps = qps[qps > 0]
        for ts, rate in qps.items():
            qps_results.append({'srv': srv, 'queries_per_second': rate})
    qps_all = pd.DataFrame(qps_results)
    if not qps_all.empty:
        percentiles = qps_all.groupby('srv')['queries_per_second'].quantile(0.99).reset_index()
        percentiles.rename(columns={'queries_per_second': 'qps_99th_percentile'}, inplace=True)
        other_agg_with_expl = other_agg_with_expl.merge(percentiles, how='left', on='srv')
    else:
        other_agg_with_expl['qps_99th_percentile'] = np.nan
    other_agg_with_expl = other_agg_with_expl[[
        'srv',
        'srv_requests',
        'srv_errors',
        'success_percentage',
        'percentage_of_all_requests',
        'qps_99th_percentile',
        'explanation',
    ]]
    print(other_agg_with_expl.to_string(index=False))
    print(f'Total                 {sum_agg['tot_req']['all_results'].astype(int)}     {sum_agg['tot_5xx']['all_errors'].astype(int)}')

    # TODO things to fix in the data cleaning
    # 2. if, for example ws_transfer/<NOSRV> has a bunch of 500s, need to figure out where they were supposed to go
    #    and add that to the failures for the service
    # 3. should this row show up in the results: "ws.cadc/<NOSRV> 80 0"

    # Suppose df_ip_count has columns ['ip_address', 'count']
    # and df_ip_country has columns ['ip_address', 'country']

    # Example:
    # df_ip_count = pd.DataFrame({'ip_address': [...], 'count': [...]})
    # df_ip_country = pd.DataFrame({'ip_address': [...], 'country': [...]})

    # merged = pd.merge(df_ip_count, df_ip_country, on='ip_address', how='inner')
    # country_counts = merged.groupby('country')['count'].sum().reset_index()
    # # Ensure 'country' column is of type object to avoid dtype mismatch
    # country_counts['country'] = country_counts['country'].astype(object)
    # # Add a row for the total
    # total_count = country_counts['count'].sum()
    # total_row = pd.DataFrame([{'country': 'Total', 'count': total_count}])
    # country_counts = pd.concat([country_counts, total_row], ignore_index=True)
    # print(country_counts)


def main_rolling_bin(f_name, year):
    df1 = pd.read_csv(f_name)
    # for now, remove the cadc-app and cadc-ws entries from the srv reports - don't double-count
    df1 = df1[df1['srv'] != 'cadc-ws']
    df1 = df1[df1['srv'] != 'cadc-app']
    df = df1

    # Convert start_ts to datetime if not already
    df['start_ts'] = pd.to_datetime(df['start_ts'], unit='s', errors='coerce')
    print(df.columns)

    # Calculate rolling 7-day error rates for each service
    services = df['srv'].unique()
    start_date = pd.Timestamp(f'{year}-01-01')
    end_date = pd.Timestamp(f'{year}-12-31')
    window_size = 7  # days

    with open(f'./rolling_bin_outages_{year}.csv', 'w') as f:
        f.write('srv,num_windows_above_5pct,percent_windows\n')
        for service in services:
            service_df = df[df['srv'] == service].copy()
            service_df = service_df.sort_values('start_ts')
            # Create a daily time range for the year, excluding June 12, 2025 to June 25, 2025
            # the exclusion range is because there are no haproxy logs for those days
            days = pd.date_range(start=start_date, end=end_date, freq='D')
            exclude_start = pd.Timestamp('2025-06-12')
            exclude_end = pd.Timestamp('2025-06-25')
            x = exclude_end - exclude_start
            print(x)
            days = days[(days < exclude_start) | (days > exclude_end)]
            error_rates = []
            for day in days:
                window_start = day
                window_end = day + pd.Timedelta(days=window_size)
                mask = (service_df['start_ts'] >= window_start) & (service_df['start_ts'] < window_end)
                window_df = service_df[mask]
                total_requests = window_df['tot_req'].sum()
                total_errors = window_df['tot_5xx'].sum()
                error_rate = (total_errors / total_requests) if total_requests > 0 else 0
                error_rates.append((day, error_rate))
            error_rates = np.array(error_rates, dtype=[('date', 'O'), ('error_rate', 'f8')])
            num_windows_above_5pct = np.sum(error_rates['error_rate'] > 0.05)
            # print(num_windows_above_5pct)
            # print(error_rates.shape)
            if num_windows_above_5pct > 0.05 * (365 - int(x) ):
                print(f'{service} did not meet standard in {year}')
                percent_windows = num_windows_above_5pct / ( 365.0 - float(x) )
                print(f"{service} {num_windows_above_5pct} {percent_windows}")
                f.write(f'{service},{num_windows_above_5pct},{percent_windows*100.0:.1f}\n')
                # if service in ['cadc-ws-caom2ops', 'cadc-ws-sia']:
                #     print(error_rates)


def main_all_services_rolling_bin(f_name, year):
    # this does the work for a single service?
    df1 = pd.read_csv(f_name)
    # df1 = pd.read_csv('./sad_srv_aggregate.csv')
    # for now, remove the cadc-app and cadc-ws entries from the srv reports - don't double-count
    df1 = df1[df1['srv'] != 'cadc-ws']
    df1 = df1[df1['srv'] != 'cadc-app']
    # df2 = pd.read_csv('./community_aggregate.csv')
    # df = pd.concat([df1, df2], ignore_index=True)
    df = df1
    # print(df.info())

    # Convert start_ts to datetime if not already
    df['start_ts'] = pd.to_datetime(df['start_ts'], unit='s', errors='coerce')
    print(df.columns)

    # Calculate rolling 7-day error rates for all services
    # services = df['srv'].unique()
    start_date = pd.Timestamp(f'{year}-01-01')
    end_date = pd.Timestamp(f'{year}-12-31')
    window_size = 7  # days

    # for service in services:
    #     service_df = df[df['srv'] == service].copy()
    #     service_df = service_df.sort_values('start_ts')

    # Create a daily time range for the year
    days = pd.date_range(start=start_date, end=end_date, freq='D')
    error_rates = []
    for day in days:
        window_start = day
        window_end = day + pd.Timedelta(days=window_size)
        mask = (df['start_ts'] >= window_start) & (df['start_ts'] < window_end)
        window_df = df[mask]
        total_requests = window_df['tot_req'].sum()
        total_errors = window_df['tot_5xx'].sum()
        error_rate = (total_errors / total_requests) if total_requests > 0 else 0
        error_rates.append((day, error_rate))
    error_rates = np.array(error_rates, dtype=[('date', 'O'), ('error_rate', 'f8')])
    num_windows_above_5pct = np.sum(error_rates['error_rate'] > 0.05)
    # num_windows_above_5pct = np.sum(error_rates['error_rate'] > 0.0005)
    # print(num_windows_above_5pct)
    # print(error_rates.shape)
    # print(error_rates)
    percent_windows = num_windows_above_5pct / 365.0
    if num_windows_above_5pct > 0.05 * 365:
        # print(f"{service} did not meet standard in 2024")
        print(f"yes {num_windows_above_5pct} {percent_windows}")
    else:
        print(f'The services met the standard for {year}. {percent_windows}')


def on_the_hill(f_name, year):
    df1 = pd.read_csv(f_name)
    # for now, remove the cadc-app and cadc-ws entries from the srv reports - don't double-count
    df1 = df1[df1['srv'] != 'cadc-ws']
    df1 = df1[df1['srv'] != 'cadc-app']
    # df2 = pd.read_csv('./community_aggregate.csv')
    # df = pd.concat([df1, df2], ignore_index=True)
    df = df1
    # print(df.info())

    # Convert start_ts to datetime if not already
    df['start_ts'] = pd.to_datetime(df['start_ts'], unit='s', errors='coerce')
    pd.set_option('display.max_rows', None)
    other_agg = df.groupby(['srv']).agg(srv_requests=('tot_req', 'sum'), srv_errors=('tot_5xx', 'sum'))
    # trying to identify traffic that goes to the hill site
    # remove any records that are housed at UVic: canfar, uv, or src in the name
    # the "pool" is because they're all 0 :)
    # other_agg = other_agg[~other_agg.index.str.contains('canfar')]
    for srv_value in ['canfar', 'uv_', 'src_', 'pool', 'ws_arc', 'ws_cavern', 'ws_session', 'ws_skaha', 'wsuv']:
        other_agg = other_agg[~other_agg.index.str.contains(srv_value)]
    sum_agg = other_agg.agg(all_results=('srv_requests', 'sum'), all_errors=('srv_errors', 'sum'))
    print(sum_agg)
    print(f'Total                 {sum_agg['srv_requests']['all_results'].astype(int)}     {sum_agg['srv_errors']['all_errors'].astype(int)}')
    print(other_agg)


def queries_per_second(f_name):
    # this is the Google SRE QPS metric (I think), and I think it should be part of the SLO
    df = pd.read_csv(f_name)
    # Ensure 'start_ts' is datetime
    df['start_ts'] = pd.to_datetime(df['start_ts'], unit='s', errors='coerce')
    # Sort by service and time
    df = df.sort_values(['srv', 'start_ts'])

    # Prepare result DataFrame
    results = []

    for srv, group in df.groupby('srv'):
        print(f'srv {srv}')
        group = group.set_index('start_ts').sort_index()
        # Resample to 1 hour bins, taking the last counter in each bin
        hourly = group['tot_req'].resample('1H').last().dropna()
        # Calculate the rate as the difference between consecutive bins divided by 3600 seconds
        qps = hourly.diff() / 3600.0
        # Remove negative or NaN rates (if any)
        qps = qps[qps > 0]
        for ts, rate in qps.items():
            results.append({'srv': srv, 'timestamp': ts, 'queries_per_second': rate})

    qps_df = pd.DataFrame(results)
    # print(qps_df)
    # Calculate and print the 99th percentile QPS for each service
    if not qps_df.empty:
        percentiles = qps_df.groupby('srv')['queries_per_second'].quantile(0.99)
        print("\n99th percentile QPS per service:")
        pd.set_option('display.max_rows', None)
        print(percentiles)
    # print(qps_df)


if __name__ == '__main__':
    year = '2025'
    f_name = f'./srv_aggregate_{year}.csv'
    # main_cfht(f_name, year)
    main_rolling_bin(f_name, year)
    # guess_at_query_types()
    # main_all_services_rolling_bin(f_name, year)
    # on_the_hill(f_name, year)
    # queries_per_second(f_name)