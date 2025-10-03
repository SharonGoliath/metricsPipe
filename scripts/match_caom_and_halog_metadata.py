import dask.bag as db
import dask.dataframe as dd
import pandas as pd
import json
import os
import sys


# NEOSSAT/CSA would like to know how many people use the NEOSSAT/CSA archive each year, from which countries and which products 
# they use (processed  data? raw data?).

def main_instrument(scheme, collection, caom_f_name, log_f_name, **kwargs):
    caom = dd.read_csv(caom_f_name).compute()
    collection_ips_df = pd.read_csv(log_f_name)
    merged = pd.merge(collection_ips_df, caom, how='inner', on=['f_name'])
    merged.to_csv(f'./{collection}_ips.csv', header=True, index=False)

    keywords = kwargs.get('keywords')
    agg_files = kwargs.get('agg_files')
    for index, keyword in enumerate(keywords):
        print(f'Sum by keyword {keyword}')
        agg = merged.groupby([keyword]).agg(counts=(keyword, 'size')).sort_values('counts', ascending=False)
        if len(agg) > 0:
            agg.to_csv(agg_files[index], header=True)


if __name__ == '__main__':
    if len(sys.argv) <= 5:
        print('Usage: python match_caom_and_halog_metadata.py <scheme> <collection> <caom metadata file> <log file> <keyword list ...>')
        print('Find metrics for <scheme>:<collection> using metadata from <caom metadata file> and log scrapes from <log file>.')
        print('<caom metadata file> needs to have column headers, at least one of which must be "f_name".')
        print('both input files need to have an f_name column, as pd.merge for the log and caom metadata occurs on that column')
        print('<log file> is a list of the halog entries, with IP addresses, that have <scheme>:<collection>/ in the request.')
        print('keyword list is the list of attribute names for which to do a groupby call on the merged results')
        print('use from a Docker image with halog and dask installed')
        print('e.g. python match_caom_and_halog_metadata.py cadc NEOSSAT neossat_f_name_target_name.csv neossat_fits.out target_name calibrationLevel')
    else:
        scheme = sys.argv[1]
        collection = sys.argv[2]
        caom_info = sys.argv[3]
        log_f_name = sys.argv[4]
        keywords = []
        for ii in range(5, len(sys.argv)):
            print(f'keyword {sys.argv[ii]}')
            keywords.append(sys.argv[ii])

        print(f'Finding metrics for {scheme}:{collection}, comparing {caom_info} with {log_f_name}')
        kwargs = {
            'agg_files': [],
        }
        for ii in keywords:
            kwargs['agg_files'].append(f'{collection}_{ii}_2024.csv')
        kwargs['keywords'] = keywords
        main_instrument(scheme, collection, caom_info, log_f_name, **kwargs)

