# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2025.                            (c) 2025.
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
#  : 4 $
#
# ***********************************************************************
#

# import dask.bag as db
# import dask.dataframe as dd
import pandas as pd
# import json
import logging

from collections import defaultdict
from datetime import datetime, timedelta
from glob import glob
from os import stat, path, unlink

from caom2pipe.data_source_composable import LocalFilesDataSourceRunnerMeta
from caom2pipe.execute_composable import CaomExecuteRunnerMeta, OrganizeExecutesRunnerMeta
from caom2pipe.manage_composable import CadcException, exec_cmd, exec_cmd_info, StorageName, TaskType
from metricsPipe.log_visit import LogVisitor


__all__ = ['AggregateHAlogVisitor']

bins = defaultdict(list)


class AggregateHAlogVisitor(LogVisitor):
    # def __init__(self, **kwargs):
    #     self.logger = logging.getLogger(__name__)
    #     self.storage_name = kwargs.get('storage_name', None)

    def visit(self):
        self.logger.error('Begin visit')
        # GoC Service Availability
        #
        # haproxy doesn't work with gzip'd files
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
        # need it to look like:

        # 8760 h/year * 0.95 = 8322 h/year
        # 336 h/2 week period * 0.95 = 320 h/2 week period
        if len(bins) != 8760:
            start_time = datetime(year=2024, month=1, day=1)
            end_time = datetime(year=2024, month=12, day=31)
            for i in range(0, 8760):
                ts = start_time.timestamp() + timedelta(hours=i).total_seconds()
                # I want to know which timestamp to start at, based on the timestamp in the file name, which is a day
                # bucket
                bins[ts] = []

        # this is one visitor: 
        # for each day:
        #     find the files that apply to that day
        #     for each file:
        #         decompress
        #         for each hour:
        #             halog -srv -time start_ts:end_ts < file > file.start_ts.end_ts
        #             read as a DataFrame
        #             add the start_ts and end_ts to the DataFrame
        #             append to the output file
        #
        # read the aggregated file
        # find the complete list of services, and fill in for services that do not have hourly hits
        # group by two weeks
        # calculate the two week percentage


        output_file = self.config.lookup.get('output_file')
        for source_name in self.storage_name.source_names:
            # Process the DataFrame as needed
            self.logger.error(f'Processed source name: {source_name}')
            # 20240103 
            # YYYYMMDD
            day_bin = datetime.strptime(path.basename(source_name.replace('.gz', '')).split('-')[-1], '%Y%m%d')
            if source_name.endswith('.gz'):
                exec_cmd(f'gunzip {source_name}')
            for hour in range(0, 24):
                start_ts = (day_bin + timedelta(hours=hour)).timestamp()
                end_ts = (day_bin + timedelta(hours=(hour + 1))).timestamp()
                halog_out_fqn = f'{self.config.working_directory}/halog_out.{start_ts}.{end_ts}'
                info_cmd = f'halog -srv -time {start_ts}:{end_ts} < {source_name.replace(".gz", "")} > {halog_out_fqn} 2> /dev/null'
                exec_cmd(info_cmd)

                new_df = pd.read_csv(halog_out_fqn, sep=' ')
                new_df['start_ts'] = start_ts
                new_df['end_ts'] = end_ts
                # self.logger.error(new_df.info())
                # self.logger.error(new_df.head())
                # self.logger.error(new_df.columns)

                if len(new_df) > 2:
                    new_df.to_csv(output_file, mode='a', header=False, index=False)
                unlink(halog_out_fqn)
            if source_name.endswith('.gz'):
                exec_cmd(f'gzip {source_name.replace('.gz', '')}')
        self.logger.error('End visit')


def visit(**kwargs):
    # ignore observation 
    return AggregateHAlogVisitor(**kwargs).visit()
