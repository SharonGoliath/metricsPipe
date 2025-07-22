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

import dask.bag as db
import dask.dataframe as dd
import pandas as pd
import logging

from datetime import datetime
from glob import glob
from os import stat, path

from caom2pipe.data_source_composable import LocalFilesDataSourceRunnerMeta
from caom2pipe.execute_composable import CaomExecuteRunnerMeta, OrganizeExecutesRunnerMeta
from caom2pipe.manage_composable import CadcException, exec_cmd_info, StorageName, TaskType


__all__ = ['HAProxyVisitor']

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
#     - halog:: cat haproxy.log-20250428 | halog -u -H > halog.out
#     - pd.merge with caom2 table query results, counts by calibration level, instrument
# 

def my_load(line):
    bits = line.split()
    # print(f'10 {bits[10]} 12 {bits[12]} 13{bits[13]} 14 {bits[14]}')
    # only track successful retrievals
    if bits[10] == '200' and bits[12] == '-' and bits[13] == '-' and bits[14] == '----' and bits[18] == '"GET':
        ip_address = bits[5].split(':')[0]
        f_name = bits[-2].split('/')[-1].split('?RUNID')[0].split('?SUB')[0]
        # if 'fits' in line:
        #     print(f'ip_address {ip_address} f_name {f_name}\nbits{bits[-2]}')
        if 'fits' in f_name:
            # print(f'ip_address {ip_address} f_name {f_name}')
            return {
                'ip_address': ip_address,
                'f_name': f_name,
            }
    return {}


class HAProxyVisitor:
    def __init__(self, **kwargs):
        self.config = kwargs.get('config', None)
        self.storage_name = kwargs.get('storage_name', None)
        self.logger = logging.getLogger(__name__)

    def visit(self):
        self.logger.debug('Begin visit')
        # questions for IP, file name correlation
        # 
        # cannot use halog, because it doesn't report IP addresses in any of its configurations
        # 
        # use requests which look like this:
        # Apr 27 04:01:59 localhost haproxy[18895]: 132.246.195.201:54940 [27/Apr/2025:04:01:50.805] wsuv~ uv_minoc/ws-uv-02 0/0/0/30/9160 200 267998814 - - ---- 90/88/3/2/0 0/0 {|} "GET /minoc/files/dXJpPWNhZGM6Q0ZIVC8zMTcyMTAzby5maXRzLmZ6JmdudD1SZWFkR3JhbnQmc3ViPXN0b3JvcHM=~tH9uJXHBV33YVdPe7YcBO6PnTILqGKTM7je_ov4Xng20IK-U8scVvlMJgPuh77qgt4OfTHoVP8DQtopndfcLfOFjVyCTOXMdMelx5Cz3VOScllqTTCjNAYNSsvGmDYDwg4f47BvD-H8r9cmAl3Tg0NVLW58qhtYuDrSWI8J5sFWWosiLrp9UvHv7BB35sn_4HPrANjg-RXA81__JdABIc5DV9xAtAtuCDX1TS0D3Zh4tQv2SsGx7eNOfLcLJutK5XPDBUMhFxbEP_xq2YzUoqScee-RJPHiD-zFIknreL68Ek7Ga7rfIBHJxHd4f_BPjE1EKWYCe5JKx9CoXJA4SEnnew1IDf1X1F8tUA8yuoroRrVOjcXT3Dksqy2Wu8srhQuxK9z-ZYVWXoO06V8OA_HHpmwuRr589qlq2aeoAM9_gCHfwd5oI63fjFDhQj2FPce5gojzhjHZ7ePULJuX-HLaDoB8ZTNp8bu9Dwl7xpL4O8JgfwjSAfFA7ZlAYxcgievsuTabETqhvz5fOitiHlM_ntRfR9lJWpdaOY9WRS6ufEkOI9Il5dKTv_7fK-KLoRu5g8YBY1HMoKodnomDwqah5Kl4Pzrcu6ie64_KKPif2E0dO2hQhpZljb7yjiOYdG3E5Pm-sgYBs8KLfOhDyxzb2OgG1g9J21kd46gzPm4s=/cadc:CFHT/3172103o.fits.fz HTTP/1.1"
        # 
        # or like this:
        # Apr 27 17:09:00 localhost haproxy[18895]: 10.21.2.133:42482 [27/Apr/2025:17:08:48.613] wsuv~ uv_minoc/ws-uv-03 0/0/0/498/11536 200 493033374 - - ---- 53/51/8/4/0 0/0 {|} "GET /minoc/files/cadc:CFHT/3172193f.fits.fz HTTP/1.1"
        # 
        # or both?
        # 
        # only success, which looks like 
        # status == 200, and reporting codes look like " - - ---- "
        #
        # IP Address is the 5th field
        # file name is the last field, split by '/'
        cumulative_fqn = self.config.lookup.get('cumulative_fqn')
        caom_fqn = self.config.lookup.get('caom_fqn')
        instrument = self.config.lookup.get('instrument')
        caom = pd.read_csv(caom_fqn)
        # print(caom.head())
        # print(caom.info())
        # instrument_df = caom[caom.instrument_name == instrument]
        for source_name in self.storage_name.source_names:
            # Process the DataFrame as needed
            ips = db.read_text(
                    source_name
                ).filter(
                    lambda line: f'{self.config.scheme}:{self.config.collection}/' in line
                ).map(my_load)
            meta = {
                'ip_address': 'object',
                'f_name': 'object',
            } 
            ips_df = ips.to_dataframe(meta=meta).compute().drop_duplicates()
            self.logger.info(f'Number of CFHT FITS files: {len(ips_df)}')
            ips_df.to_csv('./ips.csv', header=True, index=False)
            if len(ips_df) > 0:
                # print(ips_df.head())
                # print(ips_df.info())
                try:
                    merged = pd.merge(ips_df, caom, how='inner', on=['f_name'])            
                    self.logger.info(f'Number of WIRCam CFHT files: {len(merged)}')
                    merged.to_csv(
                        cumulative_fqn, mode='a', header=not path.exists(cumulative_fqn), index=False
                    )
                except Exception as e:
                    self.logger.error(e)
                    import traceback
                    self.logger.error(traceback.format_exc())
            self.logger.info(f'Processed source name: {source_name}')
        self.logger.debug('End visit')


def visit(**kwargs):
    # ignore observation 
    return HAProxyVisitor(**kwargs).visit()
