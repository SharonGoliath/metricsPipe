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
import json
import logging

from datetime import datetime
from glob import glob
from os import stat, path

from caom2pipe.data_source_composable import LocalFilesDataSourceRunnerMeta
from caom2pipe.execute_composable import CaomExecuteRunnerMeta, OrganizeExecutesRunnerMeta
from caom2pipe.manage_composable import CadcException, StorageName, TaskType


__all__ = ['LogVisitor']

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

def my_load(line):
    try:
        data = json.loads(line.strip())
        user = data.get('user', None)
        if user and user == 'cadcops' or user == 'storops' or user == 'goliaths':
            return None
        return {
            'uri': data.get('resource', None),
            'user': data.get('user', None),
        }
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
    return None


class LocalLogFileDataSource(LocalFilesDataSourceRunnerMeta):

    def __init__(self, config):
        super().__init__(
            config=config, 
            cadc_client=None, 
            recursive=config.recurse_data_sources, 
            scheme=config.scheme,
            storage_name_ctor=StorageName,
        )

    def _append_work(self, prev_exec_dt, exec_dt, entry_path):
        """Finds the incremental work."""
        for glob_pattern in self._config._data_source_globs:
            dir_listing = glob(f'{entry_path}/{glob_pattern}', recursive=self._recursive)
            self._logger.debug(f'Found {len(dir_listing)} entries in {entry_path} matching {glob_pattern}')
            for entry in dir_listing:
                # order the stats check before the default_filter check, because CFHT likes to work with tens
                # of thousands of files, not a few, and the default filter is the one that opens and reads every
                # file to see if it's a valid FITS file
                #
                # send the dir_listing value
                # skip dot files, but have a special exclusion, because otherwise the entry.stat() call will
                # sometimes fail.
                entry_stats = stat(entry)
                entry_st_mtime_dt = datetime.fromtimestamp(entry_stats.st_mtime)
                if exec_dt >= entry_st_mtime_dt >= prev_exec_dt:
                    temp_storage_name = self._storage_name_ctor(source_names=[entry])
                    self._temp[entry_st_mtime_dt].append(temp_storage_name)

    def _find_work(self, entry_path):
        """Finds all the work."""
        for glob_pattern in self._config._data_source_globs:
            dir_listing = glob(f'{entry_path}/{glob_pattern}', recursive=self._recursive)
            self._logger.debug(f'Found {len(dir_listing)} entries in {entry_path} matching {glob_pattern}')
            for entry in dir_listing:
                self._logger.error(entry)
                temp_storage_name = self._storage_name_ctor(source_names=[entry])
                self._logger.info(f'Adding {entry} to work list.')
                self._work.append(temp_storage_name)


class MetricsPipeVisit(CaomExecuteRunnerMeta):
    """
    Defines the pipeline step for Collection creation or augmentation by
    a visitor of metadata into CAOM.
    """

    def __init__(
        self,
        clients,
        config,
        meta_visitors,
        reporter,
    ):
        super().__init__(clients, config, meta_visitors, reporter)
        
    def _set_preconditions(self):
        pass

    def _visit_meta(self):
        """Execute metadata-only visitors on an Observation in
        memory."""
        if self.meta_visitors:
            kwargs = {
                'config': self._config,
                'clients': self._clients,
                'storage_name': self._storage_name,
                'reporter': self._reporter,
            }
            for visitor in self.meta_visitors:
                try:
                    visitor.visit(**kwargs)
                except Exception as e:
                    raise CadcException(e)

    def execute(self, context):
        super().execute(context)

        self._logger.debug('the metadata visitors')
        self._visit_meta()

        self._logger.debug('write the updated xml to disk for debugging')
        self._write_model()

        self._logger.debug('End execute')


class MetricsPipeOrganizeExecutes(OrganizeExecutesRunnerMeta):
    """A class that extends OrganizeExecutes to handle the choosing of the correct executors based on the config.yml.
    Attributes:
        _needs_delete (bool): if True, the CAOM repo action is delete/create instead of update.
        _reporter: An instance responsible for reporting the execution status.
    Methods:
        _choose():
            Determines which descendants of CaomExecute to instantiate based on the content of the config.yml
            file for an application.
    """

    def __init__(
            self,
            config,
            meta_visitors,
            data_visitors,
            needs_delete=False,
            store_transfer=None,
            modify_transfer=None,
            clients=None,
            reporter=None,
    ):
        super().__init__(
            config,
            meta_visitors,
            data_visitors,
            store_transfer=None,
            modify_transfer=None,
            clients=clients,
            reporter=reporter,
            needs_delete=False,
        )

    def _choose(self):
        """The logic that decides which descendants of CaomExecute to instantiate. This is based on the content of
        the config.yml file for an application.
        """
        super()._choose()
        if self._needs_delete:
            raise CadcException('No need identified for this yet.')

        if TaskType.VISIT in self.task_types:
                self._logger.debug(
                    f'Over-riding with executor MetricsPipeMetaVisit for tasks {self.task_types}.'
                )
                self._executors = []
                self._executors.append(
                    MetricsPipeVisit(self._clients, self.config, self._meta_visitors, self._reporter)
                )
        else:
            raise CadcException('wut')


class LogVisitor:
    def __init__(self, **kwargs):
        self.logger = logging.getLogger(__name__)
        self.storage_name = kwargs.get('storage_name', None)
        self.config = kwargs.get('config', None)

    def visit(self):
        self.logger.error('Begin visit')
        output_file = f'{self._config.collection}_cumulative_user_counts.csv'
        for source_name in self.storage_name.source_names:
            self.logger.error(f'Processing source name: {source_name}')
            # Load the log file
            log_bag = db.read_text(source_name).filter(
                lambda line: line.startswith('{') 
                and '"message":' not in line 
                and 'transaction' not in line
                and f'{self._config.scheme}:{self._config.collection}' in line
                and '"method":"GET",' in line
                and '"phase":"end",' in line
            ).map(my_load)
            # Convert to DataFrame
            meta = {
                'uri': 'object',
                'user': 'object',
            }
            log_df = log_bag.to_dataframe(meta=meta).compute()
            agg = log_df.groupby(['user']).agg(
                    total_requests=('user', 'size'),
            )
            if len(agg) > 0:
                agg.to_csv(output_file, mode='a', header=not path.exists(output_file), index=False)

            # Process the DataFrame as needed
            self.logger.info(f'Processed {len(log_df)} entries from {source_name.file_uri}')
        self.logger.error('End visit')


def visit(**kwargs):
    # ignore observation 
    return LogVisitor(**kwargs).visit()
