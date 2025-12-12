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
import subprocess

from datetime import datetime
from glob import glob
from os import stat, path

from caom2pipe.data_source_composable import LocalFilesDataSourceRunnerMeta
from caom2pipe.execute_composable import CaomExecuteRunnerMeta, OrganizeExecutesRunnerMeta
from caom2pipe.manage_composable import CadcException, StorageName, TaskType
from metricsPipe.log_visit import LogVisitor


__all__ = ['HitsAndBytes']

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


def exec_cmd(cmd, log_level_as=logging.debug, timeout=None):
    """
    This does command execution as a subprocess call.

    :param cmd_array array of the command being executed, usually because
        it's of the form ['/bin/bash', '-c', 'what you really want to do']
    :param log_level_as control the logging level from the exec call
    :param timeout value in seconds, after which the process is terminated
        raising the TimeoutExpired exception.
    :return None
    """
    cmd_array = ['/bin/bash', '-c', cmd]
    cmd_text = ' '.join(ii for ii in cmd_array)
    try:
        child = subprocess.Popen(cmd_array, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            output, outerr = child.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            logging.warning(f'Command {cmd_array} timed out.')
            # child process is not killed if the timeout expires, so kill the
            # process and finish communication
            child.kill()
            child.stdout.close()
            child.stderr.close()
            output, outerr = child.communicate()
        if len(output) > 0:
            log_level_as(f'stdout {output.decode("utf-8")}')
        if len(outerr) > 0:
            logging.error(f'stderr {outerr.decode("utf-8")}')
        print(child.returncode)
        if child.returncode == 1:
            # what zgrep returns when nothing is found
            pass
        elif child.returncode != 0 and child.returncode != -9:
            # not killed due to a timeout
            logging.warning(f'Command {cmd_text} failed with {child.returncode}.')
            raise CadcException(
                f'Command {cmd_text} ::\nreturncode {child.returncode}, '
                f'\nstdout {output.decode("utf-8")}\' \nstderr '
                f'{outerr.decode("utf-8")}\''
            )
    except Exception as e:
        if isinstance(e, CadcException):
            raise e
        logging.warning(f'Error with command {cmd_text}:: {e}')
        logging.debug(traceback.format_exc())
        raise CadcException(f'Could not execute cmd {cmd_text}. Exception {e}')


def my_load(line):
    bits = line.split()
    try:
        if len(bits) in [18, 20, 21]:
            return {
                'bytes': int(bits[11]),
            }
        elif(
            ( len(bits) == 11 and 'SSL handshake failure' in line )
            or ( len(bits) == 14 and 'heartbeat attack' in line )
            or ( len(bits) == 14 and 'Proxy www-cadc stopped' in line )
        ):
            return {
                'bytes': 0,
            }
        else:
            print(len(bits))
            print(bits)
            return {
                'bytes': int(bits[11]),
            }
    except Exception as e:
        print(f"Error {e} {line}")
    return {}


class HitsAndBytes(LogVisitor):

    def visit(self):
        self.logger.debug('Begin visit')
        output_file = './hits_and_bytes_cumulative.csv'
        for source_name in self.storage_name.source_names:
            self.logger.info(f'Processing source name: {source_name}')

            # interim_name = source_name
            marker = source_name.split('-')[-1]
            grep_output_fqn = f'{self.config.working_directory}/grep_output.txt'
            grep_cmd = f'grep www-cadc {source_name} > {grep_output_fqn}'
            if source_name.endswith('.gz'):
                # gunzip --stdout /logs/dao-proxy-03/haproxy.log-20250811.gz > dao3.log-20250811
                # interim_name = f'{self.config.working_directory}/{path.basename(source_name.replace('.gz', ''))}/out.log'
                # exec_cmd(f'gunzip --stdout {source_name} > {interim_name}')
                grep_cmd = f'zgrep www-cadc {source_name} > {grep_output_fqn}'

            exec_cmd(grep_cmd)
            # Load the log file
            log_bag = db.read_text(grep_output_fqn).map(my_load)

            # Convert to DataFrame
            meta = {
                'bytes': 'int',
            }
            log_df = log_bag.to_dataframe(meta=meta).compute()
            agg = log_df.agg(total_bytes=('bytes', 'sum'))
            agg['marker'] = marker
            agg['hits'] = len(log_df)
            agg['gb'] = agg['bytes'] / 1024.0 / 1024.0 / 1024.0  # kb, MB, GB
            print(agg)
            if len(agg) > 0:
                agg.to_csv(output_file, mode='a', header=not path.exists(output_file), index=False)

            # Process the DataFrame as needed
            self.logger.info(f'Processed {len(log_df)} entries from {source_name}')
        self.logger.debug('End visit')


def visit(**kwargs):
    # ignore observation
    return HitsAndBytes(**kwargs).visit()
