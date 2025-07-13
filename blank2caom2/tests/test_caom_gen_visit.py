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
#  $Revision: 4 $
#
# ***********************************************************************
#

from mock import patch

from blank2caom2 import file2caom2_augmentation, main_app
from cadcdata import FileInfo
from caom2.diff import get_differences
from caom2pipe import astro_composable as ac
from caom2pipe import manage_composable as mc

import glob
import os


def pytest_generate_tests(metafunc):
    obs_id_list = glob.glob(f'{metafunc.config.invocation_dir}/data/*.fits.header')
    metafunc.parametrize('test_name', obs_id_list)


def test_main_app(test_name, test_config, tmp_path, change_test_dir):
    test_config.change_working_directory(tmp_path.as_posix())
    storage_name = main_app.BlankName([test_name])
    FileInfo(id=storage_name.file_uri, file_type='application/fits')
    headers = ac.make_headers_from_file(test_name)
    storage_name.file_info = {storage_name.file_uri: file_info}
    storage_name.metadata = {storage_name.file_uri: headers}
    test_reporter = ExecutionReporter2(test_config)
    kwargs = {
        'storage_name': storage_name,
        'reporter': test_reporter,
        'config': test_config,
    }
    expected_fqn = test_name.replace('.fits.header', '.expected.xml')
    in_fqn = expected_fqn.replace('.expected', '.in')
    actual_fqn = expected_fqn.replace('expected', 'actual')
    if os.path.exists(actual_fqn):
        os.unlink(actual_fqn)
    observation = None
    if os.path.exists(in_fqn):
        observation = mc.read_obs_from_file(in_fqn)
    observation = file2caom2_augmentation.visit(observation, **kwargs)
    if observation is None:
        assert False, f'Did not create observation for {test_name}'
    else:
        if os.path.exists(expected_fqn):
            expected = mc.read_obs_from_file(expected_fqn)
            compare_result = get_differences(expected, observation)
            if compare_result is not None:
                mc.write_obs_to_file(observation, actual_fqn)
                compare_text = '\n'.join([r for r in compare_result])
                msg = (
                    f'Differences found in observation {expected.observation_id}\n'
                    f'{compare_text}'
                )
                raise AssertionError(msg)
        else:
            mc.write_obs_to_file(observation, actual_fqn)
            assert False, f'nothing to compare to for {test_name}, missing {expected_fqn}'
    # assert False  # cause I want to see logging messages
