# CSD-4421
import io
import sys
import pandas as pd
from astropy.table import Table
from astropy.time import Time
from cadcutils import net
from cadctap import CadcTapClient


def _run_query(
    query_string,
    resource_id='ivo://cadc.nrc.ca/argus',
    hush=False,
    timeout=10,
    certificate='/usr/src/app/cadcproxy.pem',
):
    subject = net.Subject(certificate=certificate)
    if not hush:
        print('start')
    buffer = io.StringIO()
    client = CadcTapClient(subject, resource_id=resource_id)
    client.query(
        query_string,
        output_file=buffer,
        data_only=True,
        response_format='tsv',
        timeout=timeout,
    )
    temp = Table.read(buffer.getvalue().split('\n'), format='ascii.tab')
    if not hush:
        print(f'{len(temp)} end')
    return temp.to_pandas()


def z(x):
    try:
        return Time(x, format='mjd').to_value('iso', subfmt='date_hms')
    except Exception as e:
        return None


def query(collection):
    qs = f"""
    SELECT P.time_bounds_lower
    FROM caom2.Observation AS O
    JOIN caom2.Plane AS P ON O.obsID = P.obsID
    WHERE O.collection = '{collection}'
    """
    t = _run_query(qs)
    t['dt'] = t['time_bounds_lower'].apply(z)
    t['datetime'] = pd.to_datetime(t['dt'])
    year_counts = t.groupby(t['datetime'].dt.year).size()
    print(collection)
    print(year_counts)


def main():
    print(f'How many planes that were observed per year, that are available from CADC for {collection}.')
    collection = sys.argv[1]
    query(collection)


if __name__ == '__main__':
    main()
