import base64
import csv
from collections import defaultdict


# first, run this query on the host that is named below:
# ldapsearch -W -H ldaps://ns1-05.cadc.dao.nrc.ca -D uid=webproxy,ou=SpecialUsers,dc=canfar,dc=net -b ou=Users,ou=ds,dc=canfar,dc=net cn sn givenName email memberOf > ldap.csv
# this gives all the names of CADC/CANFAR users, their last name (sn), given name (givenName), email address, and
# CADC user id (cn)
#
# then, on prolix, do these things:
# prolix-02:/var/log/logging-data/site/cadc>find . -name 'access.cadcsite-*-20*.log.gz' -mtime -365
# this finds all the access log files for the last year
# then use jq to trawl those log files for successful logins:
#
# for ii in $( cat lgz ); do zgrep '^{.*}$' /var/log/logging-data/site/cadc/$ii | jq -r 'try select(.path=="/access/login" and .success==true) | .user' ; done | sort -u > unique_users_one_year_20260220.txt
#
# then, change the input and output file names, and run this python script, and the file that is produced will
# satisfy CADC-15156 criteria.

input_file = "ldap_all_users_20260220.out"  ## grep email ldap_all_users_20260219.out | wc -l 9981 entries
canfar_users_last_year_input_file = "unique_users_one_year_20260220.txt"
all_users_output_file = "all_users_20260220.csv"
cadc_users_output_file = "cadc_users_20260220.csv"
canfar_users_output_file = "canfar_users_20260220.csv"
canfar_users_one_year_output_file = "canfar_users_one_year_20260220.csv"


user_type = ['CADC User', 'CANFAR User', 'CADC Staff']
cadc_staff_users = [
    'geoffrey.melnychuk@nrc.ca',
    'Geoffrey.Melnychuk@nrc-cnrc.gc.ca',
    'kevin.casteels@nrc.gc.ca',
    'kcasteels@gmail.com',
    'lingcadc@gmail.com',
    'Dustin.Jenkins@nrc-cnrc.gc.ca',
    'dustin@test.com',
    'at88mph@shaw.ca',
    'at88dustin@test.com',
    'attest@test.com',
    'at88mph@hotmail.com',
    'djenkins.cadc@gmail.com',
    'cyclodaddy@shaw.ca',
    'charanjot.brar@mcgill.ca',
    'shiny.brar@nrc-cnrc.gc.ca',
    'isabella.ghiurea@nrc-cnrc.gc.ca',
    'Stephen.Gwyn@astrsp-mrs.fr',
    'gwyn.cadc@gmail.com',
    'stephen.gwyn@nrc.ca',
    'Stephen.Gwyn@nrc-cnrc.gc.ca',
    'sharon.goliath@nrc-cnrc.gc.ca',
    'Severin.Gaudet@nrc-cnrc.gc.ca',
    'majorb@nrc-cnrc.gc.ca',
    'major.brian@gmail.com',
    'brian.major@nrc-cnrc.gc.ca',
    'Adrian.Damian@nrc.ca',
    'pdowler.cadc@gmail.com',
    'chris.willott@nrc.ca',
    'tobiashenrybrown@gmail.com',
    'sfabbro@uvic.ca',
    'seb@supernova.ist.utl.pt',
    'sebastien.fabbro@nrc-cnrc.gc.ca',
    'hossteim@uvic.ca',
    'JJ.Kavelaars@nrc-cnrc.gc.ca',
    'jjk@uvic.ca',
    'jjkavelaars.cadc@gmail.com',
    'john.ouellette@gmail.com',
    'john.ouellette@nrc-cnrc.gc.ca',
    'cadc@nrc-cnrc.gc.ca',
    'ling.shao@nrc-cnrc.gc.ca',
    'Ling.Shao@nrc.gc.ca',
    'david.bohlender@nrc-cnrc.gc.ca',
    'david.bohlender@gmail.com',
    'jeff.burke@nrc-cnrc.gc.ca',
]


def add_record(record, records, all_keys, last_years_canfar_users):
    if record:
        sn = record.get('sn')
        email = record.get('email')
        cn = record.get('cn')
        if sn is None or '$EXTERNAL-SN' in sn or (email is not None and len(email) > 0 and 'adass2022' in email[0]):
            return

        if email is not None and email[0] in cadc_staff_users:
            record['userType'] = 'CADC Staff'
        else:
            members = record.get('memberOf')
            if members is not None and 'cn=skaha-users,ou=Groups,ou=ds,dc=canfar,dc=net' in members:
                record['userType'] = 'CANFAR User'
            else:
                record['userType'] = 'CADC User'

        if cn is not None and len(cn) > 0:
            cn = cn[0]
        record['lastYear'] = cn in last_years_canfar_users

        if 'memberOf' in record.keys():
            del record['memberOf']
        if 'dn' in record.keys():
            del record['dn']
        records.append(record.copy())
        all_keys.update(record.keys())


def flatten_record(record):
    return {k: ";".join(v) if isinstance(v, list) else v for k, v in record.items()}


def flatten_mail_record(record):
    return {k: ";".join(v) if isinstance(v, list) else v for k, v in record.items() if k in ['sn', 'givenName', 'email']}


def output_all_the_files(records):
    with open(all_users_output_file, "w", newline='') as f_all, \
        open(cadc_users_output_file, "w", newline='') as f_cadc, \
        open(canfar_users_output_file, "w", newline='') as f_canfar, \
        open(canfar_users_one_year_output_file, "w", newline='') as f_canfar_last_year:
        field_names = ['sn', 'givenName', 'email']
        all_writer = csv.DictWriter(f_all, fieldnames=field_names)
        all_writer.writeheader()
        cadc_writer = csv.DictWriter(f_cadc, fieldnames=field_names)
        cadc_writer.writeheader()
        canfar_writer = csv.DictWriter(f_canfar, fieldnames=field_names)
        canfar_writer.writeheader()
        canfar_one_year_writer = csv.DictWriter(f_canfar_last_year, fieldnames=field_names)
        canfar_one_year_writer.writeheader()
        for record in records:
            row = flatten_mail_record(record)
            all_writer.writerow(row)
            if record['userType'] == 'CANFAR User':
                canfar_writer.writerow(row)
                if record['lastYear']:
                    canfar_one_year_writer.writerow(row)
            else:
                cadc_writer.writerow(row)


def main():
    records = []
    current_record = defaultdict(list)
    all_keys = set()
    last_years_canfar_users = []

    with open(canfar_users_last_year_input_file, 'r') as f:
        last_years_canfar_users = [line.strip() for line in f if line.strip()]

    # print(last_years_canfar_users)

    with open(input_file, "r") as f:
        # Parse LDIF records from the input file
        for line in f:
            line = line.strip()
            if not line:
                continue
            if line.startswith('#'):
                continue
            if ":" in line:
                key, value = line.split(":", 1)
                if key in ['result', 'search']:
                    continue
                value = value.strip()
                key = key.strip()
                # Check for base64 encoded values (RFC 2849: double colon)
                if line.count(":") == 2 and line.split(":", 2)[1] == "":
                    try:
                        value = base64.b64decode(value).decode('utf-8', errors='replace')
                    except Exception as e:
                        print(f'key {key} value {value} current record {current_record}')
                current_record[key].append(value)
            if line.startswith("dn:") and current_record:
                add_record(current_record, records, all_keys, last_years_canfar_users)
                current_record = defaultdict(list)
        # Add the last record
        add_record(current_record, records, all_keys, last_years_canfar_users)

    output_all_the_files(records)


if __name__ == '__main__':
    main()
