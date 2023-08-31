"""
Main module
"""

import datetime as dt
import json
import os
import shutil
import sqlite3
import cx_Oracle as cxo
import pyodbc
import func
import statement as stmt


def main():
    try:
        cwd = os.getcwd()
        today = dt.datetime.today()
        if 'HOME' in os.environ:
            root = os.environ['HOME']
        else:
            root = os.environ['HOMEDRIVE'] + os.environ['HOMEPATH']
        rootop1 = os.path.join(root, 'slate_sync_vars')
        rootop2 = 'C:\\slate_sync_vars'
        if os.path.exists(rootop1):
            root = rootop1
        else:
            root = rootop2
        localdb = 'temp{}.db'.format(today.strftime('%Y%m%d%H%M%S'))
        with open(os.path.join(root, 'connect.json')) as file:
            connop = json.load(file)
        with open(os.path.join(root, 'qvars.json')) as file:
            qvars = json.load(file)
        if not (func.validate_keys(connop, ('oracle', 'sqlserver'))
                and func.validate_keys(connop['sqlserver'],
                                  ('driver',
                                   'host',
                                   'database',
                                   'user',
                                   'password'))
                and func.validate_keys(connop['oracle'],
                                  ('user',
                                   'password',
                                   'host',
                                   'port',
                                   'service_name'))
                and func.validate_keys(qvars, ('oracle',))
                and func.validate_keys(qvars['oracle'], ('termlb', 'termub'))):
            raise KeyError('JSON files malformed; refer to README.')
        for tdir in ['audit', 'update', '.archive']:
            testdir = os.path.join(cwd, tdir)
            if not os.path.exists(testdir):
                os.mkdir(testdir)
    except (KeyError, OSError, json.JSONDecodeError) as e:
        print(repr(e))
    else:
        try:
            lconn = sqlite3.connect(localdb)

            # Setup local database
            lcur = lconn.cursor()
            lcur.execute('DROP TABLE IF EXISTS orabase')
            lcur.execute('DROP TABLE IF EXISTS oraaux1')
            lcur.execute('DROP TABLE IF EXISTS oraaux2')
            lcur.execute('DROP TABLE IF EXISTS mssbase')
            lcur.execute('DROP TABLE IF EXISTS mssaux1')
            lcur.execute('DROP TABLE IF EXISTS mssaux2')
            lcur.execute('DROP TABLE IF EXISTS oraref1')
            lcur.execute('DROP TABLE IF EXISTS oraref2')
            lcur.execute('DROP TABLE IF EXISTS oraref3')
            lconn.commit()
            lcur.execute(stmt.ct_orb)
            lcur.execute(stmt.ct_orx1)
            lcur.execute(stmt.ct_orx2)
            lcur.execute(stmt.ct_msb)
            lcur.execute(stmt.ct_msx1)
            lcur.execute('CREATE TABLE mssaux2 (emplid text, column integer)')
            lcur.execute('CREATE TABLE mssaux3 (emplid text)')
            lcur.execute('CREATE TABLE oraref1 (acad_prog text, acad_plan text)')
            lcur.execute('CREATE TABLE oraref2 (prog_action text, prog_reason text)')
            lcur.execute('CREATE TABLE oraref3 (prog_status text, prog_action text unique, rank int)')
            lconn.commit()
            lcur.execute('CREATE INDEX orab ON orabase (emplid, adm_appl_nbr)')
            lcur.execute('CREATE INDEX orax1 ON oraaux1 (emplid, adm_appl_nbr)')
            lcur.execute('CREATE INDEX orax2 ON oraaux2 (emplid)')
            lcur.execute('CREATE INDEX mssb ON mssbase (emplid, adm_appl_nbr)')
            lcur.execute('CREATE INDEX mssx1 ON mssaux1 (emplid)')
            lcur.execute('CREATE INDEX mssx2 ON mssaux2 (emplid)')
            lcur.execute('CREATE INDEX mssx3 ON mssaux3 (emplid)')
            lcur.execute('CREATE INDEX orar1 ON oraref1 (acad_prog, acad_plan)')
            lcur.execute('CREATE INDEX orar2 ON oraref2 (prog_action, prog_reason)')
            lcur.execute('CREATE INDEX orar3 ON oraref3 (prog_status, prog_action)')
            lconn.commit()
            lcur.executemany('INSERT INTO oraref2 VALUES (?, ?)', [
                    ('APPL', ' '),
                    ('WAPP', ' '),
                    ('WADM', ' ')])
            lcur.executemany('INSERT INTO oraref3 VALUES (?, ?, ?)', [
                    ('AP', 'APPL', 0),
                    ('AP', 'DDEF', 1),
                    ('AD', 'ADMT', 2),
                    ('CN', 'DENY', 2),
                    ('AC', 'MATR', 3),
                    ('CN', 'WAPP', 3),
                    ('CN', 'WADM', 3)])
            lconn.commit()

            # Retrieve data from SQL Server database
            with pyodbc.connect(driver=connop['sqlserver']['driver'],
                                server=connop['sqlserver']['host'],
                                database=connop['sqlserver']['database'],
                                uid=connop['sqlserver']['user'],
                                pwd=connop['sqlserver']['password']) as conn:
                print(f'Connected to SQL Server database, driver version: {conn.getinfo(pyodbc.SQL_DRIVER_VER)}\n')
                with conn.cursor() as cur:
                    cur.execute(stmt.qi_msb)
                    fc = 0
                    while True:
                        rows = cur.fetchmany(500)
                        if not rows:
                            print(f'\nFetched and inserted {cur.rowcount} total rows.\n\n')
                            break
                        lcur.executemany('INSERT INTO mssbase VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', rows)
                        lconn.commit()
                        print(f'Fetched and inserted from row {fc*500 + 1}...')
                        fc += 1
                    lcur.execute("UPDATE mssbase SET prog_reason = ' ' WHERE prog_reason IS NULL")
                    lconn.commit()
                    cur.execute(stmt.qi_msx1)
                    fc = 0
                    while True:
                        rows = cur.fetchmany(500)
                        if not rows:
                            print(f'\nFetched and inserted {cur.rowcount} total rows.\n\n')
                            break
                        lcur.executemany('INSERT INTO mssaux1 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', rows)
                        lconn.commit()
                        print(f'Fetched and inserted from row {fc*500 + 1}...')
                        fc += 1
                    cur.execute(stmt.qi_msx3)
                    fc = 0
                    while True:
                        rows = cur.fetchmany(500)
                        if not rows:
                            print(f'\nFetched and inserted {cur.rowcount} total rows.\n\n')
                            break
                        lcur.executemany('INSERT INTO mssaux3 VALUES (?)', rows)
                        lconn.commit()
                        print(f'Fetched and inserted from row {fc*500 + 1}...')
                        fc += 1
            conn.close()

            # Retrieve data from Oracle database
            with cxo.connect(connop['oracle']['user'],
                             connop['oracle']['password'],
                             cxo.makedsn(connop['oracle']['host'],
                                         connop['oracle']['port'],
                                         service_name=connop['oracle']['service_name'])) as conn:
                print(f'Connected to Oracle database, driver version: {conn.version}\n')
                with conn.cursor() as cur:
                    cur.execute(stmt.qi_orb, qvars['oracle'])
                    fc = 0
                    while True:
                        rows = cur.fetchmany(500)
                        if not rows:
                            print(f'\nFetched and inserted {cur.rowcount} total rows.\n\n')
                            break
                        lcur.executemany('INSERT INTO orabase VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', rows)
                        lconn.commit()
                        print(f'Fetched and inserted from row {fc*500 + 1}...')
                        fc += 1
                    cur.execute(stmt.qi_orx1, qvars['oracle'])
                    fc = 0
                    while True:
                        rows = cur.fetchmany(500)
                        if not rows:
                            print(f'\nFetched and inserted {cur.rowcount} total rows.\n\n')
                            break
                        lcur.executemany('INSERT INTO oraaux1 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', rows)
                        lconn.commit()
                        print(f'Fetched and inserted from row {fc*500 + 1}...')
                        fc += 1
                    cur.execute(stmt.qi_orx2, qvars['oracle'])
                    fc = 0
                    while True:
                        rows = cur.fetchmany(500)
                        if not rows:
                            print(f'\nFetched and inserted {cur.rowcount} total rows.\n\n')
                            break
                        lcur.executemany('INSERT INTO oraaux2 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', rows)
                        lconn.commit()
                        print(f'Fetched and inserted from row {fc*500 + 1}...')
                        fc += 1
                    cur.execute(stmt.qi_orr1)
                    fc = 0
                    while True:
                        rows = cur.fetchmany(500)
                        if not rows:
                            print(f'\nFetched and inserted {cur.rowcount} total rows.\n\n')
                            break
                        lcur.executemany('INSERT INTO oraref1 VALUES (?, ?)', rows)
                        lconn.commit()
                        print(f'Fetched and inserted from row {fc*500 + 1}...')
                        fc += 1
                    cur.execute(stmt.qi_orr2)
                    fc = 0
                    while True:
                        rows = cur.fetchmany(500)
                        if not rows:
                            print(f'\nFetched and inserted {cur.rowcount} total rows.\n\n')
                            break
                        lcur.executemany('INSERT INTO oraref2 VALUES (?, ?)', rows)
                        lconn.commit()
                        print(f'Fetched and inserted from row {fc*500 + 1}...')
                        fc += 1

            # Query local database
            row_metauser = '\'slate_sync - ' + connop['oracle']['user'].upper() + '\''
            row_metadttm = 'SYSDATE'
            row_metadata = (row_metauser, row_metadttm)

            lcur.execute(stmt.q0001)
            func.query_to_csv(os.path.join(cwd, 'audit', 'INVALID_PP_COMBO.csv'), lcur)
            lcur.execute(stmt.q0002)
            func.query_to_csv(os.path.join(cwd, 'audit', 'INVALID_AR_COMBO.csv'), lcur)
            lcur.execute(stmt.q0003)
            func.query_to_csv(os.path.join(cwd, 'audit', 'INVALID_ACTION_UPDATE.csv'), lcur)
            lcur.execute(stmt.q0004)
            func.query_to_csv(os.path.join(cwd, 'audit', 'CHANGES_TO_LOCKED_APPLICATIONS.csv'), lcur)

            lcur.execute(stmt.q0006)
            ldata = func.query_to_csv(os.path.join(cwd, 'update', 'TYPE_CHANGE.csv'),
                                 lcur,
                                 [0, 1, 2],
                                 os.path.join(cwd, '.archive', 'TYPE_CHANGE_{}.csv'.format(today.strftime('%Y%m%d'))))
            func.query_to_update(os.path.join(cwd, 'update', 'update_type.txt'),
                            'PS_ADM_APPL_DATA',
                            ldata,
                            ['ADMIT_TYPE'],
                            row_metadata,
                            archivename=os.path.join(cwd, '.archive', 'update_type_{}.txt'.format(today.strftime('%Y%m%d'))),
                            static_targets=[('ADM_UPDATED_DT', 'TRUNC(SYSDATE)'), ('ADM_UPDATED_BY', row_metauser)])
            lcur.execute(stmt.q0007)
            ldata = func.query_to_csv(os.path.join(cwd, 'update', 'LEVEL_CHANGE.csv'),
                                 lcur,
                                 [0, 1, 3],
                                 os.path.join(cwd, '.archive', 'LEVEL_CHANGE_{}.csv'.format(today.strftime('%Y%m%d'))))
            func.query_to_update(os.path.join(cwd, 'update', 'update_level.txt'),
                            'PS_ADM_APPL_DATA',
                            ldata,
                            ['ACADEMIC_LEVEL'],
                            row_metadata,
                            archivename=os.path.join(cwd, '.archive', 'update_level_{}.txt'.format(today.strftime('%Y%m%d'))),
                            static_targets=[('ADM_UPDATED_DT', 'TRUNC(SYSDATE)'), ('ADM_UPDATED_BY', row_metauser)])
            lcur.execute(stmt.q0008)
            ldata = func.query_to_csv(os.path.join(cwd, 'update', 'TERM_CHANGE.csv'),
                                 lcur,
                                 [0, 1, 4],
                                 os.path.join(cwd, '.archive', 'TERM_CHANGE_{}.csv'.format(today.strftime('%Y%m%d'))))
            func.query_to_update(os.path.join(cwd, 'update', 'update_term_prog.txt'),
                            'PS_ADM_APPL_PROG',
                            ldata,
                            ['ADMIT_TERM', 'REQ_TERM'],
                            row_metadata,
                            archivename=os.path.join(cwd, '.archive', 'update_term_prog_{}.txt'.format(today.strftime('%Y%m%d'))))
            func.query_to_update(os.path.join(cwd, 'update', 'update_term_plan.txt'),
                            'PS_ADM_APPL_PLAN',
                            ldata,
                            ['REQ_TERM'],
                            row_metadata,
                            archivename=os.path.join(cwd, '.archive', 'update_term_plan_{}.txt'.format(today.strftime('%Y%m%d'))))
            lcur.execute(stmt.q0009)
            ldata = func.query_to_csv(os.path.join(cwd, 'update', 'PROG_CHANGE.csv'),
                                 lcur,
                                 [0, 1, 5],
                                 os.path.join(cwd, '.archive', 'PROG_CHANGE_{}.csv'.format(today.strftime('%Y%m%d'))))
            func.query_to_update(os.path.join(cwd, 'update', 'update_prog.txt'),
                            'PS_ADM_APPL_PROG',
                            ldata,
                            ['ACAD_PROG'],
                            row_metadata,
                            archivename=os.path.join(cwd, '.archive', 'update_prog_{}.txt'.format(today.strftime('%Y%m%d'))))
            lcur.execute(stmt.q0010)
            ldata = func.query_to_csv(os.path.join(cwd, 'update', 'PLAN_CHANGE.csv'),
                                 lcur,
                                 [0, 1, 6],
                                 os.path.join(cwd, '.archive', 'PLAN_CHANGE_{}.csv'.format(today.strftime('%Y%m%d'))))
            func.query_to_update(os.path.join(cwd, 'update', 'update_plan.txt'),
                            'PS_ADM_APPL_PLAN',
                            ldata,
                            ['ACAD_PLAN'],
                            row_metadata,
                            archivename=os.path.join(cwd, '.archive', 'update_plan_{}.txt'.format(today.strftime('%Y%m%d'))))
            lcur.execute(stmt.q0011)
            ldata = func.query_to_csv(os.path.join(cwd, 'update', 'FEE_STATUS_CHANGE.csv'),
                                 lcur,
                                 [0, 1, 9],
                                 os.path.join(cwd, '.archive', 'FEE_STATUS_CHANGE_{}.csv'.format(today.strftime('%Y%m%d'))))
            func.query_to_update(os.path.join(cwd, 'update', 'update_fee_status.txt'),
                            'PS_ADM_APPL_DATA',
                            ldata,
                            ['APPL_FEE_STATUS'],
                            row_metadata,
                            archivename=os.path.join(cwd, '.archive', 'update_fee_status_{}.txt'.format(today.strftime('%Y%m%d'))),
                            static_targets=[('APPL_FEE_DT', 'TRUNC(SYSDATE)'), ('ADM_UPDATED_DT', 'TRUNC(SYSDATE)'), ('ADM_UPDATED_BY', row_metauser)])
            lcur.execute(stmt.q0012)
            ldata = func.query_to_csv(os.path.join(cwd, 'update', 'REASON_CHANGE.csv'),
                                 lcur,
                                 [0, 1, 8, 26, 27],
                                 os.path.join(cwd, '.archive', 'REASON_CHANGE_{}.csv'.format(today.strftime('%Y%m%d'))))
            func.query_to_update(os.path.join(cwd, 'update', 'update_reason.txt'),
                            'PS_ADM_APPL_PROG',
                            ldata,
                            ['PROG_REASON'],
                            row_metadata,
                            ['EFFDT', 'EFFSEQ'],
                            [('TO_DATE(', ', \'YYYY-MM-DD\')'), ('', '')],
                            os.path.join(cwd, '.archive', 'update_reason_{}.txt'.format(today.strftime('%Y%m%d'))))
            lcur.execute(stmt.q0013)
            ldata = func.query_to_csv(os.path.join(cwd, 'update', 'ACTION_CHANGE.csv'),
                                 lcur,
                                 [0, 1, 7, *range(21, 60)],
                                 os.path.join(cwd, '.archive', 'ACTION_CHANGE_{}.csv'.format(today.strftime('%Y%m%d'))))
            sdata = func.filter_rows_by_val(ldata, 2, 'ADMT')
            func.query_to_update(os.path.join(cwd, 'update', 'update_dep_calc_needed.txt'),
                            'PS_ADM_APP_CAR_SEQ',
                            sdata,
                            archivename=os.path.join(cwd, '.archive', 'update_dep_calc_needed_{}.txt'.format(today.strftime('%Y%m%d'))),
                            static_targets=[('DEP_CALC_NEEDED', '\'Y\'')])
            sdata = func.filter_rows_by_val(ldata, 2, 'MATR')
            func.query_to_update(os.path.join(cwd, 'update', 'update_create_prog_status.txt'),
                            'PS_ADM_APP_CAR_SEQ',
                            sdata,
                            archivename=os.path.join(cwd, '.archive', 'update_create_prog_status_{}.txt'.format(today.strftime('%Y%m%d'))),
                            static_targets=[('CREATE_PROG_STATUS', '\'R\'')])
            ids = set()
            if ldata:
                stmt_groups = []
                excerpt = ''
                for i, row in enumerate(ldata):
                    if (i % 250) == 0 and i > 0:
                        stmt_groups.append(excerpt)
                        excerpt = ''
                    effseq = (str(row[9] + 1) if dt.datetime.strptime(row[8], '%Y-%m-%d').date() == today.date() else '1')
                    excerpt += '  INTO PS_ADM_APPL_PROG VALUES ({})\n'.format(
                            ', '.join(func.prep_sql_vals(*row[3:8]))
                            + ', TRUNC(SYSDATE), {}, '.format(effseq)
                            + ', '.join(func.prep_sql_vals(*row[10:14]))
                            + ', TRUNC(SYSDATE), '
                            + ', '.join(func.prep_sql_vals(*row[15:28]))
                            + ', '
                            + ', '.join([*row_metadata, *row_metadata]))
                    excerpt += '  INTO PS_ADM_APPL_PLAN VALUES ({})\n'.format(
                            ', '.join(func.prep_sql_vals(*row[29:34]))
                            + ', TRUNC(SYSDATE), {}, '.format(effseq)
                            + ', '.join(func.prep_sql_vals(row[36]))
                            + ', TO_DATE({}, \'YYYY-MM-DD\'), '.format(*func.prep_sql_vals(row[34]))
                            + ', '.join(func.prep_sql_vals(*row[38:]))
                            + ', '
                            + ', '.join([*row_metadata, *row_metadata]))
                    ids.add(row[0])
                stmt_groups.append(excerpt)
                while True:
                    try:
                        with open(os.path.join(cwd, 'update', 'insert_action.txt'), 'w') as file:
                            for row in stmt_groups:
                                file.write('INSERT ALL\n{}SELECT * FROM dual;\n'.format(row))
                        shutil.copyfile(os.path.join(cwd, 'update', 'insert_action.txt'),
                                        os.path.join(cwd, '.archive', 'insert_action_{}.txt'.format(today.strftime('%Y%m%d'))))
                        break
                    except OSError as e:
                        print(str(e))
                        input('Ensure that the file or directory is not open or locked, then press any enter to try again.')

            # Auxiliary queries
            ffids = []
            lcur.execute(stmt.q0014)
            counter = 0
            while True:
                frows = lcur.fetchmany(500)
                if not frows:
                    print(f'\nFetched and wrote {lcur.rowcount} total rows.\n\n')
                    break
                print(f'Fetched and wrote from row {counter * 500 + 1}...')
                counter += 1
                # Filter for illegal characters
                for row in frows:
                    for letter in row[1]:
                        if not (31 < ord(letter) < 127):
                            ffids.append((row[0], 1))
                            break
            lcur.execute(stmt.q0015)
            counter = 0
            while True:
                frows = lcur.fetchmany(500)
                if not frows:
                    print(f'\nFetched and wrote {lcur.rowcount} total rows.\n\n')
                    break
                print(f'Fetched and wrote from row {counter * 500 + 1}...')
                counter += 1
                # Filter for illegal characters
                for row in frows:
                    for num in range(11, 17):
                        if row[num] is not None:
                            for letter in row[num]:
                                if not (31 < ord(letter) < 127):
                                    ffids.append((row[0], num))
                                    break
            lcur.execute(stmt.q0016)
            counter = 0
            while True:
                frows = lcur.fetchmany(500)
                if not frows:
                    print(f'\nFetched and wrote {lcur.rowcount} total rows.\n\n')
                    break
                print(f'Fetched and wrote from row {counter * 500 + 1}...')
                counter += 1
                # Filter for illegal characters
                for row in frows:
                    for num in range(5, 11):
                        if row[num] is not None:
                            for letter in row[num]:
                                if not (31 < ord(letter) < 127):
                                    ffids.append((row[0], num))
                                    break
            lcur.execute(stmt.q0017)
            counter = 0
            while True:
                frows = lcur.fetchmany(500)
                if not frows:
                    print(f'\nFetched and wrote {lcur.rowcount} total rows.\n\n')
                    break
                print(f'Fetched and wrote from row {counter * 500 + 1}...')
                counter += 1
                # Filter for illegal characters
                for row in frows:
                    for letter in row[2]:
                        if not (31 < ord(letter) < 127):
                            ffids.append((row[0], 2))
                            break
            lcur.execute(stmt.q0018)
            counter = 0
            while True:
                frows = lcur.fetchmany(500)
                if not frows:
                    print(f'\nFetched and wrote {lcur.rowcount} total rows.\n\n')
                    break
                print(f'Fetched and wrote from row {counter * 500 + 1}...')
                counter += 1
                # Filter for illegal characters
                for row in frows:
                    for letter in row[3]:
                        if not (31 < ord(letter) < 127):
                            ffids.append((row[0], 3))
                            break
            lcur.execute(stmt.q0019)
            counter = 0
            while True:
                frows = lcur.fetchmany(500)
                if not frows:
                    print(f'\nFetched and wrote {lcur.rowcount} total rows.\n\n')
                    break
                print(f'Fetched and wrote from row {counter * 500 + 1}...')
                counter += 1
                # Filter for illegal characters
                for row in frows:
                    for letter in row[4]:
                        if not (31 < ord(letter) < 127):
                            ffids.append((row[0], 4))
                            break
            if ffids:
                lcur.executemany('INSERT INTO mssaux2 VALUES (?, ?)', ffids)
                lconn.commit()
            lcur.execute(stmt.q0049)
            func.query_to_csv(os.path.join(cwd, 'audit', 'ILLEGAL_CHAR_DATA.csv'), lcur)
            lcur.execute(stmt.q0050)
            func.query_to_csv(os.path.join(cwd, 'update', 'BIO_DEMO_CHANGE.csv'),
                                 lcur,
                                 archivename=os.path.join(cwd, '.archive', 'BIO_DEMO_CHANGE_{}.csv'.format(today.strftime('%Y%m%d'))),
                                 header=False)
            if ids:
                stmt_groups = []
                excerpt = ''
                for i, member in enumerate(ids):
                    if (i % 500) == 0 and i > 0:
                        stmt_groups.append(excerpt)
                        excerpt = ''
                    excerpt += '  INTO PS_L_DIRXML VALUES ({})\n'.format(*func.prep_sql_vals(member))
                stmt_groups.append(excerpt)
                while True:
                    try:
                        with open(os.path.join(cwd, 'update', 'insert_dirxml.txt'), 'w') as file:
                            for row in stmt_groups:
                                file.write('INSERT ALL\n{}SELECT * FROM dual;\n'.format(row))
                        shutil.copyfile(os.path.join(cwd, 'update', 'insert_dirxml.txt'),
                                        os.path.join(cwd, '.archive', 'insert_dirxml_{}.txt'.format(today.strftime('%Y%m%d'))))
                        break
                    except OSError as e:
                        print(str(e))
                        input('Ensure that the file or directory is not open or locked, then press any enter to try again.')

            # Cleanup local database
            lcur.execute('DROP TABLE IF EXISTS orabase')
            lcur.execute('DROP TABLE IF EXISTS oraaux1')
            lcur.execute('DROP TABLE IF EXISTS oraaux2')
            lcur.execute('DROP TABLE IF EXISTS mssbase')
            lcur.execute('DROP TABLE IF EXISTS mssaux1')
            lcur.execute('DROP TABLE IF EXISTS oraref1')
            lcur.execute('DROP TABLE IF EXISTS oraref2')
            lcur.execute('DROP TABLE IF EXISTS oraref3')
            lconn.commit()
        except (pyodbc.DatabaseError) as e:
            conn.close()
            print(str(e))
        except (OSError, cxo.Error, sqlite3.DatabaseError) as e:
            print(str(e))
        finally:
            lconn.rollback()
            lcur.close()
            lconn.close()
            os.remove(localdb)
    finally:
        input('Press enter to finish...')


if __name__ == '__main__':
    main()
