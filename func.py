"""
Function module
"""

import csv
import shutil
from collections import defaultdict
from tempfile import TemporaryFile


codex = defaultdict(lambda: '?', {
    chr(161): '!',
    chr(166): '|',
    chr(169): 'C',
    chr(170): '^a',
    chr(171): '<<',
    chr(174): 'R',
    chr(176): '^o',
    chr(177): '+-',
    chr(178): '^2',
    chr(179): '^3',
    chr(180): '\'',
    chr(185): '^1',
    chr(186): '^o',
    chr(187): '>>',
    chr(188): '1/4',
    chr(188): '1/2',
    chr(188): '3/4',
    chr(191): '?',
    chr(192): 'A',
    chr(193): 'A',
    chr(194): 'A',
    chr(195): 'A',
    chr(196): 'A',
    chr(197): 'A',
    chr(198): 'Ae',
    chr(199): 'C',
    chr(200): 'E',
    chr(201): 'E',
    chr(202): 'E',
    chr(203): 'E',
    chr(204): 'I',
    chr(205): 'I',
    chr(206): 'I',
    chr(207): 'I',
    chr(208): 'Dh',
    chr(209): 'N',
    chr(210): 'O',
    chr(211): 'O',
    chr(212): 'O',
    chr(213): 'O',
    chr(214): 'O',
    chr(216): 'O',
    chr(217): 'U',
    chr(218): 'U',
    chr(219): 'U',
    chr(220): 'U',
    chr(221): 'Y',
    chr(222): 'Th',
    chr(223): 'ss',
    chr(224): 'a',
    chr(225): 'a',
    chr(226): 'a',
    chr(227): 'a',
    chr(228): 'a',
    chr(229): 'a',
    chr(230): 'ae',
    chr(231): 'c',
    chr(232): 'e',
    chr(233): 'e',
    chr(234): 'e',
    chr(235): 'e',
    chr(236): 'i',
    chr(237): 'i',
    chr(238): 'i',
    chr(239): 'i',
    chr(240): 'dh',
    chr(241): 'n',
    chr(242): 'o',
    chr(243): 'o',
    chr(244): 'o',
    chr(245): 'o',
    chr(246): 'o',
    chr(248): 'o',
    chr(249): 'u',
    chr(250): 'u',
    chr(251): 'u',
    chr(252): 'u',
    chr(253): 'y',
    chr(254): 'th',
    chr(255): 'y',
    chr(8211): '-',
    chr(8212): '-',
    chr(8213): '-',
    chr(8215): '_',
    chr(8216): '\'',
    chr(8217): '\'',
    chr(8218): ',',
    chr(8219): '\'',
    chr(8220): '"',
    chr(8221): '"',
    chr(8222): ',,',
    chr(8224): '+',
    chr(8225): '+',
    chr(8226): '*',
    chr(8230): '...',
    chr(8240): '%',
    chr(8242): '\'',
    chr(8243): '\"',
    chr(8249): '<',
    chr(8250): '>',
    chr(8252): '!!',
    chr(8260): '/'
})


def validate_keys(srcdict, keys=tuple()):
    for key in keys:
        if key not in srcdict:
            return False
    for key in srcdict:
        if key not in keys:
            return False
    return True


def prep_sql_vals(*args):
    prepped = []
    for value in args:
        if isinstance(value, str):
            prepped.append('\'' + value.replace('\'', '\'\'') + '\'')
        else:
            prepped.append(str(value))
    return prepped


def filter_rows_by_val(iterable, index, value):
    filtered = []
    for row in iterable:
        if row[index] == value:
            filtered.append(row)
    return filtered


def query_to_csv(filename, cursor, return_indices=None, archivename=None, header=True):
    """
    If archivename is supplied, it should be a path string for
    an "archived" copy of the file.
    """
    return_data = []
    with TemporaryFile('r+', newline='') as tfile:
        twriter = csv.writer(tfile)
        header_row = []
        should_return = False
        for row in cursor.description:
            if len(row) > 0:
                header_row.append(row[0])
        if header:
            twriter.writerow(header_row)
        if return_indices:
            for index in return_indices:
                if len(header_row) >= index:
                    should_return = True

        counter = 0
        while True:
            frows = cursor.fetchmany(500)
            if not frows:
                print(f'\nFetched and wrote {cursor.rowcount} total rows.\n\n')
                break
            print(f'Fetched and wrote from row {counter*500 + 1}...')
            counter += 1
            twriter.writerows(frows)
            if should_return:
                for row in frows:
                    return_row = []
                    for index in return_indices:
                        return_row.append(row[index])
                    return_data.append(return_row)

        write_perm = False
        tfile.seek(0)
        treader = csv.reader(tfile)
        for i, row in enumerate(treader):
            if i == 1:
                write_perm = True
                break
        if write_perm:
            tfile.seek(0)
            while True:
                try:
                    with open(filename, 'w', newline='') as file:
                        file.write(tfile.read())
                    if archivename:
                        shutil.copyfile(filename, archivename)
                    break
                except OSError as e:
                    print(str(e))
                    input('Ensure that the file or directory is not open or locked, then press enter to try again.')
    return return_data


def query_to_update(update_filename,
                    update_table,
                    data,
                    dynamic_targets=None,
                    update_metadata=None,
                    where_addendums=[],
                    addendum_decorators=[],
                    archivename=None,
                    static_targets=[]):
    """The data argument should be the return value of query_to_csv
    wherein the return_indices supplied were at least three in quantity
    whereby the first referred to the relevant emplid column,
    the second referred to the relevant adm_appl_nbr column,
    the third referenced the column of update source values,
    and the remaining optional indices cohere with the subsequent argument;
    the optional dynamic_targets argument should be a list of strings
    wherein each is the name of a column to be updated;
    the optional where_addendums argument should be a list of strings
    wherein the length is equal to the number of return_indices supplied
    to query_to_csv minus three and whereby each string is the name
    of an additional column to be used in the where clause;
    the optional addendum_decorators argument should be a list of
    two-string tuples wherein the length of addendum_decorators equals
    the length of where_addendums and whereby the first argument in each tuple
    is the prefix for the corresponding where_addendums argument and the second
    argument is the postfix; the optional static_targets argument should be
    a list of string two-tuples wherein the first element is a column name to be
    updated and the second element is the update value."""
    if data and len(data[0]) >= 3 and (
            not where_addendums
            or not addendum_decorators
            or len(where_addendums) == len(addendum_decorators)):
        stmt_groups = []
        excerpts = []
        while len(excerpts) < (len(where_addendums) + 3):
            excerpts.append('')
        for i, row in enumerate(data):
            if (i % 500) == 0 and i > 0:
                stmt_groups.append(excerpts.copy())
                ei = 0
                while ei < len(excerpts):
                    excerpts[ei] = ''
                    ei += 1
            excerpts[0] += ('\n  \'' + row[1] + '\', \'' + row[2] + '\',')
            excerpts[1] += ('\n  \'' + row[0] + '\',')
            excerpts[2] += ('\n  \'' + row[0] + '\', \'' + row[1] + '\',')
            for (wi, addendum) in enumerate(where_addendums):
                dvalue = prep_sql_vals(row[wi + 3])[0]
                if addendum_decorators:
                    dvalue = addendum_decorators[wi][0] + dvalue + addendum_decorators[wi][1]
                excerpts[wi + 3] += ('\n  \'' + row[1] + '\', ' + dvalue + ',')
        stmt_groups.append(excerpts)
        for row in stmt_groups:
            for i, string in enumerate(row):
                row[i] = string.rstrip(',') + '\n'
        while True:
            try:
                with open(update_filename, 'w') as file:
                    for row in stmt_groups:
                        stmt = 'UPDATE {}\nSET '.format(update_table)
                        targets = []
                        if update_metadata:
                            targets.append('SCC_ROW_UPD_OPRID = {}, SCC_ROW_UPD_DTTM = {}'.format(*update_metadata))
                        if static_targets:
                            for pair in static_targets:
                                targets.append('{} = {}'.format(*pair))
                        if dynamic_targets:
                            for target in dynamic_targets:
                                targets.append('{} = DECODE(ADM_APPL_NBR, {})'.format(target, row[0]))
                        stmt += ', '.join(targets)
                        stmt += '\nWHERE EMPLID IN ({}) AND ADM_APPL_NBR = DECODE(EMPLID, {})'.format(row[1], row[2])
                        if where_addendums:
                            for wi, addendum in enumerate(where_addendums):
                                stmt += ' AND {} = DECODE(ADM_APPL_NBR, {})'.format(addendum, row[wi + 3])
                        stmt += ';\n'
                        file.write(stmt)
                if archivename:
                    shutil.copyfile(update_filename, archivename)
                break
            except OSError as e:
                print(str(e))
                input('Ensure that the file or directory is not open or locked, then press any enter to try again.')
    return None


def translate_ascii(string):
    rstring = ''
    for letter in string:
        if ord(letter) < 128:
            rstring += letter
        else:
            rstring += codex[letter]
    return rstring
