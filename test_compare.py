#!/usr/bin/env python3
import pytest
import random
import codecs
import sys
import os

from ast import literal_eval
from shutil import copyfile
from subprocess import call, Popen, PIPE

separator = "|"


def random_string(length):
    return codecs.encode(os.urandom(length), 'base64').decode().strip()


def get_line():
    res = '/'.join( [random_string(random.randint(7, 14)) for _ in range(4)] )
    if not res.startswith('/'):
        res = '/' + res
    res += separator + str(random.randint(1,10000000)) + separator + random_string(6)
    return res


def generate_dumps(tmpdir, base_filename, prefix_num=10, lines=10000, nfiles=3, bad_files=100, spread_across=2):
    base_filename = tmpdir + '/' + base_filename
    orig_filename = base_filename + str(nfiles)
    line_cnt = lines*prefix_num if prefix_num > 0 else lines
    with open(orig_filename, 'w') as fd:
        for _ in range(line_cnt):
            print(  get_line(), file=fd  )

    res = {}
    for i in range(1, nfiles+1):
        if prefix_num > 0:
            prefixes = [random_string(random.randint(7, 14)) for i in range(prefix_num)]
        else:
            prefixes = [ '' ]
        filename = base_filename + str(i)
        res[filename] = {'prefixes': prefixes, 'lost': None}
        if i != nfiles:
            copyfile(orig_filename, filename)
        if prefix_num > 0:
            for pnum in range(prefix_num):
                call(["/bin/sed", "-i", filename, "-e", f"{pnum*lines+1},{(pnum+1)*lines}s!^!{'/' + prefixes[pnum]}!"])

    #Spoil files
    indexes = [i+1 for i in range(nfiles)]
    n_lost = bad_files // spread_across
    for _ in range(spread_across):
        idx = random.choice(indexes)
        indexes.remove(idx)
        filename = base_filename + str(idx)
        removed = []
        for i in range(n_lost):
            row_num = random.randint(1, line_cnt - i)
            p = Popen(["/bin/sed", "-ne", f"{row_num}p", filename], stdout=PIPE)
            stdout, e_cod = p.communicate()
            call(["/bin/sed", "-i", f"{row_num}d", filename])
            line = stdout.decode().strip()
            for pref in res[filename]['prefixes']:
                if line.startswith('/' + pref) and pref:
                    line = line[(len(pref) + 1):]
                try:
                    line, _ = line.split(separator, maxsplit=1)
                except:
                    pass
            removed.append(line)

        res[filename]['lost'] = removed
    return res


@pytest.mark.parametrize(
        "prefixes,n_files,lines",
        [
            (0, 3, 10000),
            (0, 4, 10000),
            (0, 5, 10000),
            (3, 3, 10000),
            (3, 4, 10000),
            (3, 5, 10000),
            (6, 3, 10000),
            (6, 4, 10000),
            (6, 5, 10000),
            (9, 3, 10000),
            (9, 4, 10000),
            (9, 5, 10000),
	    (10, 3, 10000),
            (10, 4, 10000),
            (10, 5, 10000),
            (10, 3, 100000),
            (10, 3, 1000000),
        ]
    )
def test_script(prefixes, n_files, lines):
    tmpdir = './tests'
    base_name = 'gen_dump'
    dumps = generate_dumps(tmpdir, base_name, prefix_num=prefixes, nfiles=n_files, lines=lines)
    missed_files = {k: [] for k in dumps.keys()}
    for i in range(prefixes if prefixes != 0 else 1):
        start_sym = '/' if prefixes > 0 else ''
        opt = ','.join([f"{dump}%{start_sym}{dumps[dump]['prefixes'][i]}%|" for dump in dumps])
        print(['./compare_v2.py', '-t', './tests', '-d', opt])
        p = Popen(['./compare_v2.py', '-t', './tests', '-d', opt], stdout=PIPE)
        stdout, stderr = p.communicate()
        for line in stdout.decode().split('\n'):
            try:
                data = literal_eval(line)
            except SyntaxError:
                pass
            else:
                for k, v in data.items():
                    for fil in v:
                        fname = fil.replace('_sorted', '')
                        missed_files[fname].append(k)

    bad = False
    for key in dumps:
        if dumps[key]['lost'] is not None:
            try:
                assert set(dumps[key]['lost']) == set(missed_files[key])
            except AssertionError:
                print("Failed:")


if __name__ == '__main__':
    test_script(0,3,10000)
