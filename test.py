#!/usr/bin/env python3
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
    with open(orig_filename, 'w') as fd:
        for _ in range(lines*prefix_num):
            print(  get_line(), file=fd  )

    res = {}
    for i in range(1, nfiles+1):
        prefixes = [random_string(random.randint(7, 14)) for i in range(prefix_num)]
        filename = base_filename + str(i)
        res[filename] = {'prefixes': prefixes, 'lost': None}
        if i != nfiles:
            copyfile(orig_filename, filename)
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
            row_num = random.randint(1, lines*nfiles - i)
            p = Popen(["/bin/sed", "-ne", f"{row_num}p", filename], stdout=PIPE)
            stdout, e_cod = p.communicate()
            call(["/bin/sed", "-i", f"{row_num}d", filename])
            line = stdout.decode().strip()
            for pref in res[filename]['prefixes']:
                if line.startswith('/' + pref):
                    line = line[(len(pref) + 1):]
                try:
                    line, _ = line.split(separator, maxsplit=1)
                except:
                    pass
            removed.append(line)

        res[filename]['lost'] = removed
    return res



if __name__ == '__main__':
    prefixes = 10
    n_files = 3
    tmpdir = './tests'
    base_name = 'gen_dump'
    dumps = generate_dumps(tmpdir, base_name, prefix_num=prefixes, nfiles=n_files, lines=10000)
    missed_files = {k: [] for k in dumps.keys()}
    for i in range(prefixes):
        opt = ','.join([f"{dump}%/{dumps[dump]['prefixes'][i]}%|" for dump in dumps])
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
                if set(dumps[key]['lost']) != set(missed_files[key]):
                    print(f"Mismatch: {key}")
                    print(f"{sorted(dumps[key]['lost'])}")
                    print(f"{sorted(missed_files[key])}")
                    bad = True
                    sys.exit(1)
            except Exception as e:
                print(f"Except {e}", key, dumps, missed_files)
                sys.exit(1)

    if not bad:
        print("All OK")
