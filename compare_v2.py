#!/usr/bin/env python3
import argparse
import sys
import os

from subprocess import call
from contextlib import contextmanager

def sort_file(filename, tmpdir, ncpus=None):
    if not os.path.exists(tmpdir): 
        os.mkdir(tmpdir)
    output = tmpdir + '/' + os.path.basename(filename) + '_sorted'
    extra = [] if ncpus is None else ["--parallel", ncpus]
    call(
            ["/usr/bin/sort", "-k", "1", "-t", "|", "-T", tmpdir, "-o", output] + extra + [filename],
            env={'LC_COLLATE': 'utf-8'}
        )
    return output

@contextmanager
def open_files(path_array):
    FDs = []
    for path in path_array:
        fd = open(path)
        FDs.append(fd)
    try:
        yield FDs
    finally:
        for fd in FDs:
            fd.close()


def compare_sorted(files_data, separator='|'):
    files = [d['path'] for d in files_data]
    prefixes = [(d['prefix'], len(d['prefix'])) for d in files_data]
    with open_files(files) as FDs:
        lines = [None for _ in range(len(FDs))]
        eof = False
        min_line = None
        rest_diff = False
        old_rest = None
        while not eof:
            eof = True
            for i, fd in enumerate(FDs):
                if min_line == chr(255):
                    break
                if lines[i] == min_line:
                    line = fd.readline()
                    if line != '':
                        eof = False
                        line = line.strip()
                        if line.startswith(prefixes[i][0]):
                            line = line[prefixes[i][1]:]
                            if separator is not None:
                                line, rest = line.split(separator, maxsplit=1)
                            if old_rest:
                                rest_diff = rest == old_rest
                                old_rest = rest
                        else:
                            line = chr(255) if min_line is not None else None
                    else:
                        line = chr(255)
                    lines[i] = line

            min_line = min(lines) if None not in lines else None
            miss_data= []
            if min_line is not None:
                for i, line in enumerate(lines):
                    if line > min_line:
                        miss_data.append(files[i])

                if miss_data:
                    yield {min_line: miss_data}

                if not miss_data and rest_diff:
                    yield {min_line: 'mismatch'}


def compare(dumps):
    sorted_dumps = []
    for i, dump in enumerate(dumps):
        try:
            path, prefix = dump['path'], dump['prefix']
        except (TypeError, KeyError):
            path = dump
            prefix = ''
        sorted_dumps.append({ 'path': sort_file(path, args.tmpdir), 'prefix': prefix})
    for res in compare_sorted(sorted_dumps):
        print(res)
        


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--dumps', help="Dump list to be compared, comma-separated.", type=str)
    parser.add_argument('-t', '--tmpdir', help="Temporary directory.", type=str)
    args = parser.parse_args()
    dumps = args.dumps.split(',')
    dump_data = []
    for dump in dumps: 
        try:
            path, prefix, separator= dump.split('%')
        except ValueError:
            path = dump
            prefix = ''
            separator = '|'
        dump_data.append({'path': path, 'prefix': prefix, 'separator': separator})
    compare(dump_data)
