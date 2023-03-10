#!/usr/bin/env python3

import os
import sys
import rados
import argparse

from multiprocessing.pool import ThreadPool
from subprocess import run, PIPE
from tempfile import mkstemp

DEF_NTHREADS = 1
DEF_NPROCS = 1
DEF_TMPDIR = '/tmp'
DEF_OBJECT_SIZE = 64*1024*1024


def stat(pool, object_name, striper=False):
    striper_opt = ['--striper'] if striper else []
    result = run(['rados', '-p', pool] + striper_opt + ['stat', object_name], stdout=PIPE, stderr=PIPE)
    if result.returncode == 0:
        _, _, res = result.stdout.decode('utf-8').rpartition(' ')
        res = int(res)
    else:
        res = None
    return res


def sort_file(filename, tmpdir, ncpus=1):
    sorted_path = None
    if os.path.exists(filename) and os.path.isdir(tmpdir):
        _tfd, sorted_path= mkstemp(dir=tmpdir)
        os.close(_tfd)
        with open(sorted_path, 'w') as fd:
            if ncpus > 1:
                par_opts = ['--parallel', ncpus]
            else:
                par_opts = []
            out = run(['sort'] + par_opts + [filename], stdout=fd)
        if out.returncode != 0:
            print("Failed to sort file {0}:\n{1}\n{2}".format(filename, out.stdout, out.stderr), file=sys.stderr)
            os.path.unlink(sorted_path)
            sorted_path = None
    return sorted_path


def process_results(async_results, object_size):
    for fn, oc, a1, a2 in async_results:
        a1.wait()
        a2.wait()
        if not a1.successful() or not a2.successful():
            print(fn)
        else:
            sz1, sz2 = a1.get(), a2.get() 
            if sz1 + (oc - 1)*object_size != sz2:
                print(fn)


def find_stub(dump, ceph_pool, object_size, nprocs=2):
    async_results = []
    last_obj = None 
    thread_pool = ThreadPool(nprocs)
    obj_count = 0
    with open(dump) as fd:
        for line in fd:
            line = line.rstrip()
            filename = line[:-17]
            last_filename = last_obj[:-17] if last_obj else filename
            if filename != last_filename:
                async_results.append(
                        (last_filename, obj_count, thread_pool.apply_async(stat, (ceph_pool, last_obj)), thread_pool.apply_async(stat, (ceph_pool, last_filename, True)))
                    )
                obj_count = 1
            else:
                obj_count += 1

            if len(async_results) > 100: 
                process_results(async_results, object_size)
                async_results = []

            last_obj = line

    async_results.append(
            (last_filename, obj_count, thread_pool.apply_async(stat, (ceph_pool, last_obj)), thread_pool.apply_async(stat, (ceph_pool, last_filename, True)))
        )
    process_results(async_results, object_size)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--pool', help="Rados pool to use", required=True)
    parser.add_argument('-n', '--nthreads', help="Number of threads to use. Default is {0}.".format(DEF_NTHREADS), default=DEF_NTHREADS, type=int)
    parser.add_argument('-N', '--Nprocs', help="Number of processes to use for sort. Default is {0}.".format(DEF_NPROCS), default=DEF_NPROCS, type=int)
    parser.add_argument('-c', '--cleanup', help="Remove temporary files after exit.", action='store_true')
    parser.add_argument('-o', '--object_size', help="Object size. Default is {0}".format(DEF_OBJECT_SIZE), type=int, default=DEF_OBJECT_SIZE)
    gr = parser.add_mutually_exclusive_group()
    gr.add_argument('-s', '--sorted', help="Indicates that the file with object names is already sorted.", action='store_true')
    gr.add_argument('-t', '--tmpdir', help="Temporary directory to store sorted object dump. Default is {0}".format(DEF_TMPDIR), default=DEF_TMPDIR)
    parser.add_argument('obj_dump', help="File with all object names from given pool.")
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    if args.sorted:
        dump = args.obj_dump
    else:
       dump = sort_file(args.obj_dump, args.tmpdir)

    if dump is not None:
        find_stub(dump, args.pool, args.object_size, args.nthreads)

    if args.cleanup:
        if not args.sorted:
            os.path.unlink(dump)
        else:
            print("Will not delete file {0} that was not created by me".format(dump), file=sys.stderr)
