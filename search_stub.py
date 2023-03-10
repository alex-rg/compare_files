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


def filename2object(filename, obj_num):
    return '{0}.{1:0>16x}'.format(filename, obj_num)


def check_file(ctx, file_name, obj_count, object_size=None):
    res = True
    try:
        size = int(ctx.get_xattr(filename2object(file_name, 0), 'striper.size'))
    except (rados.NoData, rados.ObjectNotFound):
        res = False
    else:
        try:
            last_obj_size = ctx.stat(filename2object(file_name, obj_count-1))[0]
        except rados.ObjectNotFound:
            res = False
        else:
            if object_size is None:
                object_size = int(ctx.get_xattr(filename2object(file_name, 0), 'striper.layout.object_size'))
            if object_size * (obj_count-1) + last_obj_size != size:
                res = False
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
            out = run(['sort'] + par_opts + [filename], stdout=fd, env={'LC_COLLATE': 'C'})
        if out.returncode != 0:
            print("Failed to sort file {0}:\n{1}\n{2}".format(filename, out.stdout, out.stderr), file=sys.stderr)
            os.unlink(sorted_path)
            sorted_path = None
    return sorted_path


def process_results(async_results):
    for filename, ares in async_results:
        ares.wait()
        if not ares.successful():
            print(filename)
        else:
            if not ares.get():
                print(filename)


def find_stub(dump, ceph_pool, object_size=None, nprocs=1, conffile='/etc/ceph/ceph.conf'):
    cluster = rados.Rados(conffile=conffile)
    cluster.connect()
    ctx = cluster.open_ioctx(ceph_pool)

    async_results = []
    last_obj = None 
    thread_pool = None
    if nprocs > 1:
        thread_pool = ThreadPool(nprocs)
    obj_count = 0
    line = None
    with open(dump) as fd:
        while line != '':
            line = fd.readline().rstrip()
            filename = line[:-17]
            last_filename = last_obj[:-17] if last_obj else filename
            if filename != last_filename:
                fargs = (ctx, last_filename, obj_count, object_size)
                if thread_pool:
                    async_results.append(  ( last_filename, thread_pool.apply_async(check_file, fargs) )  )
                else:
                    print("checking |{0}|".format(last_filename), file=sys.stderr)
                    if not check_file(*fargs):
                        print(last_filename)
                obj_count = 1
            else:
                obj_count += 1

            if len(async_results) > 10: 
                process_results(async_results)
                async_results = []

            last_obj = line


    #async_results.append(
    #        (last_filename, thread_pool.apply_async(stat, (ceph_pool, last_obj)), thread_pool.apply_async(stat, (ceph_pool, last_filename, True)))
    #    )
    process_results(async_results)
    ctx.close()


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--pool', help="Rados pool to use", required=True)
    parser.add_argument('-n', '--nthreads', help="Number of threads to use. Default is {0}.".format(DEF_NTHREADS), default=DEF_NTHREADS, type=int)
    parser.add_argument('-N', '--Nprocs', help="Number of processes to use for sort. Default is {0}.".format(DEF_NPROCS), default=DEF_NPROCS, type=int)
    parser.add_argument('-c', '--cleanup', help="Remove temporary files after exit.", action='store_true')
    parser.add_argument('-o', '--object_size', help="Object size. If omitted, it will be requested from each object." \
            + "Such an approach has lower performance, but allow one to handle files with different object sizes",
            type=int,
            default=None
        )
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
            os.unlink(dump)
        else:
            print("Will not delete file {0} that was not created by me".format(dump), file=sys.stderr)
