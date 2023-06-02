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

FLUSH_STEP = 1000

def filename2object(filename, obj_num):
    "Given file's name and object number, get full object's name"
    return '{0}.{1:0>16x}'.format(filename, obj_num)


def simple_check_file(ctx, file_name, obj_count, object_size=None):
    """
    Check whether file is 'stub' or not. Stub means its size according to metadata differs from its real size.
    Here we assume that the number of objects constituting the file is known. The check is quick, i.e. we do not
    stat every individual object.

    @param ctx:         rados context
    @param file_name:   name of the file to check
    @param obj_count:   number of ceph objects that store file's data
    @param object_size: maximum size of a single ceph object
    """
    res = True
    try:
        size = int(ctx.get_xattr(filename2object(file_name, 0), 'striper.size'))
        if object_size is None:
            object_size = int(ctx.get_xattr(filename2object(file_name, 0), 'striper.layout.object_size'))
    except (rados.NoData, rados.ObjectNotFound):
        res = False
    else:
        try:
            last_obj_size = ctx.stat(filename2object(file_name, obj_count-1))[0]
        except rados.ObjectNotFound:
            res = False
        else:
            if object_size * (obj_count-1) + last_obj_size != size:
                res = False
    return res


def fully_check_file(ctx, file_name):
    """
    Check whether file is 'stub' or not. Stub means its size according to metadata differs from its real size.
    The check is full -- all file's objects are statted.

    @param ctx:         rados context
    @param file_name:   name of the file to check
    """
    res = True
    try:
        file_size = int(ctx.get_xattr(filename2object(file_name, 0), 'striper.size'))
        obj_size = int(ctx.get_xattr(filename2object(file_name, 0), 'striper.layout.object_size'))
    except (rados.NoData, rados.ObjectNotFound):
        res = False
    else:
        if file_size > 0:
            obj_count = file_size // obj_size + (0 if obj_size % file_size == 0 else 1)
        else:
            obj_count = 1
        real_size = 0
        for obj_idx in range(obj_count):
            try:
                obj_size = ctx.stat(filename2object(file_name, obj_idx))[0]
            except rados.ObjectNotFound:
                res = False
                break
            else:
                real_size += obj_size
        if real_size != file_size:
            res = False
    return res


def sort_file(filename, tmpdir, ncpus=1):
    """
    Sort given file.

    @param filename: name of the file to sort
    @param tmpdir:   directory where sorted file will be stored. It also will be used by 'sort' utility
    @return:         path of the sorted file
    """
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
    """
    Print stub files that has been already found.

    @param async_results: array [(<file_name>, <async_result>), ...], where <async_result> is the output of async
                          application of the 'check_file' function to filename
    """
    for filename, ares in async_results:
        ares.wait()
        if not ares.successful():
            print(filename)
        else:
            if not ares.get():
                print(filename)


def find_stub(dump, ceph_pool, object_size=None, nprocs=1, conffile='/etc/ceph/ceph.conf'):
    """
    Find stub files and print them to stdout. File is considered to be stub if its size differs
    from the 'size' value written in its metadata.

    @param dump:        a file with the list of all cehp objects in the pool, separated by newlines
    @param ceph_pool:   ceph pool name
    @param object_size: maximum object size (defined by libradosstriper)
    @param nprocs:      number of threads to use
    @param conffile:    ceph config file
    """
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
    idx = 0
    with open(dump) as fd:
        while line != '':
            idx += 1
            if idx % 1000 == 0:
                print("processing line ", idx, file=sys.stderr)
            line = fd.readline().rstrip()
            filename = line[:-17]
            last_filename = last_obj[:-17] if last_obj else filename
            if filename != last_filename:
                fargs = (ctx, last_filename, obj_count, object_size)
                if thread_pool:
                    async_results.append(  ( last_filename, thread_pool.apply_async(check_file, fargs) )  )
                else:
                    if not check_file(*fargs):
                        print(last_filename)
                obj_count = 1
            else:
                obj_count += 1

            if len(async_results) > FLUSH_STEP: 
                process_results(async_results)
                async_results = []

            last_obj = line


    #async_results.append(
    #        (last_filename, thread_pool.apply_async(stat, (ceph_pool, last_obj)), thread_pool.apply_async(stat, (ceph_pool, last_filename, True)))
    #    )
    process_results(async_results)
    ctx.close()


def verify_stub(file_list, ceph_pool, conffile='/etc/ceph/ceph.conf'):
    cluster = rados.Rados(conffile=conffile)
    cluster.connect()
    ctx = cluster.open_ioctx(ceph_pool)

    with open(file_list) as fd:
        for file_name in fd:
            file_name = file_name.rstrip()
            if not fully_check_file(ctx, file_name):
                print(file_name, "STUB")
    ctx.close()


def parse_args():
    parser = argparse.ArgumentParser()
    parser = argparse.ArgumentParser(epilog="""
The script is used to identify potentially 'stub' files on the ceph-based storage element. A file is
considered to be stub if its size differs from the 'size' value written in file's metadata

The check is done in two steps: search for potentially stub files and verification.

To perform the initial search a dump of all objects from a given rados pool is needed. The script will then
count the sizes of all objects corresponding to given file and compare this value with the one
stored in file's metadata. If there is a mismatch, file is stub.

Please note that the files that were partially transferred at the time when the dump was collected
also will be reported as stub. That means that additional verification of all found files is absolutely
necessary.

To verify files provide file list to the script in verification mode

Usage example:
/* search for potentially stub files, save results */
$ search_stub.py -p lhcb search_stub -o "$((64*1024*1024))" -s ../lhcb_dump_sorted | tee potentially_stub
/* verify that previously found files are indeed stub */
$ search_stub.py -p lhcb search_stub ./potentially_stub
""", formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-p', '--pool', help="Rados pool to use", required=True)
    subparsers = parser.add_subparsers(dest='subcommand')
    p1 = subparsers.add_parser("search_stub", help="Search for potentially stub files") 
    p1.add_argument('-n', '--nthreads', help="Number of threads to use. Default is {0}.".format(DEF_NTHREADS), default=DEF_NTHREADS, type=int)
    p1.add_argument('-N', '--Nprocs', help="Number of processes to use for sort. Default is {0}.".format(DEF_NPROCS), default=DEF_NPROCS, type=int)
    p1.add_argument('-c', '--cleanup', help="Remove temporary files after exit.", action='store_true')
    p1.add_argument('-o', '--object_size', help="Object size. If omitted, it will be requested from each object." \
            + "Such an approach has lower performance, but allow one to handle files with different object sizes",
            type=int,
            default=None
        )
    gr = p1.add_mutually_exclusive_group()
    gr.add_argument('-s', '--sorted', help="Indicates that the file with object names is already sorted.", action='store_true')
    gr.add_argument('-t', '--tmpdir', help="Temporary directory to store sorted object dump. Default is {0}".format(DEF_TMPDIR), default=DEF_TMPDIR)
    p1.add_argument('obj_dump', help="File with all object names from given pool.")

    p2 = subparsers.add_parser("verify_stub", help="Verify that files are indeed stub") 
    p2.add_argument('stub_list', help="List of stub files.")
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    if args.subcommand == 'search_stub':
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
    else args.subcommand == 'verify_stub':
        verify_stub(args.stub_list, args.pool)
