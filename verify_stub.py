#!/usr/bin/env python3

import os
import sys
import rados
import argparse

from multiprocessing.pool import ThreadPool
from subprocess import run, PIPE
from tempfile import mkstemp


def filename2object(filename, obj_num):
    return '{0}.{1:0>16x}'.format(filename, obj_num)


def fully_check_file(ctx, file_name):
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
    parser.add_argument('-p', '--pool', help="Rados pool to use", required=True)
    parser.add_argument('file_list', help="File with file names that should be check for subness.")
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    verify_stub(args.file_list, args.pool)
