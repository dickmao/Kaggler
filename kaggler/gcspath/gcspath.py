# -*- coding: utf-8 -*-
# pylint: disable=wrong-import-position

import os
import re
import sys
import subprocess

def fusermount(mountpoint, **kwargs):
    subprocess.run([
        'bash',
        os.path.join(os.path.dirname(__file__), 'fusermount.sh'),
        gcspath(**kwargs).partition('gs://')[-1],
        mountpoint
    ], stdout=subprocess.PIPE).check_returncode()
    return mountpoint

def gcspath(competition=None, dataset=None):
    dopts = ['-d', dataset] if dataset else []
    copts = ['-c', competition] if competition else []
    if not dopts and not copts:
        return None
    result = subprocess.run([
        'bash',
        os.path.join(os.path.dirname(__file__), 'gcspath.sh'),
        *copts,
        *dopts
    ], stdout=subprocess.PIPE)
    return next(x for x in result.stdout.decode().split('\n') if re.compile('^gs://').search(x))
