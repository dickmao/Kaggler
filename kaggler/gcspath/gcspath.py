# -*- coding: utf-8 -*-
# pylint: disable=wrong-import-position

import os
import re
import sys
import io
import math
import subprocess
import shlex
import inspect
import importlib
import gslib
from pathlib import Path
from contextlib import redirect_stdout
from google.oauth2 import service_account
from time import sleep

def authenticate(json):
    credentials = service_account.Credentials.from_service_account_file(json)
    return credentials.with_scopes(['https://www.googleapis.com/auth/cloud-platform'])

class Restorer():
    def __init__(self, value):
        self._current = value
    def __enter__(self):
        self._old = sys.argv
        sys.argv = self._current
        if 'gslib.__main__' in sys.modules:
            importlib.reload(gslib.__main__) # noqa F821
    def __exit__(self, *args):
        sys.argv = self._old

def sanitize(label):
    return label.replace('/', '-')

def error(message):
    frame = inspect.currentframe().f_back
    print("{}:{}, {}(): {}".format(frame.f_code.co_filename,
                                   frame.f_lineno, frame.f_code.co_name,
                                   message))

def download(dir, url, recreate=None):
    if not recreate and len(os.listdir(dir)) != 0:
        error("Directory {} not empty".format(dir))
        return None
    return gsutil_rsync(dir, url)

def gsutil_rsync(dir, url):
    try:
        with Restorer(['gsutil', '-m', '-q', 'rsync', '-r', url, dir]):
            import gslib.__main__
            gslib.__main__.main()
    except Exception as e:
        raise e

def gsutil_rsync_retry(dir, url, retries=1):
    for i in range(retries+1):
        try:
            gsutil_rsync(dir, url)
            break
        except Exception as e:
            error("Retrying #{}: {}".format(i+1, str(e)))

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
        '-e',
        os.path.join(os.path.dirname(__file__), 'gcspath.sh'),
        *copts,
        *dopts
    ], stdout=subprocess.PIPE)
    ret = next(iter([x for x in result.stdout.decode().split('\n') if re.compile('^gs://').search(x)]), None)
    if not ret:
        print(result.stdout.decode())
    return ret

def url_size(url, base10=False):
    '''In GiB.'''
    with Restorer(['gsutil', 'du', '-s', url]):
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            import gslib.__main__
            gslib.__main__.main()
            sz = next(iter(stdout.getvalue().split()), None)
        try:
            float(sz)
        except ValueError:
            error("Could not ascertain bucket size of {}".format(url))
            return None
        else:
            return math.ceil(float(sz)/((10 ** 9) if base10 else (2 << 29)))

def disk_ensure_format(device):
    for _ in range(5):
        if Path(device).is_block_device():
            break
        sleep(3)
    if not Path(device).is_block_device():
        error("Did not see device {}".format(device))
        return None
    fstype = subprocess.run(shlex.split('sudo blkid -s TYPE -o value {}'.format(device)), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        fstype.check_returncode()
        fstype = fstype.stdout.decode('utf-8').rstrip()
    except subprocess.CalledProcessError:
        fstype = None
        if 0 != os.system("sudo mkfs.ext4 -m 0 -E lazy_itable_init=0,lazy_journal_init=0,discard {}".format(device)):
            error("Cannot mkfs.ext4 {}".format(device))
            raise
    if 0 != os.system("sudo bash -c 'grep -wqs {} /proc/mounts || mount -o rw,user {} {}'".format(device, device, dir)):
        error("Cannot mount {} to {}".format(device, dir))
        raise
    elif 0 != os.system("sudo chmod 777 {}".format(dir)):
        error("Cannot chmod {} for write".format(dir))
        raise

def disk_ensure_data(dir, url):
    du = 0
    for f in Path(dir).glob('**/*'):
        if f.is_file():
            du += f.stat().st_size
        if du > 2**10:
            break
    if du <= 2**10:
        gsutil_rsync_retry(dir, url)
