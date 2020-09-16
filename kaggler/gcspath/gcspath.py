# -*- coding: utf-8 -*-
# pylint: disable=wrong-import-position

import os
import re
import sys
import subprocess
import boto3
import inspect
import botocore
import io
import math
import psutil
import path
from contextlib import redirect_stdout, redirect_stderr
from pathlib import Path
from urllib.request import urlopen
from time import sleep

def instanceid():
    try:
        return urlopen('http://169.254.169.254/latest/meta-data/instance-id', timeout=1) \
            .read().decode()
    except:
        pass
    return None

def az():
    try:
        return urlopen('http://169.254.169.254/latest/meta-data/placement/availability-zone', timeout=1) \
            .read().decode()
    except:
        pass
    return None

class Restorer():
    def __init__(self, value):
        self._current = value

    def __enter__(self):
        self._old = sys.argv
        sys.argv = self._current

    def __exit__(self, *args):
        sys.argv = self._old

def error(message):
    frame = inspect.currentframe().f_back
    print("{}:{}, {}(): {}".format(frame.f_code.co_filename,
                                   frame.f_lineno, frame.f_code.co_name,
                                   message))

def download(dir, competition=None, dataset=None, recreate=None):
    output = None
    Path(dir).mkdir(parents=True, exist_ok=True)
    if not recreate and len(os.listdir(dir)) != 0:
        error("Directory {} not empty".format(dir))
    else:
        url = gcspath(competition=competition, dataset=dataset)
        try:
            with Restorer(['gsutil', '-m', '-q', 'rsync', '-r', url, dir]):
                stderr = io.StringIO()
                with redirect_stderr(stderr):
                    stdout = io.StringIO()
                    with redirect_stdout(stdout):
                        import gslib.__main__
                        gslib.__main__.main()
                        output = next(iter(reversed(stderr.getvalue()).split()), None)
        except Exception as e:
            error("{}: {}".format(str(e), stderr.getvalue()))
    return output

def ebs_volume(dir, competition=None, dataset=None, recreate=None):
    volume = None
    instance_id = instanceid()
    if not instance_id:
        download(dir, competition=competition, dataset=dataset, recreate=recreate)
    else:
        ec2 = boto3.resource('ec2')
        label = competition or dataset
        volumes = ec2.volumes.filter(Filters=[{'Name': 'tag:name',
                                               'Values': [label]}])
        volume = next(iter(volumes), None)
        client = boto3.client('ec2')
        if volume and recreate:
            for i, d in [(a.InstanceId, a.Device) for a in volume.attachments]:
                volume.detach_from_instance(Device=d, InstanceId=i, Force=True)
                volume.delete()
            try:
                client.get_waiter('volume_deleted').wait(VolumeIds=[volume.id])
            except botocore.exceptions.WaiterError as e:
                error("VolumeId {}, {}".format(volume.id, str(e)))
            volume = None
        if not volume:
            region = boto3.session.Session().region_name
            if not region:
                error("Need to set aws default region")
                return None
            url = gcspath(competition=competition, dataset=dataset)
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
                    sz = math.ceil(float(sz)/(2 << 29))
            volume = ec2.create_volume(
                AvailabilityZone=az(),
                Size=sz,
                TagSpecifications=[
                    {
                        'ResourceType': 'volume',
                        'Tags': [
                            {
                                'Key': 'name',
                                'Value': label,
                            },
                        ]
                    }
                ],
            )
            try:
                client.get_waiter('volume_available').wait(VolumeIds=[volume.id])
            except botocore.exceptions.WaiterError as e:
                error("VolumeId {}, {}".format(volume.id, str(e)))

            attached = False
            for _ in range(5):
                response = client.attach_volume(
                    Device='/dev/sdf',
                    InstanceId=instance_id,
                    VolumeId=volume.id,
                )
                attached = (response['State'] == 'attached')
                if attached:
                    fstype = next(iter([part.fstype for part in psutil.disk_partitions() if path.startswith(part.mountpoint)]))
                    if not fstype:
                        os.system("sudo mkfs -t xfs /dev/xvdf")
                    break
                else:
                    sleep(3)
            if not attached:
                error("Cannot attach {} to {}".format(volume.id, instance_id))
    return volume

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
