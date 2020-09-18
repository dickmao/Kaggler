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
from contextlib import redirect_stdout, redirect_stderr
from pathlib import Path
from urllib.request import urlopen
from time import sleep

def get_instanceid():
    try:
        return urlopen('http://169.254.169.254/latest/meta-data/instance-id', timeout=1) \
            .read().decode()
    except:
        pass
    return None

def get_az():
    try:
        return urlopen('http://169.254.169.254/latest/meta-data/placement/availability-zone', timeout=1) \
            .read().decode()
    except:
        pass
    return None

def get_region():
    try:
        return urlopen('http://169.254.169.254/latest/meta-data/placement/region', timeout=1) \
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

def download(dir, url, recreate=None):
    if not recreate and len(os.listdir(dir)) != 0:
        error("Directory {} not empty".format(dir))
        return None
    return gsutil_rsync(dir, url)

def gsutil_rsync(dir, url):
    output = None
    try:
        with Restorer(['gsutil', '-m', '-q', 'rsync', '-r', url, dir]):
            stderr = io.StringIO()
            with redirect_stderr(stderr):
                stdout = io.StringIO()
                with redirect_stdout(stdout):
                    import gslib.__main__
                    gslib.__main__.main()
                    if stderr.getvalue():
                        output = next(iter(reversed(stderr.getvalue()).split()))
    except Exception as e:
        error("{}: {}".format(str(e), stderr.getvalue()))
    return output

def ebs_volume(dir, competition=None, dataset=None, recreate=None):
    Path(dir).mkdir(parents=True, exist_ok=True)
    url = gcspath(competition=competition, dataset=dataset)
    volume = None
    instance_id = get_instanceid()
    if not instance_id:
        download(dir, url, recreate)
    else:
        region = get_region()
        ec2 = boto3.resource('ec2', region_name=region)
        label = competition or dataset
        snapshots = ec2.snapshots.filter(
            Filters=[{'Name': 'description', 'Values': [label]},],
            OwnerIds=['self',],
        )
        snapshot = next(iter(snapshots), None)
        client = boto3.client('ec2', region_name=region)
        if snapshot and recreate:
            snapshot.delete()
            snapshot = None
        volume = None
        if not snapshot:
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
                AvailabilityZone=get_az(),
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
        else:
            try:
                snapshot.wait_until_completed(
                    Filters=[{'Name': 'description', 'Values': [label]},],
                    OwnerIds=['self',],
                )
            except botocore.exceptions.WaiterError as e:
                error("SnapshotId {}, {}".format(snapshot.id, str(e)))
                return None
            volume = ec2.create_volume(
                AvailabilityZone=get_az(),
                SnapshotId=snapshot.id,
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
        if not volume:
            error("Could not create volume")
            return None
        try:
            client.get_waiter('volume_available').wait(VolumeIds=[volume.id])
        except botocore.exceptions.WaiterError as e:
            error("VolumeId {}, {}".format(volume.id, str(e)))
            return None

        attached = False
        device = '/dev/xvdf'

        try:
            client.attach_volume(
                Device=device,
                InstanceId=instance_id,
                VolumeId=volume.id,
            )
        except botocore.exceptions.ClientError as e:
            error("VolumeId {}, {}".format(volume.id, str(e)))
        for _ in range(5):
            response = client.describe_volumes(
                VolumeIds=[volume.id],
            )
            response_volume = next(iter(response['Volumes']), None)
            if response_volume:
                attached = any([att for att in response_volume['Attachments'] if \
                                att['InstanceId'] == instance_id \
                                and att['State'] == 'attached' \
                                and Path(device).is_block_device()])
                if attached:
                    fstype = next(iter([part.fstype for part in psutil.disk_partitions() if part.device == device]), None)
                    if not fstype:
                        if 0 != os.system("sudo mkfs -t ext4 {}".format(device)):
                            error("Cannot mkfs.ext4 {}".format(device))
                            break
                    if 0 != os.system("sudo mount {} {}".format(device, dir)):
                        error("Cannot mount {} to {}".format(device, dir))
                    elif not fstype and gsutil_rsync(dir, url):
                        client.create_snapshot(
                            Description=label,
                            VolumeId=volume.id,
                        )
                    break
            sleep(3)
        if not attached:
            error("Cannot attach {} to {}".format(volume.id, instance_id))
            return None
        try:
            client.modify_instance_attribute(
                Attribute='blockDeviceMapping',
                BlockDeviceMappings=[
                    {
                        'DeviceName': device,
                        'Ebs': {
                            'DeleteOnTermination': True,
                            'VolumeId': volume.id,
                        },
                    },
                ],
                InstanceId=instance_id,
            )
        except botocore.exceptions.ClientError as e:
            error("Device {} cannot delete-on-terminate: {}".format(device, str(e)))

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
