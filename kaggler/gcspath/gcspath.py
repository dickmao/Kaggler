# -*- coding: utf-8 -*-
# pylint: disable=wrong-import-position

import os
import re
import sys
import subprocess
import boto3
import inspect

def error(message):
    frame = inspect.currentframe()
    print("{}:{}, {}(): {}".format(frame.f_code.co_filename,
                                  frame.f_lineno, frame.f_code.co_name,
                                  message))

def ebs_volume(competition=None, dataset=None, recreate=None):
    ec2 = boto3.resource('ec2')
    volumes = ec2.volumes.filter(Filters=[{'Name': 'tag:name',
                                           'Values': [competition or dataset]}])
    volume = next(iter(volumes), None)
    client = boto3.client('ec2')
    if volume:
        if not recreate:
            return volume
        else:
            for i, d in [(a.InstanceId, a.Device) for a in volume.attachments]:
                volume.detach_from_instance(Device=d, InstanceId=i, Force=True)
            volume.delete()
            try:
                client.get_waiter('volume_deleted').wait(VolumeIds=[volume.id])
            except botocore.exceptions.WaiterError as e:
                error("VolumeId {}, {}".format(volume.id, str(e)))
                return volume
    volumes = ec2.volumes.filter(Filters=[{'Name': 'tag:name',
                                           'Values': [competition or dataset]}])
    region = boto3.session.Session().region_name
    if not region:
        error("Need to set aws default region")
    azs = client.describe_availability_zones(Filters=[
        {
            'Name': 'group-name',
            'Values': [region],
        },
        {
            'Name': 'state',
            'Values': ['available'],
        },
    ])['AvailabilityZones']
    total_size = 0
    bucket = boto3.resource('s3').Bucket('mybucket')
    for object in bucket.objects.all():
        total_size += object.size
        print(object.size)
    print(total_size)
    sz = 123
    volume = ec2.create_volume(
        AvailabilityZone=azs[randrange(len(azs))]['ZoneName'],
        Size=sz,
        DryRun=True,
        TagSpecifications=[
            {
                'ResourceType': 'volume',
                'Tags': [
                    {
                        'Key': 'name',
                        'Value': competition or dataset,
                    },
                ]
            }
        ],
    )
    try:
        client.get_waiter('volume_available').wait(VolumeIds=[volume.id])
    except botocore.exceptions.WaiterError as e:
        error("VolumeId {}, {}".format(volume.id, str(e)))
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
