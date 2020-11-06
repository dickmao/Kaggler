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
import shlex
import socket
import zipfile
import datetime
from itertools import chain
from contextlib import redirect_stdout
from pathlib import Path
from urllib.request import urlopen
from time import sleep
import importlib

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

def get_mac():
    try:
        return urlopen('http://169.254.169.254/latest/meta-data/network/interfaces/macs/', timeout=1) \
            .read().decode()
    except:
        pass
    return None

def get_subnet():
    try:
        return urlopen('http://169.254.169.254/latest/meta-data/network/interfaces/macs/{}/subnet-id'.format(get_mac()), timeout=1) \
            .read().decode()
    except:
        pass
    return None

def get_vpc():
    try:
        return urlopen('http://169.254.169.254/latest/meta-data/network/interfaces/macs/{}/vpc-id'.format(get_mac()), timeout=1) \
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
        if 'gslib.__main__' in sys.modules:
            importlib.reload(gslib.__main__) # noqa F821
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
    try:
        with Restorer(['gsutil', '-m', '-q', 'rsync', '-r', url, dir]):
            import gslib.__main__
            gslib.__main__.main()
    except Exception as e:
        raise e

def gsutil_rsync_retry(dir, url, retries=1):
    for i in range(retries):
        try:
            gsutil_rsync(dir, url)
            break
        except Exception:
            error("Retrying #{}".format(i+1))

def mount_retry(dir, region, fs_id):
    addr = '{}.efs.{}.amazonaws.com'.format(fs_id, region)
    for _ in range(15):
        try:
            socket.gethostbyname(addr)
        except socket.gaierror as e:
            error('gethostbyname ({}): {}'.format(addr, str(e)))
        if 0 == os.system("sudo bash -c 'grep -qs {} /proc/mounts || mount -t nfs4 -o nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2 {}:/ {}'".format(fs_id, addr, dir)):
            return True
        error('Retrying mount.nfs4 {}'.format(addr))
        sleep(4)
    return False

def setup_expiry_slate(common, policies, label, eventsc, lambdac, iamc):
    for expr in common:
        try:
            eval(expr, {"policies": policies,
                        "function_name": sanitize(label),
                        "role_name": sanitize(label),
                        "rule_name": sanitize(label),
                        "eventsc": eventsc,
                        "lambdac": lambdac,
                        "iamc": iamc,
                        })
        except (iamc.exceptions.NoSuchEntityException, lambdac.exceptions.ResourceNotFoundException, eventsc.exceptions.ResourceNotFoundException):
            pass

def sanitize(label):
    return label.replace('/', '-')

def setup_expiry(label, override=()):
    label = sanitize(label)
    region = get_region()
    lambdac = boto3.client('lambda', region_name=region)
    iamc = boto3.client('iam', region_name=region)
    eventsc = boto3.client('events', region_name=region)
    policies = [
        'arn:aws:iam::aws:policy/AWSLambdaFullAccess',
        'arn:aws:iam::aws:policy/CloudWatchEventsFullAccess',
        'arn:aws:iam::aws:policy/AmazonEventBridgeFullAccess',
        'arn:aws:iam::aws:policy/IAMFullAccess',
        'arn:aws:iam::aws:policy/AmazonElasticFileSystemFullAccess',
        'arn:aws:iam::aws:policy/AmazonEC2FullAccess',
    ]
    common = [
        'lambdac.remove_permission(FunctionName=function_name, StatementId=function_name)',
        'eventsc.remove_targets(Rule=rule_name, Ids=[ function_name ])',
        'eventsc.delete_rule(Name=rule_name)',
        'lambdac.delete_function(FunctionName=function_name)',
        '[iamc.detach_role_policy(RoleName=role_name, PolicyArn=arn) for arn in policies]',
        'iamc.delete_role(RoleName=role_name)',
    ]
    package = io.BytesIO()
    with zipfile.ZipFile(package, 'w') as z:
        info = zipfile.ZipInfo('handler.py')
        info.external_attr = 0o777 << 16
        z.writestr(info, """from __future__ import print_function
import json
import boto3
import os
import re
def lambda_handler(event, context):
    print('Received event: ' + json.dumps(event, indent=2))
    lambdac = boto3.client('lambda', region_name=event['region'])
    iamc = boto3.client('iam', region_name=event['region'])
    eventsc = boto3.client('events', region_name=event['region'])
    efsc = boto3.client('efs', region_name=event['region'])
    policies = {}
    function_name = context.function_name
    role_name = function_name
    rule_name = function_name
    efs_name = function_name
    fs_response = efsc.describe_file_systems(CreationToken=efs_name)
    efs = next(iter(fs_response['FileSystems']), None)
    if efs:
        fs_response = efsc.describe_mount_targets(FileSystemId=efs['FileSystemId'])
        for target in fs_response['MountTargets']:
            efsc.delete_mount_target(MountTargetId=target['MountTargetId'])
        efsc.delete_file_system(FileSystemId=efs['FileSystemId'])

        for _ in range(5):
            fs_response = efsc.describe_file_systems(CreationToken=efs_name)
            efs = next(iter(fs_response['FileSystems']), None)
            if not efs:
                break
            sleep(3)

        ec2c = boto3.client('ec2', region_name=event['region'])
        sg_response = ec2c.describe_security_groups(
            Filters=[
                {{
                    'Name': 'group-name',
                    'Values': [
                        efs_name,
                    ]
                }},
            ],
        )
        sg = next(iter(sg_response['SecurityGroups']), None)
        if sg:
            ec2c.delete_security_group(GroupId=sg['GroupId'])
""".format(policies) + \
                   '\n'.join(["""
    try:
        {}
    except (iamc.exceptions.NoSuchEntityException, lambdac.exceptions.ResourceNotFoundException, eventsc.exceptions.ResourceNotFoundException):
        pass
    """.format(expr) for expr in common]))

    setup_expiry_slate(common, policies, label, eventsc, lambdac, iamc)

    try:
        iamc.create_role(RoleName=label, AssumeRolePolicyDocument="""{
      "Version": "2012-10-17",
      "Statement": [
        {
          "Effect": "Allow",
          "Principal": {
            "Service": [
              "lambda.amazonaws.com",
              "events.amazonaws.com"
            ]
          },
          "Action": "sts:AssumeRole"
        }
      ]
    }
    """)
        for _ in range(5):
            role = iamc.get_role(RoleName=label)
            if not role['Role']['Arn']:
                sleep(3)
        for arn in policies:
            iamc.attach_role_policy(RoleName=label, PolicyArn=arn)
        lambdac.create_function(
            FunctionName=label,
            Runtime='python3.6',
            Role=role['Role']['Arn'],
            Handler='handler.lambda_handler',
            Code={'ZipFile': package.getvalue()},
        )
        for _ in range(5):
            lambdaf = lambdac.get_function(
                FunctionName=label,
            )
            if not lambdaf['Configuration']['FunctionArn']:
                sleep(3)
        now = datetime.datetime.utcnow()
        dow = { 0: 1, 1: 2, 2: 3, 3: 4, 4: 5, 5: 6, 6: 0 }[now.weekday()]
        expire_dow = ((dow + 2) % 7)
        cron_expire_dow = expire_dow + 1
        eventsc.put_rule(
            Name=label,
            RoleArn=role['Role']['Arn'],
            ScheduleExpression='cron({} {} ? * {} *)'.format(
                *(chain(override) if override else chain((now.minute, now.hour, cron_expire_dow)))),
            State='ENABLED',
        )
        eventsc.put_targets(
            Rule=label,
            Targets=[
                {
                    'Arn': lambdaf['Configuration']['FunctionArn'],
                    'Id': label,
                }
            ]
        )
        rule = eventsc.describe_rule(Name=label)
        lambdac.add_permission(
            FunctionName=label,
            StatementId=label,
            Action="lambda:InvokeFunction",
            Principal="events.amazonaws.com",
            SourceArn=rule['Arn'],
        )
    except Exception as e:
        error('{}\nCleaning up...'.format(e))
        setup_expiry_slate(common, policies, label, eventsc, lambdac, iamc)

def efs_populate(dir, competition=None, dataset=None, recreate=None):
    Path(dir).mkdir(parents=True, exist_ok=True)
    instance_id = get_instanceid()
    if not instance_id:
        url = gcspath(competition=competition, dataset=dataset)
        download(dir, url, recreate)
    else:
        region = get_region()
        efs_client = boto3.client('efs', region_name=region)
        ec2_client = boto3.client('ec2', region_name=region)
        label = sanitize(competition or dataset)
        fs_response = efs_client.describe_file_systems(CreationToken=label)
        efs = next(iter(fs_response['FileSystems']), None)
        if recreate:
            if efs:
                try:
                    fs_response = efs_client.describe_mount_targets(FileSystemId=efs['FileSystemId'])
                    for target in fs_response['MountTargets']:
                        efs_client.delete_mount_target(MountTargetId=target['MountTargetId'])
                    efs_client.delete_file_system(FileSystemId=efs['FileSystemId'])
                    # i want sg associated with filesystem... cannot... must use tags
                    sg_response = ec2_client.describe_security_groups(
                        Filters=[
                            {
                                'Name': 'group-name',
                                'Values': [
                                    label,
                                ]
                            },
                        ],
                    )
                    sg = next(iter(sg_response['SecurityGroups']), None)
                    if sg:
                        ec2_client.delete_security_group(GroupId=sg['GroupId'])
                    efs = None
                except Exception as e:
                    error("Could not delete filesystem {}: {}".format(efs['FileSystemId'], str(e)))
        fs_id = None
        if efs:
            fs_id = efs['FileSystemId']
        else:
            fs_response = efs_client.create_file_system(
                CreationToken=label,
                Tags=[
                    {
                        'Key': 'name',
                        'Value': label,
                    },
                ],
            )
            fs_id = fs_response['FileSystemId']
        try:
            ec2_client.create_security_group(
                Description=label,
                GroupName=label,
                VpcId=get_vpc(),
            )
        except botocore.exceptions.ClientError as e:
            if e.response.get('Error', {}).get('Code', 'Unknown') != 'InvalidGroup.Duplicate':
                raise e
        sg_response = ec2_client.describe_security_groups(
            Filters=[
                {
                    'Name': 'group-name',
                    'Values': [
                        label,
                    ]
                },
            ],
        )
        sg = next(iter(sg_response['SecurityGroups']), None)
        sg_id = sg['GroupId']
        try:
            ec2_client.authorize_security_group_ingress(
                GroupId=sg_id,
                IpPermissions=[
                    {
                        'FromPort': 2049,
                        'IpProtocol': 'tcp',
                        'IpRanges': [
                            {
                                'CidrIp': '0.0.0.0/0',
                                'Description': label,
                            },
                        ],
                        'ToPort': 2049,
                    },
                ],
            )
        except botocore.exceptions.ClientError as e:
            if e.response.get('Error', {}).get('Code', 'Unknown') != 'InvalidPermission.Duplicate':
                raise e

        subnet_id = get_subnet()
        for _ in range(5):
            try:
                efs_client.create_mount_target(
                    FileSystemId=fs_id,
                    SubnetId=subnet_id,
                    SecurityGroups=[
                        sg_id,
                    ],
                )
            except efs_client.exceptions.MountTargetConflict:
                break
            except efs_client.exceptions.IncorrectFileSystemLifeCycleState as e:
                error('Retrying create mount target following: {}'.format(str(e)))
                sleep(3)
                pass

        target = None
        for _ in range(24):
            fs_response = efs_client.describe_mount_targets(
                FileSystemId=fs_id,
            )
            target = next(iter([x for x in fs_response['MountTargets'] if x['LifeCycleState'] == 'available']), None)
            if target:
                break
            possible = next(iter(fs_response['MountTargets']), None)
            if possible:
                error('Target found with LifeCycleState: {}'.format(possible['LifeCycleState']))
            sleep(5)

        if target:
            url = gcspath(competition=competition, dataset=dataset)
            if not url:
                error('Could not find bucket for {}'.format(label))
            elif not mount_retry(dir, region, fs_id):
                error("Cannot mount {} to {}".format(fs_id, dir))
            elif 0 != os.system("sudo chmod go+rw {}".format(dir)):
                error("Cannot chmod {} for write".format(dir))
            else:
                du = 0
                for f in Path(dir).glob('**/*'):
                    if f.is_file():
                        du += f.stat().st_size
                    if du > 2**30:
                        break
                if du <= 2**30:
                    gsutil_rsync_retry(dir, url)
        else:
            error('Could not create mount target')

def ebs_volume(dir, competition=None, dataset=None, recreate=None):
    Path(dir).mkdir(parents=True, exist_ok=True)
    volume = None
    instance_id = get_instanceid()
    if not instance_id:
        url = gcspath(competition=competition, dataset=dataset)
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
        volumes = ec2.volumes.filter(
            Filters=[{'Name': 'tag:name', 'Values': [label]},
                     {'Name': 'availability-zone', 'Values': [get_az()]},
                     ],
        )
        volume = next(iter(volumes), None)
        if recreate:
            if snapshot:
                snapshot.delete()
                snapshot = None
            if volume:
                volume.delete()
                volume = None
        client = boto3.client('ec2', region_name=region)
        url = gcspath(competition=competition, dataset=dataset)
        if not url:
            error("Could not find bucket for {}".format(competition or dataset))
            return None

        if not volume:
            if not snapshot:
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
            try:
                client.get_waiter('volume_available').wait(VolumeIds=[volume.id])
            except botocore.exceptions.WaiterError as e:
                error("VolumeId {}, {}".format(volume.id, str(e)))
                return None
        if not volume:
            error("Could not create volume")
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
            # might already be attached, warn and continue
            error("attach_volume {}: {}".format(volume.id, str(e)))
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
                    fstype = subprocess.run(shlex.split('sudo blkid -s TYPE -o value {}'.format(device)), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    try:
                        fstype.check_returncode()
                        fstype = fstype.stdout.decode('utf-8').rstrip()
                    except subprocess.CalledProcessError:
                        fstype = None
                        if 0 != os.system("sudo mkfs.ext4 {}".format(device)):
                            error("Cannot mkfs.ext4 {}".format(device))
                            break
                    if 0 != os.system("sudo bash -c 'grep -wqs {} /proc/mounts || mount -o rw,user {} {}'".format(device, device, dir)):
                        error("Cannot mount {} to {}".format(device, dir))
                    elif 0 != os.system("sudo chmod 777 {}".format(dir)):
                        error("Cannot chmod {} for write".format(dir))
                    elif not fstype:
                        gsutil_rsync_retry(dir, url)
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
                            # just in case, already the default
                            'DeleteOnTermination': False,
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
    return next(iter([x for x in result.stdout.decode().split('\n') if re.compile('^gs://').search(x)]), None)
