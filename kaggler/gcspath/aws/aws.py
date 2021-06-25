# -*- coding: utf-8 -*-
# pylint: disable=wrong-import-position

import os
import re
import boto3
import botocore
import io
import socket
import zipfile
import datetime
from operator import itemgetter
from itertools import chain
from pathlib import Path
from urllib.request import urlopen
from time import sleep

from ..gcspath import gcspath, error, download, \
    sanitize, url_size, disk_ensure_format, disk_ensure_data

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

def mount_retry(dir, region, fs_id, fs_ip):
    addr = '{}.efs.{}.amazonaws.com'.format(fs_id, region)
    for _ in range(15):
        try:
            socket.gethostbyname(addr)
        except socket.gaierror as e:
            error('gethostbyname ({}): {}'.format(addr, str(e)))
        if 0 == os.system("sudo bash -c 'grep -qs {} /proc/mounts || mount -t nfs4 -o nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2 {}:/ {}'".format(fs_id, fs_ip, dir)):
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

        for _ in range(60):
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
        response = iamc.create_role(RoleName=label, AssumeRolePolicyDocument="""{
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
        for arn in policies:
            iamc.attach_role_policy(RoleName=label, PolicyArn=arn)

        role_iterations = 24
        for j in range(role_iterations):
            try:
                lambdac.create_function(
                    FunctionName=label,
                    Runtime='python3.6',
                    Role=response['Role']['Arn'],
                    Handler='handler.lambda_handler',
                    Code={'ZipFile': package.getvalue()},
                )
            except lambdac.exceptions.InvalidParameterValueException as e:
                if j >= role_iterations - 1:
                    raise e
                else:
                    pass
            except lambdac.exceptions.ResourceConflictException:
                break
            sleep(5)

        for _ in range(60):
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
            RoleArn=response['Role']['Arn'],
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

def minutes_to(expr):
    """Distance in minutes between expr and now."""
    regex = re.compile(r"""cron\(([^)]+)\)""")
    (end_minute, end_hour, end_dow) = list(map(int, itemgetter(0, 1, 4)(regex.match(expr).group(1).split())))

    beg = datetime.datetime.utcnow()
    beg_dow = { 0: 2, 1: 3, 2: 4, 3: 5, 4: 6, 5: 7, 6: 1 }[beg.weekday()]
    minute_invert = (end_minute - beg.minute < 0)
    hour_invert = ((end_hour - beg.hour < 0) or (end_hour == beg.hour and minute_invert))
    day_invert = ((end_dow - beg_dow < 0) or (end_dow == beg_dow and hour_invert))
    days = ((end_dow + 7) if day_invert else end_dow) - beg_dow - (1 if hour_invert else 0)
    hours = ((end_hour + 24) if hour_invert else end_hour) - beg.hour - (1 if minute_invert else 0)
    minutes = ((end_minute + 60) if minute_invert else end_minute) - beg.minute
    return 1440*days + 60*hours + minutes

def bump_expiry(label, override=()):
    label = sanitize(label)
    region = get_region()
    iamc = boto3.client('iam', region_name=region)
    lambdac = boto3.client('lambda', region_name=region)
    eventsc = boto3.client('events', region_name=region)

    now = datetime.datetime.utcnow()
    dow = { 0: 1, 1: 2, 2: 3, 3: 4, 4: 5, 5: 6, 6: 0 }[now.weekday()]
    expire_dow = ((dow + 2) % 7)
    cron_expire_dow = expire_dow + 1
    schedule_expr = 'cron({} {} ? * {} *)'.format(
        *(chain(override) if override else chain((now.minute, now.hour, cron_expire_dow))))
    rule = eventsc.describe_rule(Name=label)
    diff = abs(minutes_to(rule['ScheduleExpression']) - minutes_to(schedule_expr))
    if diff < 180:
        return

    delete_iterations = 3
    for j in range(delete_iterations):
        try:
            lambdac.remove_permission(
                FunctionName=label,
                StatementId=label,
            )
        except lambdac.exceptions.ResourceNotFoundException:
            break
        except lambdac.exceptions.TooManyRequestsException as e:
            if j >= delete_iterations - 1 :
                raise e
            else:
                pass
        sleep(5)

    targets_response = eventsc.list_targets_by_rule(
        Rule=label,
    )

    for j in range(delete_iterations):
        try:
            eventsc.remove_targets(Rule=label, Ids=[ label ])
        except eventsc.exceptions.ResourceNotFoundException:
            break
        except eventsc.exceptions.ConcurrentModificationException as e:
            if j >= delete_iterations - 1 :
                raise e
            else:
                pass
        sleep(5)

    for j in range(delete_iterations):
        try:
            eventsc.delete_rule(Name=label)
        except eventsc.exceptions.ResourceNotFoundException:
            break
        except eventsc.exceptions.ConcurrentModificationException as e:
            if j >= delete_iterations - 1 :
                raise e
            else:
                pass
        sleep(5)

    role = iamc.get_role(RoleName=label)
    eventsc.put_rule(
        Name=label,
        RoleArn=role['Role']['Arn'],
        ScheduleExpression=schedule_expr,
        State='ENABLED',
    )
    eventsc.put_targets(
        Rule=label,
        Targets=[ { 'Arn': target['Arn'], 'Id': target['Id'] } for target in targets_response['Targets']],
    )
    rule = eventsc.describe_rule(Name=label)
    lambdac.add_permission(
        FunctionName=label,
        StatementId=label,
        Action="lambda:InvokeFunction",
        Principal="events.amazonaws.com",
        SourceArn=rule['Arn'],
    )

def efs_populate(dir, competition=None, dataset=None, recreate=None, override=()):
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
                if 0 != os.system("sudo bash -c '! grep -qs {} /proc/mounts || fusermount -u {}'".format(dir, dir)):
                    error("Cannot unmount {}".format(dir))
                    raise
                fs_response = efs_client.describe_mount_targets(FileSystemId=efs['FileSystemId'])
                for target in fs_response['MountTargets']:
                    efs_client.delete_mount_target(MountTargetId=target['MountTargetId'])
                for _ in range(60):
                    fs_response = efs_client.describe_mount_targets(FileSystemId=efs['FileSystemId'])
                    if not next(iter(fs_response['MountTargets']), None):
                        break
                    sleep(3)
                efs_client.delete_file_system(FileSystemId=efs['FileSystemId'])
                for _ in range(60):
                    fs_response = efs_client.describe_file_systems(CreationToken=label)
                    if not next(iter(fs_response['FileSystems']), None):
                        break
                    sleep(3)

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
        if efs:
            fs_id = efs['FileSystemId']
            bump_expiry(label, override)
        else:
            setup_expiry(label, override)
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
        for _ in range(4): # ThrottlingException max retries 4
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
                sleep(30)
                pass

        for _ in range(20):
            fs_response = efs_client.describe_mount_targets(
                FileSystemId=fs_id,
            )
            target = next(iter([x for x in fs_response['MountTargets'] if x['LifeCycleState'] == 'available']), None)
            if target:
                break
            possible = next(iter(fs_response['MountTargets']), None)
            if possible:
                error('Target found with LifeCycleState: {}'.format(possible['LifeCycleState']))
            sleep(15)

        if target:
            if not mount_retry(dir, region, fs_id, target['IpAddress']):
                error("Cannot mount {} to {}".format(fs_id, dir))
            elif 0 != os.system("sudo chmod go+rw {}".format(dir)):
                error("Cannot chmod {} for write".format(dir))
            else:
                url = gcspath(competition=competition, dataset=dataset)
                if url:
                    disk_ensure_data(dir, url)
                else:
                    error('Could not find bucket for {}'.format(label))
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
                sz = url_size(url)
                if not sz:
                    error("Could not size bucket for {}".format(label))
                    raise
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
        attached = False
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
                    break
            sleep(3)
        if not attached:
            error("Cannot attach {} to {}".format(volume.id, instance_id))
            return None

        disk_ensure_format(device)
        disk_ensure_data(dir, url)

        # create snapshot if not there
        # client.create_snapshot(
        #     Description=label,
        #     VolumeId=volume.id,
        # )

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
