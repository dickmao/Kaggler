# -*- coding: utf-8 -*-
# pylint: disable=wrong-import-position

# testing:
# project = 'api-project-421333809285'
# competition = 'seti-breakthrough-listen'
# label = competition
# service_account_json = os.path.join(os.path.expanduser("~"), ".config/gcloud/gat-service-account.json")
# os.environ["KAGGLE_USERNAME"] = "dicksbu"
# os.environ["KAGGLE_KEY"] = ""
# from kaggler.gcspath import *
# dir = 'mnt'
# url = 'gs://kds-94ae5957f2e2a27e2db7363a1e29443961556e8f41a50a7d1420d914'

import os
from pathlib import Path
from urllib.request import urlopen, Request
from googleapiclient import discovery
from googleapiclient.errors import HttpError
from time import sleep
from ..gcspath import gcspath, download, sanitize, authenticate, url_size, error, \
    disk_ensure_format, disk_ensure_data

def get_project():
    try:
        return urlopen(Request('http://metadata.google.internal/computeMetadata/v1/project/project-id', headers={'Metadata-Flavor': 'Google'}), timeout=1) \
            .read().decode()
    except:
        pass
    return None

def get_instance_id():
    try:
        return urlopen(Request('http://metadata.google.internal/computeMetadata/v1/instance/id', headers={'Metadata-Flavor': 'Google'}), timeout=1) \
            .read().decode()
    except:
        pass
    return None

def get_instance_name():
    try:
        return urlopen(Request('http://metadata.google.internal/computeMetadata/v1/instance/name', headers={'Metadata-Flavor': 'Google'}), timeout=1) \
            .read().decode()
    except:
        pass
    return None

def get_mac():
    try:
        return urlopen(Request('http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/mac', headers={'Metadata-Flavor': 'Google'})) \
            .read().decode()
    except:
        pass
    return None

def get_subnet():
    try:
        return urlopen(Request('http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/subnetmask', headers={'Metadata-Flavor': 'Google'})) \
            .read().decode()
    except:
        pass
    return None

def get_vpc():
    try:
        return os.path.basename(urlopen(Request('http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/network', headers={'Metadata-Flavor': 'Google'})) \
                                .read().decode())
    except:
        pass
    return None

def get_zone():
    try:
        return os.path.basename(urlopen(Request('http://metadata.google.internal/computeMetadata/v1/instance/zone', headers={'Metadata-Flavor': 'Google'})) \
            .read().decode())
    except:
        pass
    return None

def mount_retry(dir, region, fs_id, fs_ip):
    pass

def setup_expiry(label, override=()):
    label = sanitize(label)

def bump_expiry(label, override=()):
    pass

def disk_get(compute, project, zone, name):
    try:
        disks = compute.disks().list(project=project, zone=zone).execute()
    except HttpError as e:
        if e.stat_code != 404:
            raise e
    except Exception as e:
        raise e
    return next(filter(lambda item: item['name'] == name, disks['items']), None)

def wait_op(compute, project, zone, req):
    op = req.execute()
    while True:
        result = compute.zoneOperations().get(
            project=project,
            zone=zone,
            operation=op['name'],
        ).execute()
        if result['status'] == 'DONE':
            if 'error' in result:
                raise Exception(result['error'])
            break
        sleep(1)

def disk_populate(dir, competition=None, dataset=None, recreate=None, service_account_json=None, override=()):
    Path(dir).mkdir(parents=True, exist_ok=True)
    credentials = None
    if service_account_json:
        credentials = authenticate(service_account_json)
    service = discovery.build('serviceusage', 'v1', credentials=credentials)
    project = get_project()
    if service.services().get(
            name='projects/{}/services/compute.googleapis.com'.format(project),
    ).execute().get('state') == 'DISABLED':
        op = service.services().enable(
            name='projects/{}/services/compute.googleapis.com'.format(project),
        ).execute()
        while True:
            result = service.operations().get(
                name=op['name'],
            ).execute()
            if result['done']:
                if 'error' in result:
                    raise Exception(result['error'])
                break
            sleep(1)
    compute = discovery.build('compute', 'v1', credentials=credentials, cache_discovery=False)
    label = sanitize(competition or dataset)

    zone = get_zone()
    disk = disk_get(compute, project, zone, label)
    if not disk:
        recreate = True
        url = gcspath(competition=competition, dataset=dataset)
        if not url:
            error("Could not find bucket for {}".format(label))
            raise
        sz = url_size(url)
        if not sz:
            error("Could not size bucket for {}".format(label))
            raise
        wait_op(compute, project, zone,
                compute.disks().insert(
                    project=project,
                    zone=zone,
                    body={
                        "name": label,
                        "description": "kaggler-gcp",
                        "sizeGb": sz,
                    }))
        disk = disk_get(compute, project, zone, label)
    if not disk:
        error("Could not create disk {}".format(label))
        return None
    device = '/dev/sdb'
    if recreate:
        wait_op(compute, project, zone,
                compute.instances().attachDisk(
                    project=project,
                    zone=zone,
                    instance=get_instance_name(),
                    body={
                        'source': disk['selfLink']
                    }))
        disk_ensure_format(device)
        disk_ensure_data(dir, url)

        info = compute.instances().get(project=project, zone=zone,
                                       instance='instance-1').execute()
        attached = next(filter(lambda disk: os.path.basename(disk['source']) == label,
                               info["disks"]), None)
        if not attached:
            error("Could not determine deviceName for {}".format(label))
            raise
        else:
            wait_op(compute, project, zone,
                    compute.instances().detachDisk(
                        project=project,
                        zone=zone,
                        instance=get_instance_name(),
                        deviceName=attached['deviceName'],
                    ))
    wait_op(compute, project, zone,
            compute.instances().attachDisk(
                project=project,
                zone=zone,
                instance=get_instance_name(),
                body={
                    'source': disk['selfLink'],
                    'mode': "READ_ONLY",
                }))
    disk_ensure_format(device)

def filestore_populate(dir, competition=None, dataset=None, recreate=None, override=()):
    Path(dir).mkdir(parents=True, exist_ok=True)
    instance_id = get_instance_id()
    if not instance_id:
        url = gcspath(competition=competition, dataset=dataset)
        download(dir, url, recreate)
    else:
        project = get_project()
        zone = get_zone()
        service = discovery.build('serviceusage', 'v1')
        if service.services().get(
                name='projects/{}/services/file.googleapis.com'.format(project),
        ).execute().get('state') == 'DISABLED':
            op = service.services().enable(
                name='projects/{}/services/file.googleapis.com'.format(project),
            ).execute()
            while True:
                result = service.operations().get(
                    name=op['name'],
                ).execute()
                if result['done']:
                    if 'error' in result:
                        raise Exception(result['error'])
                    break
                sleep(1)
        file = discovery.build('file', 'v1', cache_discovery=False)
        label = sanitize(competition or dataset)
        parent = 'projects/%s/locations/%s' % (project, zone)
        try:
            fs = file.projects().locations().instances().get(name='{}/instances/{}'.format(parent, label)).execute()
        except HttpError as e:
            if e.stat_code != 404:
                raise e
        except Exception as e:
            raise e

        if fs:
            bump_expiry(label)
        else:
            op = file.projects().locations().instances().create(
                    parent=parent,
                    instanceId=label,
                    body={
                        'fileShares': [{
                            'capacityGb': 1024,
                            'name': 'test'
                        }],
                        'networks': [{
                            'network': 'default'
                        }],
                        'tier': 'STANDARD',
                    },
            ).execute()
            while True:
                result = file.projects().locations().operations().get(
                    name=op['name'],
                ).execute()
                if result['done']:
                    if 'error' in result:
                        raise Exception(result['error'])
                    break
                sleep(1)
            setup_expiry(label)
