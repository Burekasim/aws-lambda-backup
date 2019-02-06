# Copyright 2017 Oliver Siegmar
# Copyright 2019 Avi Keinan
#
# Forked from Oliver repo, modified to AMI with snapshots.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

'''
TODO:
Run for several days
'''

import os
import logging
from datetime import date

import boto3
from dateutil.relativedelta import relativedelta
from time import sleep

client = ''
ec2 = ''


def get_regions():
    if 'regions' in os.environ:
        return [region for region in os.environ['regions'].split(',')]
    else:
        return [region['RegionName'] for region in boto3.client('ec2', region_name='us-east-1').describe_regions()['Regions']]


def backup_tag():
    default_lmabda_backup_tag = "LambdaBackupConfiguration"
    if 'backup_tag' in os.environ:
        return str(os.environ['backup_tag'])
    else:
        return str(default_lmabda_backup_tag)


VERSION = '1.0'
logger = logging.getLogger()
logger.setLevel(logging.INFO)
lambda_backup_tag = backup_tag()


def lambda_handler(event, context):
    logger.info('Start ami/ebs-backup v{}'.format(VERSION))
    global ec2
    global client
    regions = get_regions()
    for region in regions:
        logger.info(f'Backup up region: {region}')
        ec2 = boto3.resource('ec2', region_name=region)
        client = boto3.client('ec2', region_name=region)
        backup()
        expire()


def backup():
    instances = ec2.instances.filter(Filters=[{'Name': 'tag-key',
                                               'Values': [lambda_backup_tag]}])
    for instance in instances:
        try:
            backup_instance(instance)
        except:
            logging.exception('Error creating ami/snapshot for {}'.format(instance.id))


def backup_instance(instance):
    instance_tags = dict(map(lambda x: (x['Key'], x['Value']), instance.tags or []))
    instance_name = instance_tags.get('Name', '[unnamed]')
    backup_cfg_str = instance_tags[lambda_backup_tag]
    backup_cfg = parse_config(instance, instance_name, backup_cfg_str)
    backup_label, retention = calc_retention(backup_cfg)
    backup_yearweek = date.today().strftime("%Y-%m-%d")

    if not backup_label:
        logger.info('Skip backup of instance {} ({}); {} is {}'
                    .format(instance.id, instance_name, lambda_backup_tag, backup_cfg_str))
        return

    delete_date_fmt = (date.today() + retention).strftime('%Y-%m-%d')
    logger.info('Work on instance {} ({}); Create {} backups to be deleted on {}'
                .format(instance.id, instance_name, backup_label, delete_date_fmt))

    ami = client.create_image(InstanceId=instance.id, Name=f'{instance.instance_id} {backup_label} {backup_yearweek}', NoReboot=True)
    describe_ami = client.describe_images(ImageIds=[ami['ImageId']])
    ami_snapshots_ids = [x['Ebs']['SnapshotId'] for x in describe_ami['Images'][0]['BlockDeviceMappings']]
    while not ami_snapshots_ids:
        sleep(1)
        describe_ami = client.describe_images(ImageIds=[ami['ImageId']])
        try:
            ami_snapshots_ids = [x['Ebs']['SnapshotId'] for x in describe_ami['Images'][0]['BlockDeviceMappings']]
        except KeyError:
            continue
    ami_snapshots_ids.append(ami['ImageId'])
    #ami_snapshots_ids.append(describe_ami['Images'][0]['ImageId'])

    if ami_snapshots_ids:
        logger.info('Create tags for snapshots {}'.format(ami_snapshots_ids))
        tags = {
            'Name': f'{instance.instance_id} {backup_label}',
            'BackupLabel': backup_label,
            'InstanceId': instance.instance_id,
            'InstanceName': instance_name,
            'DeleteOn': delete_date_fmt
        }
        tag_list = list(map(lambda kv: {'Key': kv[0], 'Value': kv[1]}, list(tags.items())))
        client.create_tags(Resources=ami_snapshots_ids, Tags=tag_list)


def parse_config(instance, instance_name, config):
    try:
        backup_configuration = list(map(int, config.split(',')))
        if any(i < 0 for i in backup_configuration):
            raise ValueError('Values must be >= 0')
        return backup_configuration
    except:
        raise ValueError('Syntax error in {} of {} ({}): {}'
                         .format(lambda_backup_tag, instance.id, instance_name, config))


def calc_retention(backup_configuration):
    today = date.today()
    r_daily, r_weekly, r_monthly, r_yearly = backup_configuration
    if today.day == 1:
        if today.month == 1 and r_yearly > 0:
            return 'yearly', relativedelta(years=r_yearly)
        if r_monthly > 0:
            return 'monthly', relativedelta(months=r_monthly)
    if today.weekday() == 6 and r_weekly > 0:
        return 'weekly', relativedelta(weeks=r_weekly)
    if r_daily > 0:
        return 'daily', relativedelta(days=r_daily)
    return None, None


def expire():
    delete_fmt = date.today().strftime('%Y-%m-%d')
    images = client.describe_images(Owners=['self'], Filters=[{'Name': 'tag:DeleteOn', 'Values': [delete_fmt]}])
    images_ami = [x['ImageId'] for x in images['Images']]

    snapshots = ec2.snapshots.filter(OwnerIds=['self'], Filters=[{'Name': 'tag:DeleteOn', 'Values': [delete_fmt]}])

    for image in images_ami:
        client.deregister_image(ImageId=image)

    for snapshot in snapshots:
        logger.info('Remove snapshot {} (of volume {}) created at {}'.format(snapshot.id, snapshot.volume_id, snapshot.start_time))
        snapshot.delete()

