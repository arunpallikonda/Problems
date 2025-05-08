import boto3

rds = boto3.client('rds')
redshift = boto3.client('redshift')

def get_rds_account_limits():
    quotas = rds.describe_account_attributes()
    limits = {}
    for item in quotas['AccountQuotas']:
        limits[item['AccountQuotaName']] = {
            'max': item['Max'],
            'used': item['Used']
        }
    return limits

def get_rds_snapshot_counts():
    rds_snap_count = 0
    rds_cluster_snap_count = 0

    paginator = rds.get_paginator('describe_db_snapshots')
    for page in paginator.paginate(SnapshotType='manual'):
        rds_snap_count += len(page['DBSnapshots'])

    paginator = rds.get_paginator('describe_db_cluster_snapshots')
    for page in paginator.paginate(SnapshotType='manual'):
        rds_cluster_snap_count += len(page['DBClusterSnapshots'])

    return rds_snap_count, rds_cluster_snap_count

def get_rds_storage_allocated():
    total_storage = 0
    paginator = rds.get_paginator('describe_db_instances')
    for page in paginator.paginate():
        for instance in page['DBInstances']:
            total_storage += instance['AllocatedStorage']
    return total_storage

def get_redshift_snapshot_count():
    count = 0
    paginator = redshift.get_paginator('describe_cluster_snapshots')
    for page in paginator.paginate(SnapshotType='manual'):
        count += len(page['Snapshots'])
    return count

def get_redshift_cluster_count_and_storage():
    response = redshift.describe_clusters()
    total_storage = 0
    total_clusters = len(response['Clusters'])
    storage_per_node = {
        'dc2.large': 160,
        'dc2.8xlarge': 2560,
        'ra3.xlplus': 32000,
        'ra3.4xlarge': 64000,
    }

    for cluster in response['Clusters']:
        node_type = cluster['NodeType']
        node_count = cluster['NumberOfNodes']
        total_storage += node_count * storage_per_node.get(node_type, 0)

    return total_clusters, total_storage

def validate_limits(snapshot_buffer=2):
    rds_limits = get_rds_account_limits()
    rds_snap_count, rds_cluster_snap_count = get_rds_snapshot_counts()
    rds_allocated_storage = get_rds_storage_allocated()
    redshift_snap_count = get_redshift_snapshot_count()
    redshift_cluster_count, redshift_allocated_storage = get_redshift_cluster_count_and_storage()

    return {
        'rds_instance_snapshots': {
            'used': rds_snap_count,
            'max': rds_limits.get('ManualSnapshots', {}).get('max'),
            'can_add': (rds_snap_count + snapshot_buffer) <= rds_limits.get('ManualSnapshots', {}).get('max', float('inf'))
        },
        'rds_cluster_snapshots': {
            'used': rds_cluster_snap_count,
            'max': rds_limits.get('ManualClusterSnapshots', {}).get('max'),
            'can_add': (rds_cluster_snap_count + snapshot_buffer) <= rds_limits.get('ManualClusterSnapshots', {}).get('max', float('inf'))
        },
        'rds_allocated_storage_gb': {
            'used': rds_allocated_storage,
            'max': rds_limits.get('AllocatedStorage', {}).get('max'),
            'can_add': rds_allocated_storage < rds_limits.get('AllocatedStorage', {}).get('max', float('inf'))
        },
        'rds_instances': {
            'used': rds_limits.get('DBInstances', {}).get('used'),
            'max': rds_limits.get('DBInstances', {}).get('max'),
        },
        'rds_clusters': {
            'used': rds_limits.get('DBClusters', {}).get('used'),
            'max': rds_limits.get('DBClusters', {}).get('max'),
        },
        'redshift_snapshots': {
            'used': redshift_snap_count,
            'note': 'No API for Redshift snapshot limits; manage manually'
        },
        'redshift_clusters': {
            'used': redshift_cluster_count,
            'note': 'No API for Redshift cluster limit; manage manually'
        },
        'redshift_estimated_storage_gb': {
            'used': redshift_allocated_storage,
            'note': 'Based on node type and count'
        }
    }

# Example usage:
if _name_ == '_main_':
    import json
    print(json.dumps(validate_limits(), indent=2))
