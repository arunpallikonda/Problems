import boto3

def get_rds_account_limits():
    rds = boto3.client('rds')
    quotas = rds.describe_account_attributes()
    limits = {}
    for item in quotas['AccountQuotas']:
        limits[item['AccountQuotaName']] = {
            'max': item['Max'],
            'used': item['Used']
        }
    return limits

def get_redshift_account_limits():
    redshift = boto3.client('redshift')
    response = redshift.describe_account_attributes()
    limits = {}
    for attr in response['AccountAttributes']:
        limits[attr['AttributeName']] = int(attr['AttributeValues'][0]['AttributeValue'])
    return limits

def count_rds_snapshots():
    rds = boto3.client('rds')
    paginator = rds.get_paginator('describe_db_snapshots')
    instance_snapshots = sum(len(p['DBSnapshots']) for p in paginator.paginate(SnapshotType='manual'))

    paginator = rds.get_paginator('describe_db_cluster_snapshots')
    cluster_snapshots = sum(len(p['DBClusterSnapshots']) for p in paginator.paginate(SnapshotType='manual'))

    return instance_snapshots, cluster_snapshots

def get_rds_allocated_storage():
    rds = boto3.client('rds')
    paginator = rds.get_paginator('describe_db_instances')
    return sum(instance['AllocatedStorage'] for page in paginator.paginate() for instance in page['DBInstances'])

def count_redshift_snapshots():
    redshift = boto3.client('redshift')
    paginator = redshift.get_paginator('describe_cluster_snapshots')
    return sum(len(p['Snapshots']) for p in paginator.paginate(SnapshotType='manual'))

def get_redshift_cluster_usage():
    redshift = boto3.client('redshift')
    response = redshift.describe_clusters()
    clusters = response['Clusters']
    node_storage = {
        'dc2.large': 160,
        'dc2.8xlarge': 2560,
        'ra3.xlplus': 32000,
        'ra3.4xlarge': 64000
    }
    total_storage = 0
    for cluster in clusters:
        node_type = cluster['NodeType']
        count = cluster['NumberOfNodes']
        total_storage += count * node_storage.get(node_type, 0)
    return len(clusters), total_storage

def summarize_limits(snapshot_buffer=2):
    rds_limits = get_rds_account_limits()
    redshift_limits = get_redshift_account_limits()

    rds_inst_snaps, rds_cluster_snaps = count_rds_snapshots()
    rds_storage = get_rds_allocated_storage()

    redshift_snaps = count_redshift_snapshots()
    redshift_clusters, redshift_storage = get_redshift_cluster_usage()

    return {
        'RDS Instance Snapshots': {
            'Used': rds_inst_snaps,
            'Max': rds_limits.get('ManualSnapshots', {}).get('max'),
            'WithinLimit': rds_inst_snaps + snapshot_buffer <= rds_limits.get('ManualSnapshots', {}).get('max', float('inf'))
        },
        'RDS Cluster Snapshots': {
            'Used': rds_cluster_snaps,
            'Max': rds_limits.get('ManualClusterSnapshots', {}).get('max'),
            'WithinLimit': rds_cluster_snaps + snapshot_buffer <= rds_limits.get('ManualClusterSnapshots', {}).get('max', float('inf'))
        },
        'RDS Storage (GB)': {
            'Used': rds_storage,
            'Max': rds_limits.get('AllocatedStorage', {}).get('max'),
            'WithinLimit': rds_storage <= rds_limits.get('AllocatedStorage', {}).get('max', float('inf'))
        },
        'RDS Instances': {
            'Used': rds_limits.get('DBInstances', {}).get('used'),
            'Max': rds_limits.get('DBInstances', {}).get('max')
        },
        'RDS Clusters': {
            'Used': rds_limits.get('DBClusters', {}).get('used'),
            'Max': rds_limits.get('DBClusters', {}).get('max')
        },
        'Redshift Snapshots': {
            'Used': redshift_snaps,
            'Max': redshift_limits.get('max-number-of-snapshots'),
            'WithinLimit': redshift_snaps + snapshot_buffer <= redshift_limits.get('max-number-of-snapshots', float('inf'))
        },
        'Redshift Clusters': {
            'Used': redshift_clusters,
            'Max': redshift_limits.get('max-clusters'),
            'WithinLimit': redshift_clusters < redshift_limits.get('max-clusters', float('inf'))
        },
        'Redshift Estimated Storage (GB)': {
            'Used': redshift_storage,
            'Note': 'Based on node type estimate'
        }
    }

# Example usage:
if _name_ == '_main_':
    import json
    result = summarize_limits()
    print(json.dumps(result, indent=2))
