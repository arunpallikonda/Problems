import boto3

rds = boto3.client('rds')
redshift = boto3.client('redshift')

def get_snapshot_count(service: str, snapshot_type='manual') -> int:
    count = 0
    if service == 'rds':
        paginator = rds.get_paginator('describe_db_snapshots')
        for page in paginator.paginate(SnapshotType=snapshot_type):
            count += len(page['DBSnapshots'])
    elif service == 'redshift':
        paginator = redshift.get_paginator('describe_cluster_snapshots')
        for page in paginator.paginate(SnapshotType=snapshot_type):
            count += len(page['Snapshots'])
    return count

def get_storage_size(service: str, identifier: str) -> int:
    if service == 'rds':
        response = rds.describe_db_instances(DBInstanceIdentifier=identifier)
        return response['DBInstances'][0]['AllocatedStorage']
    elif service == 'redshift':
        response = redshift.describe_clusters(ClusterIdentifier=identifier)
        cluster = response['Clusters'][0]
        node_type = cluster['NodeType']
        node_count = cluster['NumberOfNodes']
        storage_per_node = {
            'dc2.large': 160,
            'dc2.8xlarge': 2560,
            'ra3.xlplus': 32000,
            'ra3.4xlarge': 64000,
        }
        return node_count * storage_per_node.get(node_type, 0)
    return 0

def validate_preconditions(rds_id=None, redshift_id=None, snapshot_buffer=2, snapshot_limit_rds=100, snapshot_limit_redshift=100):
    status = {}

    if rds_id:
        rds_snap_count = get_snapshot_count('rds')
        rds_storage = get_storage_size('rds', rds_id)
        status['rds'] = {
            'snapshot_count': rds_snap_count,
            'within_snapshot_limit': rds_snap_count + snapshot_buffer <= snapshot_limit_rds,
            'source_storage_gb': rds_storage
        }

    if redshift_id:
        redshift_snap_count = get_snapshot_count('redshift')
        redshift_storage = get_storage_size('redshift', redshift_id)
        status['redshift'] = {
            'snapshot_count': redshift_snap_count,
            'within_snapshot_limit': redshift_snap_count + snapshot_buffer <= snapshot_limit_redshift,
            'source_storage_gb': redshift_storage
        }

    return status

# Example usage:
# print(validate_preconditions(rds_id='your-rds-instance-id', redshift_id='your-redshift-cluster-id'))
