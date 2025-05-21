import boto3

def get_rds_account_limits():
    rds = boto3.client('rds')
    response = rds.describe_account_attributes()
    quota_map = {q['AccountQuotaName']: q for q in response['AccountQuotas']}
    return {
        'DBInstances': quota_map.get('DBInstances', {}),
        'DBClusters': quota_map.get('DBClusters', {}),
        'ManualSnapshots': quota_map.get('ManualSnapshots', {}),
        'ManualClusterSnapshots': quota_map.get('ManualClusterSnapshots', {}),
        'AllocatedStorage': quota_map.get('AllocatedStorage', {}),
    }

def get_redshift_account_limits():
    redshift = boto3.client('redshift')
    
    limits = {}

    # Get max-clusters from account attributes
    response = redshift.describe_account_attributes()
    for attr in response['AccountAttributes']:
        if attr['AttributeName'] == 'max-clusters':
            limits['MaxClusters'] = int(attr['AttributeValues'][0]['AttributeValue'])

    # Count current clusters
    paginator = redshift.get_paginator('describe_clusters')
    cluster_count = 0
    for page in paginator.paginate():
        cluster_count += len(page['Clusters'])
    limits['CurrentClusters'] = cluster_count

    # Count manual snapshots
    paginator = redshift.get_paginator('describe_cluster_snapshots')
    snapshot_count = 0
    for page in paginator.paginate(SnapshotType='manual'):
        snapshot_count += len(page['Snapshots'])
    limits['ManualSnapshots'] = snapshot_count

    return limits

def check_limit(current, requested, max_allowed):
    return {
        'current_count': current,
        'max_allowed': max_allowed,
        'is_in_limit': (current + requested) <= max_allowed
    }

def validate_capacity(requested_rds, requested_redshift, redshift_snapshot_limit=100):
    rds_limits = get_rds_account_limits()
    redshift_limits = get_redshift_account_limits()

    result = {
        'RDS_DBInstances': check_limit(
            rds_limits['DBInstances'].get('Used', 0),
            requested_rds['DBInstances'],
            rds_limits['DBInstances'].get('Max', 0)
        ),
        'RDS_DBClusters': check_limit(
            rds_limits['DBClusters'].get('Used', 0),
            requested_rds['DBClusters'],
            rds_limits['DBClusters'].get('Max', 0)
        ),
        'RDS_ManualSnapshots': check_limit(
            rds_limits['ManualSnapshots'].get('Used', 0),
            requested_rds['ManualSnapshots'],
            rds_limits['ManualSnapshots'].get('Max', 0)
        ),
        'RDS_ManualClusterSnapshots': check_limit(
            rds_limits['ManualClusterSnapshots'].get('Used', 0),
            requested_rds['ManualClusterSnapshots'],
            rds_limits['ManualClusterSnapshots'].get('Max', 0)
        ),
        'RDS_AllocatedStorage': check_limit(
            rds_limits['AllocatedStorage'].get('Used', 0),
            requested_rds['AllocatedStorage'],
            rds_limits['AllocatedStorage'].get('Max', 0)
        ),
        'Redshift_Clusters': check_limit(
            redshift_limits.get('CurrentClusters', 0),
            requested_redshift['Clusters'],
            redshift_limits.get('MaxClusters', 0)
        ),
        'Redshift_ManualSnapshots': check_limit(
            redshift_limits.get('ManualSnapshots', 0),
            requested_redshift['Snapshots'],
            redshift_snapshot_limit
        )
    }

    return result

# Example inputs
requested_rds = {
    'DBInstances': 2,
    'DBClusters': 1,
    'ManualSnapshots': 3,
    'ManualClusterSnapshots': 1,
    'AllocatedStorage': 500
}

requested_redshift = {
    'Clusters': 2,
    'Snapshots': 5
}

# Run locally:
# print(validate_capacity(requested_rds, requested_redshift))
