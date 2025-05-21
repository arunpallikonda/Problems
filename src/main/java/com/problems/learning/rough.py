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
    response = redshift.describe_account_attributes()
    limits = {}
    for attr in response['AccountAttributes']:
        if attr['AttributeName'] == 'max-clusters':
            limits['MaxClusters'] = int(attr['AttributeValues'][0]['AttributeValue'])
    return limits

def count_redshift_manual_snapshots():
    redshift = boto3.client('redshift')
    paginator = redshift.get_paginator('describe_cluster_snapshots')
    page_iterator = paginator.paginate(SnapshotType='manual')

    count = 0
    for page in page_iterator:
        count += len(page['Snapshots'])
    return count

def get_current_redshift_cluster_count():
    redshift = boto3.client('redshift')
    response = redshift.describe_clusters()
    return len(response['Clusters'])

def check_limit(used, requested, max_allowed):
    return {
        'current_count': used,
        'max_allowed': max_allowed,
        'is_in_limit': (used + requested) <= max_allowed
    }

def validate_capacity(requested_rds, requested_redshift, redshift_snapshot_limit=100):
    rds_limits = get_rds_account_limits()
    redshift_limits = get_redshift_account_limits()
    redshift_snapshot_count = count_redshift_manual_snapshots()
    redshift_cluster_count = get_current_redshift_cluster_count()

    result = {}

    result['RDS_DBInstances'] = check_limit(
        rds_limits['DBInstances'].get('Used', 0),
        requested_rds['DBInstances'],
        rds_limits['DBInstances'].get('Max', 0)
    )

    result['RDS_DBClusters'] = check_limit(
        rds_limits['DBClusters'].get('Used', 0),
        requested_rds['DBClusters'],
        rds_limits['DBClusters'].get('Max', 0)
    )

    result['RDS_ManualSnapshots'] = check_limit(
        rds_limits['ManualSnapshots'].get('Used', 0),
        requested_rds['ManualSnapshots'],
        rds_limits['ManualSnapshots'].get('Max', 0)
    )

    result['RDS_ManualClusterSnapshots'] = check_limit(
        rds_limits['ManualClusterSnapshots'].get('Used', 0),
        requested_rds['ManualClusterSnapshots'],
        rds_limits['ManualClusterSnapshots'].get('Max', 0)
    )

    result['RDS_AllocatedStorage'] = check_limit(
        rds_limits['AllocatedStorage'].get('Used', 0),
        requested_rds['AllocatedStorage'],
        rds_limits['AllocatedStorage'].get('Max', 0)
    )

    result['Redshift_Clusters'] = check_limit(
        redshift_cluster_count,
        requested_redshift['Clusters'],
        redshift_limits.get('MaxClusters', 0)
    )

    result['Redshift_ManualSnapshots'] = check_limit(
        redshift_snapshot_count,
        requested_redshift['Snapshots'],
        redshift_snapshot_limit
    )

    return result

# Example usage
requested_rds = {
    'DBInstances': 2,
    'DBClusters': 1,
    'ManualSnapshots': 3,
    'ManualClusterSnapshots': 1,
    'AllocatedStorage': 500  # in GB
}

requested_redshift = {
    'Clusters': 2,
    'Snapshots': 5
}

# Run locally
# print(validate_capacity(requested_rds, requested_redshift))
