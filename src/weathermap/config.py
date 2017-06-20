import os

db_host = os.environ.get('DB_HOST', 'postgres')
db_name = os.environ.get('DB_NAME', 'postgres')
db_user = os.environ.get('DB_USER', 'postgres')
db_password = os.environ.get('DB_PASSWORD', '')

namespace = os.environ['NAMESPACE']
bottleneck_bandwidth = os.environ.get('BOTTLENECK_BANDWIDTH', '250M')

iperf_image = 'smpio/iperf:2'
server_pod_name = 'iperf-server'
client_pod_name = 'iperf-client'
measurement_time_secs = 10
approx_prepare_time_secs = 20
complete_cluster_measurement_interval_sec = 24 * 60 * 60
