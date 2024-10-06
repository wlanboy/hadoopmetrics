#pip install prometheus_client
import subprocess
import re
import sys
import time
from prometheus_client import start_http_server, Gauge

# Function to get directory sizes from HDFS
def get_hdfs_directory_sizes(path):
    command = f"hdfs dfs -du -s -h {path}"
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    return result.stdout

# Function to parse the HDFS output
def parse_hdfs_output(output):
    directories = {}
    lines = output.strip().split('\n')
    for line in lines:
        if line:
            print(line)
            size, directory = re.split(r'\s+', line, maxsplit=1)
            directories[directory] = size
    return directories

# Function to convert human-readable sizes to bytes
def size_to_bytes(size):
    size_units = {'K': 1e3, 'M': 1e6, 'G': 1e9, 'T': 1e12}
    if size[-1] in size_units:
        return float(size[:-1]) * size_units[size[-1]]
    return float(size)

# Main function to export metrics
def export_metrics(path):
    output = get_hdfs_directory_sizes(path)
    directories = parse_hdfs_output(output)
    if not directories:
        print("No directories found.")
        return
    
    gauge = Gauge('hdfs_directory_size_bytes', 'Size of HDFS directories', ['directory'])
    
    for directory, size in directories.items():
        gauge.labels(directory=directory).set(size_to_bytes(size))

if __name__ == '__main__':

    hdfs_path = sys.argv if len(sys.argv) > 1 else '/'
    start_http_server(8000)
    while True:
        export_metrics(hdfs_path)
        time.sleep(300)
