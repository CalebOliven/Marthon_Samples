import traceback
import sys
# Reconfigure the script so it outputs STDOUT and STDERR to a file
sys.stdout = open("./stdout.txt", "w")
sys.stderr = open("./stderr.txt", "w")


import psutil
import json
import socket
import psycopg2
import psycopg2.extras
import boto3

# Dev Log Imports
import re
import hashlib
import botocore

try: # Handle Py2 and Py3 imports
    from urllib.request import urlopen, Request, HTTPError
except ImportError:
    from urllib2 import urlopen, Request, HTTPError

from os import path
from datetime import datetime
from scandir import scandir # This must be added via pip install scandir, host systems are py2.7

SECRET_NAME = "$$$$$$$"
REGION_NAME = "$$$$$$"

METRICS_GATEWAY_ID_SECRET = "$$$$"

# This is the hostname to use for latency checks for DNS lookup
DNS_RESOLVER_CHECK_HOSTNAME = "dynamap-mapreadydata-rds.ngtoc-shared.chs.usgs.gov"

# Helper function to get EC2 hostname
def getEC2Hostname():
    hostname = socket.gethostname()
    if "igsaws" not in hostname.lower():
        hostname = "UNKNOWN"
    return hostname

# Helper function that gets the EC2's instance id
def getCurrentEC2ID():
    return urlopen("http://169.254.169.254/latest/meta-data/instance-id").read().decode("utf-8")

# Helper function that gets the machine type. ie: proto/test/stage/prod
def getMachineType():
    instance_id = getCurrentEC2ID()
    machine_type = None

    if instance_id and len(instance_id) > 0:
        ec2 = boto3.resource("ec2", region_name=REGION_NAME)
        instance = ec2.Instance(instance_id)
        tags = { tag.get("Key") : tag.get("Value") for tag in instance.tags }
        machine_type = tags.get("environment") or tags.get("function")
    
    return machine_type

MY_INSTANCE_ID = getCurrentEC2ID()
MY_HOSTNAME = getEC2Hostname()
MY_ENVIRONMENT = getMachineType()

_MEMO_SECRETS_DICT = None # Holds a memoized copy of secrets dict to reduce AWS calls
def getSecrets():
    global _MEMO_SECRETS_DICT
    if not _MEMO_SECRETS_DICT:
        session = boto3.session.Session()
        client = session.client(service_name='secretsmanager', region_name=REGION_NAME)    
        get_secret_value_response = client.get_secret_value(SecretId=SECRET_NAME)
        secret = get_secret_value_response['SecretString']
        secrets_dict = json.loads(secret)
        _MEMO_SECRETS_DICT = secrets_dict
    
    return _MEMO_SECRETS_DICT

def getRDSConn():
    secrets_dict = getSecrets()

    #signs in to the database that holds the tables used to metrics storage and establishes the connection cursor
    metrics_database = secrets_dict.get('$$$$')
    metrics_user = secrets_dict.get('$$$$')
    metrics_password = secrets_dict.get('$$$$')
    metrics_host = secrets_dict.get('$$$$')
    metrics_port = secrets_dict.get('$$$$')    
    conn = psycopg2.connect(database=metrics_database,
                            user=metrics_user,
                            password=metrics_password,
                            host=metrics_host,
                            port=metrics_port) 
    return conn

# Get all job logs from a given directory path
def getDirJobLog(jobDir):
    logs = [entry for entry in scandir(jobDir) if entry.is_file() and
        re.search(r'ags_MapExport_.*\.log', entry.name)]
    return logs

# Extract the Job GUID out of the file
def getIdentifiers(fd):
    guid = None
    processId = None
    with open(fd.path, 'r') as fp:
        lines = fp.readlines()
        for line in lines:
            if "process_id" in line:
                match = re.search(r'process_id : (?P<process_id>.+).', line)
                processId = match.group("process_id") if match is not None else None
            if "map_id" in line:
                match = re.search(r'map_id : (?P<guid>.+).', line)
                guid = match.group("guid") if match is not None else None
            if processId is not None and guid is not None:
                break # We have both identifiers, now we can stop
    print("Proceess ID: {} Map ID: {}".format(processId, guid))
    return (processId, guid)

def getAPIURL(gateway_id, machine_type, processId, guid):
    gateway_url = "https://{id}.execute-api.us-west-2.amazonaws.com/dynamap-$$$$-$$$$/api/v1/mapgen/{env}/{log_id}".format(
        id=gateway_id,
        env=machine_type,
        log_id="{}_{}".format(processId, guid)
    )
    return gateway_url

def copyLogToStorage(gateway_id, machine_type, processId, guid, logFileEntry):
    gateway_url = getAPIURL(gateway_id, machine_type, processId, guid)

    with open(logFileEntry.path, "r") as log:
        data = log.read()

        print("Uploading: {}".format(logFileEntry.path))
        request = Request(gateway_url)
        request.get_method = lambda: "PUT"
        request.data = data.encode()

        resp = urlopen(request)
        print("Response Code: {} Response Body: {}".format(resp.code, resp.read()))
        resp.close()
    pass

def getAPIGatewayID(secrets):
    return secrets[METRICS_GATEWAY_ID_SECRET]

def isRemoteLogUpToDate(gateway_id, machine_type, processId, guid, fileHandle):
    local_checksum = hashlib.md5(open(fileHandle.path, 'rb').read()).hexdigest()

    # Making head call to gateway to get file info
    gateway_url = "https://{id}.execute-api.us-west-2.amazonaws.com/dynamap-$$$$-$$$$/api/v1/mapgen/{env}/{log_id}".format(
        id=gateway_id,
        env=machine_type,
        log_id="{}_{}".format(processId, guid)
    )
    request = Request(gateway_url)
    request.get_method = lambda: "HEAD"

    try:
        response = urlopen(request)
        remote_checksum = response.info()["ETag"]
        response.close()

        upToDate = local_checksum in remote_checksum
        print("Up-To-Date: {} Local: {} Remote: {}".format(upToDate, local_checksum, remote_checksum))
        return upToDate
    except HTTPError as e:
        if e.code != 404:
            print(e.code)
            print(e.reason)
        return False
    pass

def collect_job_dev_logs():
    print("Collecting mapgen logs")

    print("Get machine type")
    machine_type = getMachineType()
    print("Machine type: {}".format(machine_type))
    if machine_type is None:
        print("No valid machine type, aborting")
        return
    
    secrets = getSecrets()

    print("Getting Api Gateway ID")
    api_gateway_id = getAPIGatewayID(secrets)
    if api_gateway_id is None:
        print("Gateway not found")

    JOB_DIR_PATH = "C:/arcgisserver/directories/arcgisoutput"
    # Scan the JOB_DIR_PATH for job directories and find those that have job logs
    jobDirs = [entry.path for entry in scandir(JOB_DIR_PATH) if entry.is_dir() and "_ags_" in entry.name]
    dirsWithLogs = [entry for entry in jobDirs if len(getDirJobLog(entry)) > 0]

    # Collect _ags_MapExport_DevRun.logs into array
    jobLogs = [ logFileHandle for entry in dirsWithLogs for logFileHandle in getDirJobLog(entry) ]

    print("Jobs logs count: {}".format(len(jobLogs)))
    # If we have no job logs, abort early
    if len(jobLogs) <= 0: return

    for fileHandle in jobLogs:
        processId, guid = getIdentifiers(fileHandle)
        
        # Verify Local vs Remote checksum
        isUpToDate = isRemoteLogUpToDate(api_gateway_id, machine_type, processId, guid, fileHandle)
        print("{} {} is up-to-date? {}".format(processId, guid, isUpToDate))
        if not isUpToDate:
            copyLogToStorage(api_gateway_id, machine_type, processId, guid, fileHandle)
    
    print("Mapgen log collection complete")
    pass

def collect_metrics():
    print("Collecting Metrics")
    hostname = socket.gethostname()
    local_ip = socket.gethostbyname(hostname)

    run_dict = {}
    run_dict['local_ip'] = local_ip

    #cpu percent returns system wide cpu usage as an average over the input interval. current setting is 4 seconds.
    run_dict['cpu_percent'] = psutil.cpu_percent(4)

    #the function virtual memory returns a list, the 3rd of which is a percent. 
    run_dict['ram_percent'] = psutil.virtual_memory()[2]

    partitions = psutil.disk_partitions()

    for p in partitions:
        usage = psutil.disk_usage(p.mountpoint)
        mountpoint_str = p.mountpoint[0].lower()
        total_key = mountpoint_str + '_total'
        used_key = mountpoint_str + '_used'
        free_key = mountpoint_str + '_free'
        run_dict[total_key] = usage.total / 2**30
        run_dict[used_key] = usage.used / 2**30
        run_dict[free_key]= usage.free / 2**30

    time = datetime.now()

    run_dict['time'] = time

    EC2_RESOURCE = boto3.resource('ec2', region_name=REGION_NAME)

    instances = EC2_RESOURCE.instances.filter(
        Filters=[
            {
                'Name': 'tag:Hostname',
                'Values': [
                    hostname
                ]
            }
        ]
    )
    for instance in instances:
        run_dict['instance_id'] = instance.id

    conn = getRDSConn()
    cur = conn.cursor()

    sql = "insert into t_metrics_monitor_per_machine (d_used, c_total, d_total, ram_percent, cpu_percent, local_ip, c_used, d_free, c_free, time, instance_id, hostname) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    data = (run_dict.get('d_used', 0), run_dict.get('c_total',0), run_dict.get('d_total',0), run_dict.get('ram_percent',0),
            run_dict.get('cpu_percent',0), run_dict.get('local_ip','na'), run_dict.get('c_used',0), run_dict.get('d_free',0),
            run_dict.get('c_free',0), run_dict.get('time', None), run_dict.get('instance_id','na'), hostname)

    cur.execute(sql, data)
    conn.commit()
    conn.close()
    print("Metrics collection complete")

def _timescale_hardware(cur):
    # CPU Utilized %
    worker_cpu_utilized_percent = psutil.cpu_percent(4) # Avg over 4 second interval
    insert_timescale(cur, "worker_cpu_utilized_percent", worker_cpu_utilized_percent)

    # RAM Utilized %
    worker_ram_utilized_percent = psutil.virtual_memory().percent
    insert_timescale(cur, "worker_ram_utilized_percent", worker_ram_utilized_percent)

    # Swap Utilized %
    worker_swap_utilized_percent = psutil.swap_memory().percent
    insert_timescale(cur, "worker_swap_utilized_percent", worker_swap_utilized_percent)

    # Disk Drive Utilized %
    worker_disk_c_utilized_percent = None
    worker_disk_d_utilized_percent = None

    partitions = psutil.disk_partitions()
    for p in partitions:
        usage = psutil.disk_usage(p.mountpoint)
        mountpoint_str = p.mountpoint[0].lower()
        if mountpoint_str == "c":
            worker_disk_c_utilized_percent = usage.percent
            insert_timescale(cur, "worker_disk_c_utilized_percent", worker_disk_c_utilized_percent)
        elif mountpoint_str == "d":
            worker_disk_d_utilized_percent = usage.percent
            insert_timescale(cur, "worker_disk_d_utilized_percent", worker_disk_d_utilized_percent)
    return

def _timescale_dns_server(cur):
    import time
    from threading import Thread
    try:
        import dns.resolver
        import dns.query
        import dns.rdatatype
        print('dnspython imported')
    except:
        print('dnspython not found. Attempting runtime install/import')
        try:
            import subprocess
            subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'dnspython'])
            import dns.resolver
            import dns.query
            import dns.rdatatype
            print('dnspython imported')
        except:
            sys.stderr.write("Failed to import dnspython\n")
            return
    
    DNS_RESOLVER_TIMEOUT = 30 # 30 seconds
    try:
        print('Start DNS resolve latency check')
        resolver = dns.resolver.Resolver()
        ns = resolver.nameservers # List of nameserver ips

        results = { server : None for server in ns } # IP Addr -> response time
        
        threads = [ Thread(target=async_dns_time, args=(server, DNS_RESOLVER_CHECK_HOSTNAME, DNS_RESOLVER_TIMEOUT, results)) for server in ns ]
        [t.start() for t in threads] # Start all the threads
        for t in threads: # Wait the threads
            t.join()

        for server, respTime in results.items():
            insert_dns_timescale(cur, server, respTime)
    except Exception as e:
        # Encountered some unknown issue
        print(e)
        return
    
    print('Completed DNS resolve latency check')
    return        

def async_dns_time(server, name, timeout, results):
    import dns.message
    import dns.query
    import dns.rdatatype
    import time

    n = dns.name.from_text(name) # Creates a DNS Name obj to query for
    q = dns.message.make_query(n, dns.rdatatype.NS) # Convert into proper query
    
    # Query
    latency_seconds = -1
    try:
        q_start = time.time()
        dns.query.udp(q, where=server, timeout=timeout)
        q_end = time.time()
        latency_seconds = q_end - q_start
        latency_seconds = round(latency_seconds, 3)
    except:
        latency_seconds = timeout
    
    results[server] = latency_seconds

    return results

def insert_dns_timescale(cur, server, respTime):
    import time
    table = 'worker_dns_latency_seconds'
    os_time = time.time() - psutil.boot_time()
    sql = "INSERT INTO {table} (time, hostname, instance_id, environment, nameserver, os_time, value) \
            VALUES (NOW(), %(hostname)s, %(instance_id)s, %(environment)s, %(server)s, %(os_time)s, %(value)s)"
    
    sql = sql.format(table=table)

    data = {
        "hostname" : MY_HOSTNAME,
        "instance_id" : MY_INSTANCE_ID,
        "environment" : MY_ENVIRONMENT,
        "server" : server,
        "os_time" : os_time,
        "value" : respTime
    }

    cur.execute(sql, data)

def collect_timescale_metrics():
    secrets = getSecrets()

    # Connect to Timescale instance
    metrics_database = secrets.get('$$$$')
    metrics_user = secrets.get('$$$$')
    metrics_password = secrets.get('$$$$')
    metrics_host = secrets.get('$$$$')
    metrics_port = secrets.get('$$$$')    
    conn = psycopg2.connect(database=metrics_database,
                            user=metrics_user,
                            password=metrics_password,
                            host=metrics_host,
                            port=metrics_port) 
    
    cur = conn.cursor()

    with conn:
        with conn.cursor() as cur:
            _timescale_hardware(cur)
    with conn:
        with conn.cursor() as cur:
            _timescale_dns_server(cur)

    # Finalize
    conn.commit()
    conn.close()
    return
    
def insert_timescale(cur, table, value):
    sql = "INSERT INTO {table} (time, hostname, instance_id, environment, value) \
            VALUES (NOW(), %(hostname)s, %(instance_id)s, %(environment)s, %(value)s)"
    
    sql = sql.format(table=table)

    data = {
        "hostname" : MY_HOSTNAME,
        "instance_id" : MY_INSTANCE_ID,
        "environment" : MY_ENVIRONMENT,
        "value" : value
    }

    cur.execute(sql, data)

def main():
    try:
        collect_metrics()
    except Exception:
        print("Collect metrics failed")
        print(traceback.format_exc())
    
    try:
        collect_timescale_metrics()
    except Exception:
        print("Collect timescale metrics failed")
        print(traceback.format_exc())
    
    try:
        collect_job_dev_logs()
    except Exception:
        print("Collect job logs failed")
        print(traceback.format_exc())
