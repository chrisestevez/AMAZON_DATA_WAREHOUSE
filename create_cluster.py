import configparser
import boto3
from botocore.exceptions import ClientError

# https://github.com/ulmefors/udacity-nd027-data-warehouse/blob/master/create_cluster.py
# https://github.com/AbdullahMu/Cloud-Data-Warehouse-with-AWS/blob/master/RedShift_Test_Cluster.ipynb

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

class CreateCluster:
    

    
    def __init__(self):
        self.key = config['AWS']['KEY']
        self.secret = config['AWS']['SECRET']
        
        self.dwh_cluster_type =config['CLUSTER']['DWH_CLUSTER_TYPE']
        self.dwh_cluster_num_nodes =config['CLUSTER']['DWH_NUM_NODES']
        self.dwh_cluster_node_type =config['CLUSTER']['DWH_NODE_TYPE']
        self.dwh_cluster_id = config['CLUSTER']['DWH_CLUSTER_IDENTIFIER']
        self.dwh_cluster_iam_role_name = config['CLUSTER']['DWH_IAM_ROLE_NAME']
        self.dwh_cluster_region = config['CLUSTER']['REGION']
        
        self.db_host = config['DB']['HOST']
        self.db_name = config['DB']['DB_NAME']
        self.db_user = config['DB']['DB_USER']
        self.db_password = config['DB']['DB_PASSWORD']
        self.db_port = config['DB']['DB_PORT']
        
        self.iam_arn = config['IAM_ROLE']['ARN']
        self.iam_read_s3 = config['IAM_ROLE']['READ_S3_ARN']

    def create_roles(self):
        pass


# [S3]
# LOG_DATA='s3://udacity-dend/log_data'
# LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
# SONG_DATA='s3://udacity-dend/song_data'   
    def main(self):
        print(self.key)  
         


# DB_PORT = config['DB']['DB_PORT']

CreateCluster().main()