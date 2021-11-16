from configparser import ConfigParser

config = ConfigParser()

config["review"] = {
    "src_host": "local",
    "src_type": "ff",
    "src_url": "C:\\Users\\aneesh\\source\\",
    "dest_host": "local",
    "dest_type": "ff",
    "dest_url": "C:\\Users\\aneesh\\"
}

config["test"] = {
    "src_host": "local",
    "src_type": "ff",
    "src_url": "C:\\Users\\aneesh\\",
    "dest_host": "local",
    "dest_type": "ff",
    "dest_url": "C:\\Users\\aneesh\\dest\\"
}

config["puppy"] = {
    "src_host": "cloud-s3",
    "src_type": "ff",
    "src_url": "S3://source-bucket/",
    "src_aws_access_key_id": 'AKIA46SFIWN5AMWMDQVB',
    "src_aws_secret_access_key": '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',
    "src_region_name": 'ap-south-1',
    "dest_host": "cloud-s3",
    "dest_type": "ff",
    "dest_url": "S3://dest-bucket/",
    "dest_aws_access_key_id": 'AKIA46SFIWN5AMWMDQVB',
    "dest_aws_secret_access_key": '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',
    "dest_region_name": 'ap-south-1'
}


with open("..\\configuration\\eng\\tables.ini", "w") as f:
    config.write(f)