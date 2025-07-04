import logging
from aperturedb.Sources import Sources

logger = logging.getLogger(__name__)

class Provider:
    def __init__(self,bucket):
        self.bucket = bucket
    def bucket_name(self):
        return self.bucket
    def url_column_name(self):
        raise NotImplementedError("Base Class")
    def url_scheme_name(self):
        raise NotImplementedError("Base Class")
    def verify(self):
        raise NotImplementedError("Base Class")
    def scan(self,filter_func):
        raise NotImplementedError("Base Class")

class GCSProvider(Provider):
    def __init__(self,bucket,gcs_key):
        super(GCSProvider,self).__init__(bucket)
        from google.cloud import storage
        self.storage = storage
        self.credentials = gcs_key
    def url_column_name(self):
        return "gs_url"
    def url_scheme_name(self):
        return "gs:"
    def verify(self):
        # creation must be in verify as we expect init to work.
        try:
            import json
            from google.oauth2 import service_account

            account_json = json.loads(self.credentials)
            creds = service_account.Credentials.from_service_account_info(account_json)
            self.client = self.storage.Client( credentials=creds)
        except Exception as e:
            logger.error(f"Failed to create client for gs: {e}")
            return False
        try:
            r=self.client.list_buckets(max_results=10)
        except Exception as e:
            logger.error(f"Failed to read buckets for gs: {e}")
            return False

        try:
            r=self.client.lookup_bucket(self.bucket)
        except Exception as e:
            logger.error(f"Could not get bucket info ({self.bucket}) for gs: {e}")
            return False
        return True

    def scan(self,filter_func):
        load_count = 0
        paths = []

        blobs = self.client.list_blobs(self.bucket) 
        for blob in blobs:
            if filter_func(blob.name):
                load_count = load_count + 1
                paths.append(blob.name)

        return paths

class AWSProvider(Provider):
    def __init__(self,bucket,access_key,secret_key):
        super(AWSProvider,self).__init__(bucket)
        import boto3
        self.client = \
        boto3.client('s3',aws_access_key_id=access_key,aws_secret_access_key=secret_key)
        # store this here because some things might be picky about region.
        self.region_name = self.client.meta.region_name
    def url_column_name(self):
        return "s3_url"
    def url_scheme_name(self):
        return "s3:"
    def verify(self):

        # Verify Account
        try:
            self.client.list_buckets(MaxBuckets=10)
        except Exception as e:
            #InvalidAccessKey - bad AK (short/wrong)
            #SignatureDoesNotMatch - bad SK
            logger.error(f"Failed to list buckets for s3: {e}")
            return False

        # Verify Bucket

        try:
            res = self.client.head_bucket(Bucket=self.bucket)
        except Exception as e:
            # 404 not found if bad bucket name
            logger.error(f"Could not get bucket info ({self.bucket}) for s3: {e}")
            return False
        return True

    def scan(self,filter_func):

        load_count = 0
        paths = []

        object_paginator = self.client.get_paginator('list_objects_v2')
        bucket_iter = object_paginator.paginate(Bucket=self.bucket)
        for page in bucket_iter:
            for obj in page['Contents']:
                name = obj['Key']
                if filter_func(name):
                    load_count = load_count + 1
                    paths.append(name)

        return paths



# potential errors to pay attention to:
# bad access key
# bad secret key
# expired key
# bad bucket name
# no access to bucket

class CustomSources(Sources):
    def __init__(self, source_provider:Provider):
        super().__init__(3)
        self.gs_client = None
        if isinstance(source_provider,AWSProvider):
            self.s3 = source_provider.client
        else:
            self.gs_client = source_provider.client
    def load_object(self,path):
        if self.gs_client:
            return self.load_from_gs_url(path,lambda blob:True )[1]
        else:
            return self.load_from_s3_url(path,lambda blob:True )[1]
    def load_from_gs_url(self,gs_url,validator):
        import numpy as np

        retries = 0
        while True:
            try:
                bucket_name = gs_url.split("/")[2]
                object_name = gs_url.split("gs://" + bucket_name + "/")[-1]

                blob = self.gs_client.bucket(bucket_name).blob(
                    object_name).download_as_bytes()
                imgbuffer = np.frombuffer(blob, dtype='uint8')
                if not validator(imgbuffer):
                    logger.warning(f"VALIDATION ERROR: {gs_url}")
                    return False, None
                return True, blob
            except:
                if retries >= self.n_download_retries:
                    break
                logger.warning("Retrying object: {gs_url}", exc_info=True)
                retries += 1
                time.sleep(2)

        logger.error(f"GS ERROR: {gs_url}")
        return False, None

