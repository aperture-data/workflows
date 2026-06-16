import os
import h5py
import numpy as np


def write_results_to_file(results, engine, index_name):

    top_k = len(results[0])
    f = open(f"output/results_{engine}_{index_name}_top{top_k}.txt", "w")
    for i in range(len(results)):
        for j in range(len(results[i])):
            str_id = results[i][j]
            f.write(f"{str_id:>8}")
        f.write(f"\n")
    f.close()


def print_percentiles(times, percentiles=None):

    if percentiles is None:
        percentiles = [10, 50, 90, 95, 99]

    for p in percentiles:
        print(f"    Percentile {p}: {np.percentile(np.array(times), p)}")
    print(f"    Average time:  {np.mean(times)}")


def create_connector_pinecone(grpc=False):
    """Create Pinecone connector with dynamic import."""
    api_key = os.environ.get("PINECONE_API_KEY", "")

    if api_key is None:
        raise Exception("API_KEY_PINECONE not set")

    if grpc:
        from pinecone import PineconeGRPC as Pinecone
        pc = Pinecone(api_key=api_key)
    else:
        from pinecone import Pinecone
        pc = Pinecone(api_key=api_key)

    return pc


def create_connector_weaviate():
    """Create Weaviate connector with dynamic import."""
    import weaviate
    from weaviate.classes.init import Auth

    cluster_url = os.environ.get("WEAVIATE_CLUSTER_URL", "")
    api_key     = os.environ.get("WEAVIATE_API_KEY", "")

    if cluster_url is None:
        raise Exception("WEAVIATE_CLUSTER_URL not set")

    if api_key is None:
        raise Exception("WEAVIATE_API_KEY not set")

    client = weaviate.connect_to_weaviate_cloud(
        cluster_url=cluster_url,
        auth_credentials=Auth.api_key(api_key),
    )

    return client


def create_connector_qdrant():
    """Create Qdrant connector with dynamic import."""
    from qdrant_client import QdrantClient

    cluster_url = os.environ.get("QDRANT_CLUSTER_URL", "")
    api_key     = os.environ.get("QDRANT_API_KEY", "")

    if cluster_url is None:
        raise Exception("QDRANT_URL not set")

    if api_key is None:
        raise Exception("QDRANT_API_KEY not set")

    client = QdrantClient(url=cluster_url, api_key=api_key)

    return client


def create_connector_lancedb():
    """Create LanceDB connector with dynamic import."""
    import lancedb

    # Use LanceDB Cloud with API key
    lancedb_key = os.environ.get('LANCEDB_API_KEY')
    if not lancedb_key:
        raise ValueError(
            "LANCEDB_API_KEY environment variable is required for cloud connection")

    # Get database URI from environment or use default cloud format
    db_uri = os.environ.get('LANCEDB_URI', 'db://default')

    db = lancedb.connect(db_uri, api_key=lancedb_key)

    return db


def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise Exception('Boolean value expected.')


class Dataset():

    def __init__(self, max=None):

        if max is not None:
            self.max = max

        self.len = self.max

    def getitem(self, idx):
        if idx < 0 or self.len <= idx:
            return None

        # Return the ith descriptor
        return self.dataset[idx]

    def __len__(self):
        return self.len


class DatasetDeepImage96(Dataset):

    def __init__(self, max=None):

        self.name = "deep-image-96-angular"
        self.path = f"input/{self.name}.hdf5"
        self.dim  = 96
        self.max  = 9_990_000

        if max is not None and max < self.max:
            self.max = max

        # print(f"Loading {self.path} ...")
        with h5py.File(self.path, "r") as f:

            self.distances = f["distances"][()]
            self.neighbors = f["neighbors"][()]
            self.test      = f["test"][()]
            self.dataset   = f["train"][()]

        # print(f"Done.")

        if max is not None and max < len(self.dataset):
            self.dataset = self.dataset[:max]

        self.len = len(self.dataset)

        assert (len(self.dataset) == self.len)
        assert (len(self.dataset[0]) == self.dim)


class DatasetYFCC100M(Dataset):

    def __init__(self, max=None):

        self.name = "yfcc100m"
        self.path = "input/YFCC100M_hybridCNN_gmean_fc6_0.bin"
        self.dim  = 4096
        self.max  = 1_000_000

        if max is not None and max < self.max:
            self.max = max

        self.len = self.max

        # print(f"Loading {self.path} ...")
        dtype = [('id', np.int64), ('descs', np.float32, (self.dim))]
        with open(self.path, 'rb') as fh:
            self.dataset = np.fromfile(fh, dtype, count=self.len)["descs"]
        # print(f"Done.")

        assert (len(self.dataset) == self.len)
        assert (len(self.dataset[0]) == self.dim)
