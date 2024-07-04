import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import pyarrow as pa
import pyarrow.parquet as pq

# Constants
num_rows = 1000
domains = ["navya.com", "pentest.org", "dev.net", "anand.com", "navyait.edu"]
types = [1, 2, 5, 6, 10, 15]

# Timestamp Generation
def random_timestamp(start, end):
    return int((start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))).timestamp())

# Generating a dummy dataset as I could'nt find a similar dataset according to my needs
data = {
    "timestamp": [random_timestamp(datetime(2023, 1, 1), datetime(2024, 1, 1)) for _ in range(num_rows)],
    "qname": [f"sub{random.randint(1, 100)}.{random.choice(domains)}" for _ in range(num_rows)],
    "domain": [random.choice(domains) for _ in range(num_rows)],
    "rcode": [random.randint(0, 5) for _ in range(num_rows)],
    "ancount": [random.randint(0, 5) for _ in range(num_rows)],
    "nscount": [random.randint(0, 5) for _ in range(num_rows)],
    "rrr1": [
        [
            {
                "name": f"sub{random.randint(1, 100)}.{random.choice(domains)}",
                "ttl": random.randint(60, 86400),
                "type": random.choice(types),
                "clas": 1,
                "data": f"data{random.randint(1, 100)}"
            }
        ] for _ in range(num_rows)
    ],
    "rrr2": [
        [
            {
                "name": f"sub{random.randint(1, 100)}.{random.choice(domains)}",
                "ttl": random.randint(60, 86400),
                "type": random.choice(types),
                "clas": 1,
                "data": f"data{random.randint(1, 100)}"
            }
        ] for _ in range(num_rows)
    ]
}

# Dataframe conversion
df = pd.DataFrame(data)

# DataFrame to pyarrow Table
table = pa.Table.from_pandas(df)

# writing it in parquet file as required
pq.write_table(table, "data/raw/dummy_dns_data.parquet")