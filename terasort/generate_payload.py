import logging
from math import floor
import random
import json
import re
import argparse
import boto3
import pandas as pd
import numpy as np
from pprint import pprint
from io import BytesIO


logger = logging.getLogger(__name__)


DEFAULT_MAX_SAMPLE_SIZE: int = 1 * 1024 * 1024
DEFAULT_SAMPLE_RATIO: float = 0.01
DEFAULT_SAMPLE_FRAGMENTS: int = 20
DEFAULT_START_MARGIN: float = 0.02
DEFAULT_END_MARGIN: float = 0.02
DEFUALT_BOUND_EXTRACTION_MARGIN: int = 1024 * 1024
DEFAULT_PAYLOAD_FILENAME = "sort_payload"


def main():
    parser = argparse.ArgumentParser(description="Generate Sort input payload")
    parser.add_argument("--partitions", type=int, required=True, help="Number of partitions")
    parser.add_argument("--bucket", type=str, required=True, help="Bucket name")
    parser.add_argument("--key", type=str, required=True, help="Object key")
    parser.add_argument("--sort-column", type=int, required=True, help="Sort key")
    parser.add_argument("--delimiter", type=str, default=",", help="Delimiter")
    parser.add_argument("--start-margin", type=float, default=DEFAULT_START_MARGIN, help="Start margin")
    parser.add_argument("--end-margin", type=float, default=DEFAULT_END_MARGIN, help="End margin")
    parser.add_argument("--sample-ratio", type=float, default=DEFAULT_SAMPLE_RATIO, help="Sample ratio")
    parser.add_argument("--sample-fragments", type=int, default=DEFAULT_SAMPLE_FRAGMENTS, help="Sample fragments")
    parser.add_argument("--max-sample-size", type=int, default=DEFAULT_MAX_SAMPLE_SIZE, help="Max sample size")
    parser.add_argument(
        "--bound-extraction-margin", type=int, default=DEFUALT_BOUND_EXTRACTION_MARGIN, help="Bound extraction margin"
    )
    parser.add_argument("--seed", type=int, default=None, help="Random seed")
    parser.add_argument("--payload-filename", type=str, default=DEFAULT_PAYLOAD_FILENAME, help="Payload filename")
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    s3_client = boto3.client("s3", endpoint_url="http://localhost:9000")
    obj_size = s3_client.head_object(Bucket=args.bucket, Key=args.key)["ContentLength"]

    # Avoid dataset head and tail
    start_limit = int(obj_size * DEFAULT_START_MARGIN)
    end_limit = int(obj_size * (1 - DEFAULT_END_MARGIN))
    choosable_size = end_limit - start_limit

    # Size of each sampled fragment
    fragment_size = floor(
        min(floor((end_limit - start_limit) * DEFAULT_SAMPLE_RATIO), DEFAULT_MAX_SAMPLE_SIZE)
        / DEFAULT_SAMPLE_FRAGMENTS
    )

    # Select bounds randomly
    num_parts = int(choosable_size / fragment_size)
    selected_fragments = sorted(random.sample(range(num_parts), DEFAULT_SAMPLE_FRAGMENTS))

    keys_arrays = []

    # Read from each bound a fragment size, adjusting limits
    for f in selected_fragments:
        lower_bound = start_limit + f * fragment_size
        upper_bound = lower_bound + fragment_size

        range_0 = max(0, lower_bound - args.bound_extraction_margin)
        range_1 = min(obj_size, upper_bound + args.bound_extraction_margin)

        body = s3_client.get_object(
            Bucket=args.bucket,
            Key=args.key,
            Range=f"bytes={range_0}-{range_1}",
        )["Body"].read()

        body_sz = len(body)
        start_byte = lower_bound - range_0
        end_byte = upper_bound - range_1
        if start_byte > 0:
            lower_bound = start_byte

            while lower_bound > 0:
                if body[lower_bound : lower_bound + 1] == b"\n":
                    lower_bound += 1
                    break
                else:
                    lower_bound -= 1
        else:
            lower_bound = 0

        if end_byte < body_sz:
            upper_bound = end_byte

            while upper_bound < body_sz:
                if body[upper_bound : upper_bound + 1] == b"\n":
                    break
                else:
                    upper_bound += 1
        else:
            upper_bound = end_byte

        body_memview = memoryview(body)
        partition = body_memview[lower_bound:upper_bound]

        df = pd.read_csv(
            BytesIO(partition),
            engine="c",
            index_col=None,
            header=None,
            delimiter=args.delimiter,
            quoting=3,
            on_bad_lines="warn",
        )

        # print(df)

        keys_arrays.append(np.array(df[args.sort_column]))

    # Concat keys, sort them
    keys = np.concatenate(keys_arrays)
    keys.sort()

    # Find quantiles (num tasks)
    quantiles = [i * 1 / args.partitions for i in range(1, args.partitions)]
    segment_bounds = [keys[int(q * len(keys))] for q in quantiles]

    pprint(segment_bounds)

    # Write parameters as JSON file
    params = [
        {
            "bucket": args.bucket,
            "key": args.key,
            "obj_size": obj_size,
            "sort_column": args.sort_column,
            "delimiter": args.delimiter,
            "partitions": args.partitions,
            "partition_idx": i,
            "segment_bounds": segment_bounds,
        }
        for i in range(args.partitions)
    ]

    with open(f"{args.payload_filename}.json", "w") as f:
        json.dump(params, f, indent=4)


if __name__ == "__main__":
    main()
