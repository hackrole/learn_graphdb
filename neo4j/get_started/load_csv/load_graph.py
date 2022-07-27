import pickle
import gzip
from itertools import islice
from functools import reduce
from typing import Any, List, Dict

import click
import pandas as pd
from tqdm import tqdm
from py2neo import Graph
from py2neo.bulk import merge_nodes, merge_relationships


def read_gzip(fname: str) -> bytes:
    with gzip.open(fname, 'rb') as f:
        return f.read()


def pickle_load(fname: str) -> Any:
    if fname.endswith(".gz"):
        content = read_gzip(fname)
        data = pickle.loads(content)
        return data
    else:
        content = pickle.load(open(fname, "rb"))
        return content


def get_neo4j_conn(uri: str, user: str, password: str) -> Graph:
    return Graph(uri, auth=(user, password))

def get_value_by_key(data, key, handler = None):
    value = data.get(key)
    if handler:
        value = handler(value)
    return (key, value)


def merge_ref(g: Graph, fname: str):
    data = pickle_load(fname)

    def iter_data():
        for key, value in tqdm(data.items()):
            for item in value:
                if 'dest_lni' not in item:
                    print(f"dest_lni not found in {item}")
                    continue

                dest_lni = item["dest_lni"]

                keys = [("is_in_headnote", bool),
                        ("is_in_footnote", bool),
                        ("is_in_overview", bool),
                        ("is_in_rfc", bool),
                        ("is_in_opintion", bool),
                        ("count", int)]
                property = dict(get_value_by_key(item, k, h) for k, h in keys)

                yield (key, property, dest_lni)

    lg = reduce(lambda x, y: x + 1, iter_data(), 0)
    it = iter_data()
    page_size = 1000
    while True:
        for _i in tqdm(range(lg // page_size)):
            chunks = list(islice(it, page_size))
            if not chunks:
                return

            merge_relationships(g.auto(), chunks, "REF", start_node_key=("Paper", "ID"), end_node_key=("Paper", "ID"))


def create_csv(fname: str):
    output = fname + ".csv"

    data = pickle_load(fname)

    def iter_data():
        for key, value in data.items():
            yield {**value, "ID": key, "title": str(value["title"])}

    df = pd.DataFrame(iter_data())
    df.to_csv(output, index=False)
    return output


def merge_paper_nodes(g: Graph, fname: str):
    data = pickle_load(fname)
    lg = len(data)
    print(f"data length: {lg}")

    def iter_data():
        for key, value in data.items():
            yield {**value, "ID": key, "title": str(value["title"])}

    count = 0
    # for key, value in islice(data.items(), 200):
    #     for i, ii in value.items():
    #         if ii is None:
    #             continue
    #         if type(ii) != str and not isinstance(ii, int):
    #             print(i, type(ii), ii)
    # return
    #     # v = {**value, "ID": key}
    #     # try:
    #     #     merge_nodes(g.auto(), [v], ("test", "ID"))
    #     #     count += 1
    #     # except Exception as ex:
    #     #     import pdb;pdb.set_trace()
    #     #     print(count, v)

    it = iter_data()
    page_size = 1000
    while True:
        for i in tqdm(range(lg // page_size)):
            chunks = list(islice(it, page_size))
            if not chunks:
                return

            merge_nodes(g.auto(), chunks, ("Paper", "ID"))


@click.command()
@click.option("--uri", default="bolt://127.0.0.1:7687")
@click.option("--user", default="neo4j")
@click.option("--pwd", default="test")
@click.option("--fname")
def cli(uri, user, pwd, fname):
    # import pdb;pdb.set_trace()
    neo_conn = get_neo4j_conn(uri, user, pwd)

    # create_csv(fname)
    # merge_paper_nodes(neo_conn, fname)
    merge_ref(neo_conn, fname)


if __name__ == "__main__":
    cli()
