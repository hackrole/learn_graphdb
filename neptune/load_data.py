import pickle
import gzip
import glob
from itertools import islice
from functools import reduce
from typing import Any, List, Dict

import click
import pandas as pd
from tqdm import tqdm
from py2neo import Graph
from py2neo.bulk import merge_nodes, merge_relationships

"""
node example:
 '3S4V-JW20-003B-354J-00000-00': {
   'title': 'Rodriguez v. East Texas Motor Freight, 1973 U.S. Dist. LEXIS 14388',
  'court_name': 'United States District Court for the Western District of Texas, San Antonio Division.',
  'jurisinfo_system_code': 'US',
  'decision_date': '1973-03-22'},

edge example
 '3V7S-2480-0039-454M-00000-00': [{'dest_lni': '3WXN-X150-00KR-C2K5-00000-00',
   'is_in_headnote': False,
   'is_in_footnote': False,
   'is_in_overview': False,
   'is_in_rfc': True,
   'is_in_opinion': True,
   'count': 1},
"""


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


def t_edge():
    fname = "./part_1_graph_cite.pkl"
    edges = pickle_load(fname)
    return edges

def t_node():
    fname = "./part_1_graph_metadata.pkl"
    nodes = pickle_load(fname)
    return nodes


def cat_it(data):
    print(len(data), type(data))
    a = dict(islice(data.items(), 10))
    return a


def to_node_csv(fnames: List[str], result_fname: str = "result.csv"):
    def get_nodes():
        for fname in fnames:
            nodes = pickle_load(fname)
            for key, value in nodes.items():
                yield {
                    ":ID": key,
                    ":LABEL": "Case",
                    "title:String": value["title"],
                    "court_name:String": value["court_name"],
                    "jurisinfo_system_code:String": value["jurisinfo_system_code"],
                    "decision_date:DateTime": value["decision_date"],
                }

    nodes = get_nodes()
    return to_csv(nodes, result_fname)


def to_edge_csv(fnames: List[str], result_fname: str = "edges.csv"):
    def get_edges():
        for fname in fnames:
            edges = pickle_load(fname)
            for key, value in edges.items():
                for item in value:
                    yield {
                        ":ID": f"{key}-{item['dest_lni']}",
                        ":START_ID": key,
                        ":END_ID": item["dest_lni"],
                        ":TYPE": "REF",
                        "is_in_headnote:Bool": item["is_in_headnote"],
                        "is_in_overview:Bool": item["is_in_overview"],
                        "is_in_footnote:Bool": item["is_in_footnote"],
                        "is_in_rfc:Bool": item["is_in_rfc"],
                        "is_in_opinion:Bool": item["is_in_opinion"],
                    }

    edges = get_edges()
    return to_csv(edges, result_fname)

def to_csv(data, output):
    df = pd.DataFrame(data)
    df.to_csv(output, index=False)


def main(basedir: str):

    node_fs = glob(f"{basedir}/part_*_metadata.*")
    to_node_csv(node_fs, "nodes.csv")


    edge_fs = glob(f"{basedir}/part_*_cite.*")
    to_edge_csv(edge_fs, "edges.csv")
