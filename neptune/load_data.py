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


def to_id_node_csv(fnames: List[str], result_fname: str = "easy_result.csv"):
    def get_nodes():
        for fname in fnames:
            nodes = pickle_load(fname)
            for key, value in nodes.items():
                yield {
                    ":ID": key,
                    ":LABEL": "Case",
                }

    nodes = get_nodes()
    return to_csv(nodes, result_fname)



def to_id_edge_csv(fname: List[str], result_fname: str = "easy_edges.csv"):
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
                    }

    edges = get_edges()
    return to_csv(edges, result_fname)

def to_csv(data, output):
    df = pd.DataFrame(data)
    df.to_csv(output, index=False)


def main(basedir: str):

    node_fs = glob.glob(f"{basedir}/part_*_metadata.*")
    # to_node_csv(node_fs, "nodes.csv")
    to_id_node_csv(node_fs)


    edge_fs = glob.glob(f"{basedir}/part_*_cite.*")
    # to_edge_csv(edge_fs, "edges.csv")
    to_id_edge_csv(edge_fs)


"""
load data in neptune notebook with curl

# copy result csv to s3
aws s3 cp ./nodes.csv s3://<bucket_name>/
aws s3 cp ./edges.csv s3://<bucket_name>/

# load data in neptune notebook
%%bash
curl -X POST \
       -H 'Content-Type: application/json' \
      https://<neptune_endpoint>:8182/loader -d '
      {
        "source" : "s3://<bucket_name>/nodes.csv",
        "format" : "opencypher",
        "iamRoleArn" : "arn:aws:iam::<account_id>:role/<bucket_name>",
        "region" : "<region_name>",
        "failOnError" : "FALSE",
        "parallelism" : "MEDIUM",
        "updateSingleCardinalityProperties" : "FALSE",
        "queueRequest" : "TRUE",
        "dependencies" : []
      }'

curl -X POST \
       -H 'Content-Type: application/json' \
      https://<neptune_endpoint>:8182/loader -d '
      {
        "source" : "s3://<bucket_name>/edges.csv",
        "format" : "opencypher",
        "iamRoleArn" : "arn:aws:iam::<account_id>:role/<bucket_name>",
        "region" : "<region_name>",
        "failOnError" : "FALSE",
        "parallelism" : "MEDIUM",
        "updateSingleCardinalityProperties" : "FALSE",
        "queueRequest" : "TRUE",
        "dependencies" : []
      }'

# check load status
%%bash
curl -G https://<neptune_endpoint>:8182/loader/
curl -G https://<neptune_endpoint>:8182/loader/f5aaa171-61f1-44f9-ace4-25e1b0178672

# test query
%%bash
time curl https://<neptune_endpoint>:8182/openCypher -d "query=Match (n1:Case) return count(n1);"

# true query timing
time curl https://<neptune_endpoint>:8182/openCypher -d "match (n:Case)-[r:REF*1]->(m:Case) where n.ID in ['618P-B9N1-JS0R-24S3-00000-00', '5507-FGB1-F04G-40CB-00000-00',
                    '4CW5-FVC0-0038-X140-00000-00', '4GSK-BN40-0039-400N-00000-00',
                    '3RX4-1P20-003F-X0FN-00000-00', '57RM-XF11-F04F-R0J8-00000-00',
                    '60R5-T1R1-JFKM-60HT-00000-00', '4SRN-SR80-TX4N-G01R-00000-00',
                    '3S4N-6HS0-001T-51WF-00000-00', '4H51-3YN0-0039-455B-00000-00',
                    '4V6T-H2T0-TXFN-82N7-00000-00', '3RVC-DFJ0-00B1-D4M6-00000-00',
                    '3S65-KDG0-003B-R206-00000-00', '3S4X-8MN0-003B-P11W-00000-00',
                    '3RX6-H430-003D-J52Y-00000-00', '4SYR-BJJ0-TX4N-G0X5-00000-00',
                    '5TSR-HGV1-JG02-S298-00000-00', '5H3M-KGX1-F04F-R0BH-00000-00',
                    '3S4N-90T0-003B-655D-00000-00', '4FFR-CG60-0039-414M-00000-00',
                    '627P-9YV1-JJD0-G0RW-00000-00', '567C-C181-F04C-S221-00000-00',
                    '473M-DGB0-0038-Y1XT-00000-00', '5KM2-1JF1-F04F-C2FS-00000-00',
                    '60F5-KSN1-DYV0-G1VH-00000-00', '3RJN-3N50-0039-441J-00000-00',
                    '3S11-TPF0-003C-R3N5-00000-00', '3S4N-80T0-001T-546B-00000-00',
                    '4J7J-KPC0-TVRV-B36V-00000-00', '588B-T8T1-F07X-W02Y-00000-00',
                    '3X4V-6X80-0038-X3JK-00000-00', '3RRS-9760-003D-W20N-00000-00',
                    '3S4V-NGY0-003B-X1N1-00000-00', '54PS-VT91-F048-8006-00000-00',
                    '3S4X-7SR0-008H-V25Y-00000-00', '4CCN-1100-0038-Y443-00000-00',
                    '3S4N-F100-008H-F1GT-00000-00', '3S4N-B2B0-0054-42WY-00000-00',
                    '3RYC-2RB0-00B1-F2F7-00000-00', '41B0-Y5K0-0038-Y4Y5-00000-00',
                    '4P64-6RH0-TXFS-92MK-00000-00', '5B99-NMX1-DXPF-C002-00000-00',
                    '3S66-5J30-0038-Y35D-00000-00', '3TPC-JYY0-0039-401R-00000-00',
                    '3RXR-0W10-003G-H0PV-00000-00', '3S4N-7R70-0054-44J1-00000-00',
                    '3RHM-YRF0-00B1-F0SS-00000-00', '3S4N-T1H0-003B-V32P-00000-00',
                    '3S4X-3JY0-003S-M14S-00000-00', '3RRM-1WD0-00B1-F1R3-00000-00']
             with m, count(r) as cr return m.ID, cr order by cr desc limit 30;"
"""
