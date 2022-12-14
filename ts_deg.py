# -*- coding: utf-8 -*-
"""
Created on Wed Dec 14 13:42:45 2022

@author: Giles
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import pickle
import lzma
from tqdm import tqdm,trange
from pyraphtory.algorithm import PyAlgorithm
from pyraphtory.graph import TemporalGraph, Row, Table
from pyraphtory.graph import Row
from pyraphtory.vertex import Vertex
from pyraphtory.context import PyRaphtory
from pyraphtory.graph import Row
from pyraphtory.algorithm import Alignment
from matplotlib import pyplot as plt


if __name__ == "__main__":
    ctx = PyRaphtory.local()
    graph = ctx.new_graph()
    
    with lzma.open("call_ts0.xz","rb") as f:
        call_ts = pickle.load(f)

    ts_st = 1e15
    ts_nd = 0
    for row in trange(len(call_ts)):
    	if type(call_ts[row,1]) == str and type(call_ts[row,2]) == str and call_ts[row,1] < 28800000:
            if call_ts[row,0] < ts_st:
                ts_st = call_ts[row,0]
            if call_ts[row,0] > ts_nd:
                ts_nd = call_ts[row,0]
            source_node = call_ts[row,1]
            destination_node = call_ts[row,2]
            timestamp = int(call_ts[row,0])
            graph.add_vertex(timestamp, source_node, vertex_type='microservice')
            graph.add_vertex(timestamp, destination_node, vertex_type='microservice')
            graph.add_edge(timestamp, source_node, destination_node, edge_type='call')
    
    print(f"End timestamp is {ts_nd}")
    
    split_deg = {}
    for w in trange(1,49):
        split = graph.unitil(int(w*10*60*1000))
        split_deg = split.execute(ctx.algorithms.generic.centrality.Degree()).to_df['name','inDegree','outDegree','degree']
    
    with lzma.open("10m_deg_res.xz","wb") as f:
        pickle.dump(split_deg,f)