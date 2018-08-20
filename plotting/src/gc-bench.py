import pandas as pd
import numpy as np
import json
from collections import defaultdict
import itertools
from functools import reduce

import rpy2.robjects.lib.ggplot2 as ggplot2
import rpy2.robjects

BASEPATH = 'plotting/data/'
DATA_FILE = 'gc-bench.json'
# BASEPATH = ''
# DATA_FILE = 'results.json'

normalize_over=1

def get_data():
    return json.load(open(BASEPATH + DATA_FILE))

def prepare_data(data):
    # print(data)
    flattened = []
    for i in data:
        flattened.extend(i)
    # print(flattened)
    # flattened = reduce(lambda x,y: x.extend(y), data) <-- does not work! don't know why!
    # print(flattened)
    sorted_data = sorted(flattened, key=lambda x: x["cores"])
    grouped_by_version = itertools.groupby(sorted_data,lambda x: x["cores"])
    d = {
        cores : {
            record["size"] : np.mean(record["sysAndTimes"]["sbfm-chan"])
            for record in records
        }
        for cores,records in grouped_by_version
    }
    # print(pd.DataFrame(d))
    d1 = {
        str(name) : {
            i : d[normalize_over][i]/v
            for i,v in value.items()
        }
        for name,value in d.items()
    }
    # print(pd.DataFrame(d1))
    d1['size'] = { k: k for k,_ in d[1].items() }
    d1.pop('1')
    return d1

def prepare_frame(data):
    d = pd.DataFrame(data)
    # print(d)
    melted = pd.melt(d, id_vars=["size"], var_name="cores")
    # print(melted)
    return melted

def plot():
    melted = prepare_frame(prepare_data(get_data()))

    from rpy2.robjects import pandas2ri
    pandas2ri.activate()
    r_melted = pandas2ri.py2ri(melted)
    # print(r_melted)

    gg = ggplot2.ggplot(r_melted) + \
         ggplot2.aes_string(x='size', y='value', colour="cores") + \
         ggplot2.geom_point(size=3) + \
         ggplot2.labs(x="# entries and fields per table", y="speedup over single core") + \
         ggplot2.theme_grey(base_size = 15) + \
         ggplot2.theme(**{'legend.position' : "right",
                          'plot.background': ggplot2.element_rect(fill="transparent", colour="NA")})
                 # ggplot2.xlim(0,13000) + \
    return gg

if __name__ == '__main__':
    gg = plot()
    gg.save("plotting/plots/gc-bench.pdf", width=8, height=4, bg="transparent")
