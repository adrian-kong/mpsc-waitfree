Test implementation of Jiffy queue

Basically fixed buffers are linked together, when queue insertion reaches a point, it needs to create a new buffer head ahead of time. Uses CAS to avoid locks. 
Buffers are always created on insertion, other callers will "assist" if not.

Adas, Friedman (2020)
https://arxiv.org/abs/2010.14189

Timnat, Petrank (2014)
https://dl.acm.org/doi/10.1145/2555243.2555261
