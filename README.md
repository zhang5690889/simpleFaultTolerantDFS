Simple Fault Tolerant Distributed File System

The readme contains two parts: DFS design and fault handling

DFS Design:

Domain Entities and their responsibilities:

Client: User end program providing user level operations including upload, download, and delete.

Proxy: The central map to every component. It is responsible for determining which master to use.

Minion: Lowest level of the system responsible for saving and retrieving data.

Master: Manager of minions. It determines allocation scheme and operations on top of minions, such as checking minion
        cluster status, and file replication status

Replication scheme:

K-way replication. Each file has k number of copies


Fault Handling and Solution

Basic DFS functionality test [test1]

K-1 Minion Offline:

1. Client should still be able to retrieve the file, because there are k copies. At least 1 copy remain in the system.
   [test2]
   
2. Master should know all minion status. [test4]

3. Master should provide API to fix k replication property [test6]

Main master Offline:
1. Proxy pick a new master [test5]

Race condition:
1. Client receives a dead master port from proxy [test3]

2. Client receives a dead minion port from master [test7]


Author: Xinwen Zhang