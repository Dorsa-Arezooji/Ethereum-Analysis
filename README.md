# Etherium-Analysis
Analysis of Etherum contracts, transactions, gas, scams and scammers' graph using spark and graphframes

## Dataset
The dataset was collected using the dumps uploaded to a repository on [BigQuery](https://bigquery.cloud.google.com/dataset/bigquery-public-data:crypto_ethereum?pli=1). A subset of this data , including contracts and transactions, was then uploaded to HDFS at `/data/ethereum`.
Also, a set of identified scams run on the Ethereum network were collected using [etherscamDB](https://etherscamdb.info/scams) and uploaded to HDFS at `/data/ethereum/scams.json`.
### Dataset Schema
* __Blocks__
```
+-------+--------------------+--------------------+----------------+-----+---------+--------+----------+-----------------+
| number|                hash|               miner|      difficulty| size|gas_limit|gas_used| timestamp|transaction_count|
+-------+--------------------+--------------------+----------------+-----+---------+--------+----------+-----------------+
|4776199|0x9172600443ac88e...|0x5a0b54d5dc17e0a...|1765656009004680| 9773|  7995996| 2042230|1513937536|               62|
|4776200|0x1fb1d4a2f5d2a61...|0xea674fdde714fd9...|1765656009037448|15532|  8000029| 4385719|1513937547|              101|
|4776201|0xe633b6dca01d085...|0x829bd824b016326...|1765656009070216|14033|  8000000| 7992282|1513937564|               99|
```
* __Transactions__
```
+------------+--------------------+--------------------+-------------------+------+-----------+---------------+
|block_number|        from_address|          to_address|              value|   gas|  gas_price|block_timestamp|
+------------+--------------------+--------------------+-------------------+------+-----------+---------------+
|     6638809|0x0b6081d38878616...|0x412270b1f0f3884...| 240648550000000000| 21000| 5000000000|     1541290680|
|     6638809|0xb43febf2e6c49f3...|0x9eec65e5b998db6...|                  0| 60000| 5000000000|     1541290680|
|     6638809|0x564860b05cab055...|0x73850f079ceaba2...|                  0|200200| 5000000000|     1541290680|
```
* __Contracts__
```
+--------------------+--------+---------+------------+--------------------+
|             address|is_erc20|is_erc721|block_number|     block_timestamp|
+--------------------+--------+---------+------------+--------------------+
|0x9a78bba29a2633b...|   false|    false|     8623545|2019-09-26 08:50:...|
|0x85aa7fbc06e3f95...|   false|    false|     8621323|2019-09-26 00:29:...|
|0xc3649f1e59705f2...|   false|    false|     8621325|2019-09-26 00:29:...|

```
* __Scams__
```
0x11c058c3efbf53939fb6872b09a2b5cf2410a1e2c3f3c867664e43a626d878c0: {
    id: 81,
    name: "myetherwallet.us",
    url: "http://myetherwallet.us",
    coin: "ETH",
    category: "Phishing",
    subcategory: "MyEtherWallet",
    description: "did not 404.,MEW Deployed",
    addresses: [
        "0x11c058c3efbf53939fb6872b09a2b5cf2410a1e2c3f3c867664e43a626d878c0",
        "0x2dfe2e0522cc1f050edcc7a05213bb55bbb36884ec9468fc39eccc013c65b5e4",
        "0x1c6e3348a7ea72ffe6a384e51bd1f36ac1bcb4264f461889a318a3bb2251bf19"
    ],
    reporter: "MyCrypto",
    ip: "198.54.117.200",
    nameservers: [
        "dns102.registrar-servers.com",
        "dns101.registrar-servers.com"
    ],
    status: "Offline"
},
```
## Analysis & Results

### 1. Total number of transactions
In `number_of_transactions.py`the total number of transactions are aggregated for each month included in the dataset.

__*Results:*__

![number of transactions vs time](https://github.com/Dorsa-Arezooji/Etherium-Analysis/blob/master/results/transactions_time.png)

### 2. Top 10 most popular services
In `top10_addresses.py` the top 10 services with the highest amounts of Ethereum received for smart contracts are yielded.

__*Results:*__

rank | address | total Ether received
-----|---------|------------
1 | 0xaa1a6e3e6ef20068f7f8d8c835d2d22fd5116444 | 8.415510081e+25
2 | 0xfa52274dd61e1643d2205169732f29114bc240b3 | 4.57874844832e+25
3 | 0x7727e5113d1d161373623e5f49fd568b4f543a9e | 4.56206240013e+25
4 | 0x209c4784ab1e8183cf58ca33cb740efbf3fc18ef | 4.31703560923e+25
5 | 0x6fc82a5fe25a5cdb58bc74600a40a69c065263f8 | 2.7068921582e+25
6 | 0xbfc39b6f805a9e40e77291aff27aee3c96915bdd | 2.11041951381e+25
7 | 0xe94b04a0fed112f3664e45adb2b8915693dd5ff3 | 1.55623989568e+25
8 | 0xbb9bc244d798123fde783fcc1c72d3bb8c189413 | 1.19836087292e+25
9 | 0xabbb6bebfa05aa13e908eaa492bd7a8343760477 | 1.17064571779e+25
10 | 0x341e790174e3a4d35b65fdc067b6b5634a61caea | 8.37900075192e+24

### 3. Scams
#### 3.1. Most lucritive forms of scams
In `scams.py`, the types of scam making the most Ether are yielded.

__*Results:*__

rank | most lucrative scam (category) | total Ether profited
-----|--------------------------------|------------
1 | Scamming | 3.901380697597355e+22
2 | Phishing | 3.1319012152715346e+22
3 | Fake ICO | 1.35645756688963e+21

#### 3.2. How different scams changed with time
In `scams.py` the total sum of Ether made off of each scam category is calculated for each month included in the dataset.

__*Results:*__

![scams vs time](https://github.com/Dorsa-Arezooji/Etherium-Analysis/blob/master/results/scams_time.png)
* In July of 2017, phishing attacks were the most profitable form of scam.
* In September of 2017, fake ICO was the most profitable form of scam.
* In September of 2018, scamming attacks were the most profitable form of scam.

### 4. Gas
#### 4.1. Transactions gas price vs time
In `gas.py`, the average price of gas is calculated for each month by: total sum of gas prices / total number of transactions.

__*Results:*__

![transaction gas price vs time](https://github.com/Dorsa-Arezooji/Etherium-Analysis/blob/master/results/Gas_Time.png)
* The average price of gas has generally decreased with peaks during the first few months of each year.

#### 4.2. Contract gas vs time
In `gas.py`, the same approach for transactions is taken for smart contracts as well, with one alteration: in order to make sure a block represents a smart contract, it needs to be joined with the `contracts` dataset. As the block `id` is unique and present in both datasets, it is used as the joining key.

__*Results:*__

![contract gas vs time](https://github.com/Dorsa-Arezooji/Etherium-Analysis/blob/master/results/contract_gas.png)
* Contracts have been requiring more gas since the start of Ethereum, and it appears that the needed gas has reached a somewhat steady state.

#### 4.3. Contract complexity vs time
The complexity of a transaction is reflected in its `difficulty` level, which is available in the `blocks` dataset.

__*Results:*__

![contract complexity vs time](https://github.com/Dorsa-Arezooji/Etherium-Analysis/blob/master/results/complexity_time.png)
* Contracts have become more complicated with peaks in late 2017 and 2018, followed by a general decrease.

* The required gas for a contract apears to be strongly correlated with the contract's complexity (difficulty): 
        corr(diff, gas) = 0.9385
* With higher complexity, more gas would be required, so the most popular services might have been mining complex contracts.

### 5. Graph Analysis
#### 5.1. Triangle count
The target dataset for this part of the analysis is the `transaction` dataset. 
To find all the nodes in the dataset (to addresses and from addresses), the to and from addresses are concatenated using the `union()` method. Then, to avoid repetition of nodes, the `distinct()` method is called on the `vertices` RDD. Next, the RDDs are converted to dataframes (with the correct formats described in `e_f(x)` and `v_f(x)`) and the graph is built using these two dataframes. Finally to find the triangles, a motif is called on the graph, searching for all sets of 3 nodes that form a triangle: a-->b, b-->c, c-->a.

__*Results:*__

The resulting file is too large to include here.

#### 5.2. Scammer wallets
In `scammers_graph.py`, the general premise is that the addresses where scammers are accumulating their stolen cryptocurrency, has a lot more inDegrees than outDegrees, and that there are fairly many inDegrees. These assumptions are used in the custom function `is_scammer()`.
The inDegrees and outDegrees are calculated for all of the nodes and converted into RDDs to allow RDD transformations. To calculate the ratio of outDegrees to inDegrees for each address, the inDegrees and outDegrees RDDs are joined with the address as the join key.
Then, the joined RDD is filtered using `is_scammer()` to see which addresses meet the conditions stated above. Lastly, the resulting RDD from the previous stage is joined with the scammers RDD to filter out the addresses that were not recognized as scammers to yield the addresses used by scammers to accumulate stolen Ether.

__*Results:*__

The results can be accessed via [scammers.txt](https://github.com/Dorsa-Arezooji/Etherium-Analysis/blob/master/results/scammers.txt).
A subset of results for refference:
wallet | inDegrees | outDegrees
-------|-----------|-----------
0x1e3c07ce10973fcaebc81468af1d3f390d2a4c71 | 165 | 15
0x69f8e87518129498da751f26ea2309db05e7270b | 360 | 29
0x40949225c4a1745a9946f6aaf763241c082cb9ac | 454 | 22
