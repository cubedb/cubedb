# CubeDB

A simple, stupid and fast in-memory multi-key counter store.

## Explanation in SQL terms 

### Data Structure
CubeDB is a database that stores data in the following structure :  
- table T, 
- string field 'p' (partition field)
- string fields D1, D2, ... Dn. (dimension fields).
- 64 bit integer fields M1, M2, .... Mn (metric fields)

### Inserting data
- You don't need to define the structure of tables prior to using it. Just insert a row into a table, and if the table did not exist, it will be created. 
- Same refers to fields. They will be created, if don't exist.
- Only partition field is mandatory. It's name is hardcoded, and it is 'p'
- Dimension fields can be null. If you don't specify all of them when inserting a row, 
- Metric fields cannot be null, their default value is 0

### Querying the data
Given you have the following:
- fields to group by: G1, G2, ... Gn 
- filtering criterias DQ1, DQ2, .... DQn, each of which are a list of possible values
- range filtering criteria for partition: PFrom, PTo

You can get the results of the following query:

```sql
select
'G1' as name,
G1,
SUM(M1), SUM(M2), ... SUM(Mn)
from T
WHERE 
D2 in (DQ2) and D3 in (DQ3) ... -- skip all filters related to G1
and p between PFrom and PTo
group by name, G1

UNION ALL

select
'G2' as name,
G2,
SUM(M1), SUM(M2), ... SUM(Mn)
from T
WHERE 
D1 in (DQ1) and D3 in (DQ3) ... -- skip all filters related to G2
and p between PFrom and PTo
group by name, G2

UNION ALL
...
UNION ALL

select
'GN' as name,
GN,
SUM(M1), SUM(M2), ... SUM(Mn)
from T
WHERE 
D1 in (DQ1) ... and D(N-1) in (DQ(N-1)) ...  -- skip all filters related to GN
and p between PFrom and PTo
group by name, GN

select
'p' as name,
p,
SUM(M1), SUM(M2), ... SUM(Mn)
from T
WHERE 
D1 in (DQ1) ... and Dn in (DQn) ...  
group by 'name', p

```
To put it in simple terms, this query gives you the SUMS of each edge of an N-dimensinal datacube.

In the future, I plan to add the support of the following query:

```sql
select 
G1, G2, ... Gn, 
SUM(M1), SUM(M2), ... SUM(Mn)
FROM T
WHERE D1 in (DQ1) and D2 in (DQ2) ...
and p between PFrom and PTo
GROUP by G1, G2, ... Gn
```

Which is a simple pivot.

### Inserting data

You can delete the data only by partitions. In practical terms, you usually specify day or hour as the value
of your partition and remove the old days. 

## Explanation through Crossfilter

Just because the previous chapter could have scared you off, here is an alternative, more visual explanation.
**TL;DR: if you have outgrown Crossfilter and need something that behaves like it, 
but scales to tenths of millions aggregates, this is for you.**


### Crossfilter

This is [Crossfilter](square.github.io/crossfilter/):
![Crossfilter](https://hsto.org/storage2/333/9d1/00a/3339d100a738e6c99fff19d89d31dbb1.png)


It is an incredibely usefull small library and probably one of the best ways of digging into simple
data invented so far. 
I believe that every organisation that requires at least some amount of data analysis would benefit
from it. And therefore, it is an obligation for every data techie to somehow demonstrate it to colleagues.

There is a beautiful visualisation library built on top of it, have a look, it is called 
[dc.js](https://dc-js.github.io/dc.js/).

In fact, if your aggregate count will never exceed a million of rows, you probably
just should stop here, read no further and enjoy the simplicity of the current client-side solution.

### Never say never

Imagine this hipothetical situation where you adopt a Crossfilter based solution in your org and it 
suddenly picks up. Actually, it is not that hypothetical - this happened every time so far in my
personal history.

What happens if you are lucky enough to work in one of those companies or departments that experience
so called exponential growth? What happens, if your data grows massively?

I'l tell you, what. You are screwed with your beautiful interactive graphs alltogether.

Implementing a data analysis solution that relies on browser capacity only is a bit like 
defecating in your own pants: you get an instant sensation of relief and warmness, but than it somehow
start to smell funny. And everyone notices it very quickly.

First of all, retrieving a couple of million records takes time. Processing those rows might make your 
browser unresponsive. If you are using json to load data, you might easily hit [the hardcoded limit of 
a maximum json](https://github.com/nodejs/node/blob/64beab0fc55f750bae648e9b69e027f2dbf3b18a/deps/v8/include/v8.h#L2083)
 string that can be deserialized in V8. You can handle these cases with pagination, but 
believe me, there is a good reason, why it is set so.  Browsers struggle to process more.

Second, you most probably will run out of 8, 16 dimensions, and you might even exceed the hard limit of 32 
dimensions supported by Crossfilter.

Third, you probably will have to start managing the csv/json fileds containing the data. 
Adding/removing fields and old data has to be managed somehow, and the larger is the dataset, the more effort
it takes to do so.

All this will quickly become a major point of pain. This is why this project has been created.

### CubeDB

This project is a server-side simulation of Crossfilter. Only the final aggregates are downloaded to the client, 
and every time you click on a filter, the request is sent to the server side, processed, and new results are retrieved.

Because of this architecture, it is quite hard to achieve 30ms reaction speed, 
however, with powerful servers and fast networks still allow you to achieve speeds <100ms even for larger datasets.