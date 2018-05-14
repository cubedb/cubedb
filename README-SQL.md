## Explanation in SQL terms

### Data structure

CubeDB is a database that stores data in the following structure :

* table T, (also referred here as _cube_)
* string field 'p' (partition field)
* string fields D1, D2, ... Dn. (dimension fields).
* 64 bit integer fields M1, M2, .... Mn (metric fields)

### Inserting data

* You don't need to define the structure of tables prior to using it. Just insert a row into a table, and if the table did not exist, it will be created.
* Same refers to fields. They will be created, if don't exist.
* Only partition field is mandatory. It's name is hardcoded, and it is 'p'
* Dimension fields can be null. If you don't specify all of them when inserting a row, they will be added as 'null'
* Metric fields default values is 0

### Querying the data

Given you have the following:

* fields to group by: G1, G2, ... Gn
* filtering criterias DQ1, DQ2, .... DQn, each of which are a list of possible values
* range filtering criteria for partition: PFrom, PTo

You can get the results of the following query:

```sql
SELECT 'G1' as name,
       G1,
       SUM(M1), SUM(M2), ... SUM(Mn)
FROM T
WHERE D2 in (DQ2) AND D3 in (DQ3) ... -- skip all filters related to G1
  AND p BETWEEN PFrom AND PTo
GROUP BY name, G1

UNION ALL

SELECT 'G2' as name,
       G2,
       SUM(M1), SUM(M2), ... SUM(Mn)
FROM T
WHERE D1 in (DQ1) AND D3 in (DQ3) ... -- skip all filters related to G2
  AND p BETWEEN PFrom AND PTo
GROUP BY name, G2

UNION ALL
...
UNION ALL

SELECT 'GN' as name,
       GN,
       SUM(M1), SUM(M2), ... SUM(Mn)
FROM T
WHERE D1 in (DQ1) ... AND D(N-1) in (DQ(N-1)) ...  -- skip all filters related to GN
  AND p BETWEEN PFrom AND PTo
GROUP BY name, GN

UNION ALL

SELECT 'p' as name,
       p,
       SUM(M1), SUM(M2), ... SUM(Mn)
FROM T
WHERE D1 in (DQ1) ... AND Dn in (DQn) ...
GROUP BY 'name', p
```

To put it in simple terms, this query gives you the SUMS of each edge of an N-dimensional data cube.
