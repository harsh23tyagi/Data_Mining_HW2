## Description

_Task1_: Frequent Itemsets- Users against business and Business against Users.
_Task2:_ Implementing SON Algorithm on a dataset

# Task 1: Simulated Data

_Case 1:_

- Calculate the combinations of frequent businesses (as singletons,pairs,triples,etc.) that are qualified asfrequent given a supportthreshold
  Eg. user1: [business11, business12, business13, ...]

_Case 2:_

- Calculate the combinations of frequent users (as singletons,pairs,triples,etc.) that are qualified asfrequent given a supportthreshold
  Eg. business1: [user1, user2, user3, ...]

# Task 2: Implementing SON Algorithm

- Reading the AZ_Yelp.CSV file into RDD and then build the case1 market-basket model
- Filter out qualified users who reviewed more thank businesses (k is the filter threshold);
- Apply the SON algorithm code to the filtered market-basket model

## Note:

The assignment was given as a part of course Data Mining at USC. I have no rights over the question. However the code is my own. It was performed on yelp data, the files named under business.json and review.json- to which I have no proprietary right.
