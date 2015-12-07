## Exercise 9

Write a MapReduce program in Hadoop that implements some statistical computations on a real data set.

### Input

* Browse to the [Adult Data Set](https://github.com/tonellotto/PAD-LABS/tree/master/data/snippets.zip) Web page and download from the *Data Folder* the [adult.data](http://archive.ics.uci.edu/ml/machine-learning-databases/adult/) dataset.
* Delete the empty lines at the end of the file, since they may generate parsing errors.
* Dataset attributes (column names) are: 

    |age|workclass|fnlwgt|education|education-num|marital-status|occupation|relationship|race|sex|capital-gain|capital-loss|hours-per-week|native-country|<trash>
    |---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|

* Row elements are separated by a comma followed by a whitespace.

### Algorithm

1. For each native-country, find the minimum and maximum age.
2. In addition to minimum and maximum, also find average value.
3. In addition to minimum and maximum, also find median value.
4. Do the previous analysis for each unique native-country AND workclass pair.

