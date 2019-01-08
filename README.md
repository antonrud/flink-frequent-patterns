# Mining Frequent Patterns

Homework for DWBI class. TU Berlin, winter term 2018/2019


## What it does

This application performs mining of frequent patterns from a dataset of retail transactions.
In particular this program is an implementation of **ECLAT algorithm** *(Equivalence Class Transformation)* - a depth-first search algorithm based on set intersection.

The tasks are performed within **Apache Flink** execution environment as batch job and mostly in parallel.


## How to use

1. Download data set from here: https://tubcloud.tu-berlin.de/s/ZtfgnxMCZ5cjJf8 (or [mirror](https://drive.google.com/open?id=1rP-4FUSdWGVqLOKbr_8O24NsDm2OONgW))
2. Place it under **src/main/resources**
3. Run *main()* method in **ECLATJob** class
4. Find results in **src/main/resources**


## Built with

* [Apache Flink](https://flink.apache.org/) - framework and distributed processing engine for stateful computations
* [Online Retail Data Set](https://archive.ics.uci.edu/ml/datasets/online+retail) - a data set for an online retail
                                                                                    

## Autors

* **Anton Rudacov** - [@antonrud](https://github.com/antonrud)
* **Ilja Hramcov** - [@hramilj](https://gitlab.tubit.tu-berlin.de/hramilj)
* **Louise Rosniewski** 
