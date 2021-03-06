# Code Samples of Work by Dusan Bosnjakovic

The focus of this repo is to highlight snippets of data science work done in a variety of languages. There are no complete projects here, as they mainly pertain to work done for my employers or clients.


## Retro Impute

Full project done to examine the impact of various imputation strategies when partial training data only is missing but predict data is not. Conclusion was that even partial training data can be beneficial to the model performance given appropriate imputation strategy.
https://github.com/dusaneh/examples/tree/master/Retro%20Impute

## Feature creation in Scala/Spark

A code snippit of historically accurate feature creation in Scala/Spark
https://github.com/dusaneh/examples/blob/master/GetMerchantLinks.scala

## Business Plots

Appropriate threshold in business contexts is important. This project demonstrates how one can develop a stable precision estimate at any score threshold with a confidence interval. The approach uses cross-validation data to arrive at multiple observations to arrive at a stable mean and a confidence interval. 
https://github.com/dusaneh/examples/blob/master/businessPlots.ipynb

## Manifest Approach

This approach takes in a sample data set, generates data types for user confirmation of treatment and lets the user specify whether variables are features/predictors or labels. Finally, the user can specify what type of a lable is it (continuous, multi-class or binary). Intermediate files are saved and the second stage can take in the sample files along with the manifest file.
https://github.com/dusaneh/examples/blob/master/Manifest%20Approach.ipynb

## Load and Explore

A simple process for loading and exploring new data.
https://github.com/dusaneh/examples/blob/master/Load%20and%20Explore.ipynb

## Parallel processing

Example of parallelizing processes in R
https://github.com/dusaneh/examples/blob/master/doParallel_parallelism.R

## Creating custom objective functions for CV

How to creat a custom objective function (e.g. MAP) in R's caret package
https://github.com/dusaneh/examples/blob/master/caret_classProb_averagePrecisionScore.R
