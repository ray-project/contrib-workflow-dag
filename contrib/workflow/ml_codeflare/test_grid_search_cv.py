import ray
from ray import workflow
from contrib import workflow as contrib_workflow
from contrib.workflow.graph.dag import DAG
from contrib.workflow.ml_codeflare.Runtime import *
import shutil
from enum import Enum
import sys
import inspect
import statistics

import timeit
from typing import Callable, List
import numpy as np
import pandas as pd
from pandas import DataFrame
import sklearn
from sklearn import datasets
from sklearn.pipeline import Pipeline
from sklearn.pipeline import FeatureUnion
from sklearn.compose import ColumnTransformer
from sklearn.decomposition import PCA
from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier
import sklearn.base as base
from sklearn.preprocessing import StandardScaler, MinMaxScaler, MaxAbsScaler, RobustScaler
from sklearn.utils.validation import check_is_fitted
from sklearn.exceptions import NotFittedError
from sklearn.model_selection import train_test_split

import contrib.workflow.ml_codeflare.Datamodel as dm
from contrib.workflow.ml_codeflare.Datamodel import *


storage = "/tmp/ray/workflow_data/"
shutil.rmtree(storage, ignore_errors=True)
#workflow.init(storage)

workflow.init()

X_digits, y_digits = datasets.load_digits(return_X_y=True)

pca = PCA()
# set the tolerance to a large value to make the example faster
logistic = LogisticRegression(max_iter=10000, tol=0.1)

start = timeit.timeit()

pipeline = dm.Pipeline()
node_pca = dm.EstimatorNode('pca', pca).get_node()
node_logistic = dm.EstimatorNode('logistic', logistic).get_node()

pipeline.add_edge(node_pca, node_logistic)

# param_grid
param_grid = {
        'pca__n_components': [5, 15, 30, 45, 64],
        'logistic__C': np.logspace(-4, 4, 4),
    }


pipeline_param = dm.PipelineParam.from_param_grid(param_grid)



#print("\n\n pipeline_param \n\n")
#for node, param in pipeline_param.get_all_params().items():
#    print(node, param)

#print("\n\n removing the 'f' from the node name \n\n")
#for node, param in pipeline_param.get_all_params().items():
#node = node.replace('f__', '__')
#    print(node)



pipeline_input = (X_digits, y_digits)

# default KFold for grid search
k = 5
kf = KFold(k)


results = grid_search_cv(kf, pipeline, pipeline_input, pipeline_param)
## finding the best parameters

end = timeit.timeit()
print("\n\n elaspe time: \n\n", end-start)

best_estimators = None
best_mean_scores = 0.0
best_n_components = 0

df = pd.DataFrame(columns = ('n_components', 'mean_test_score', 'std_test_score'))

for estimators_scores in results:
    cv_estimators = estimators_scores[0]
    scores = estimators_scores[1]
    mean = statistics.mean(scores)
    std = statistics.stdev(scores)
    n_components = 0
    params = {}
    for em_name, em in cv_estimators.items():
        params = em.get_params()
        if 'n_components' in params.keys():
            n_components = params['n_components']
            assert (n_components > 0)
            
    df = df.append({'n_components': n_components, 'mean_test_score': mean, 'std_test_score': std}, ignore_index=True)
    
    if mean > 0.92:
        print(mean)
        for em_name, em in cv_estimators.items():
            print(em_name, em.get_params())
        
    if mean > best_mean_scores:
        best_estimators = cv_estimators
        best_mean_scores = mean
        best_n_components = n_components
        
print("\n\n best pipeline: \n")
for em_name, em in best_estimators.items():
    print(em_name, em.get_params())
print("\n\n best mean_scores: ", best_mean_scores)
        

    




