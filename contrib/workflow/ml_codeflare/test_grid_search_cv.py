import ray
from ray import workflow
from contrib import workflow as contrib_workflow
from contrib.workflow.graph.dag import DAG
from contrib.workflow.ml_codeflare.Runtime import *
import shutil
from enum import Enum
import sys
import inspect

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
## prepare the data


