import ray
from ray import workflow
from ray import serve
from ray.serve import exceptions
import contrib.workflow.graph as contrib_workflow
from contrib.workflow.graph.dag import DAG
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

def feature_union(*inputtuple):
    X_list = []
    y = None
    flag = True
    for feature in list(inputtuple):
        (X, y, flag) = feature
        X_list.append(X)
    X_concat = np.concatenate(X_list, axis=1)
    return (X_concat, y, flag)

storage = "/tmp/ray/workflow_data/"
shutil.rmtree(storage, ignore_errors=True)
#workflow.init(storage)

workflow.init()

## prepare the data
X = pd.DataFrame(np.random.randint(0,100,size=(10000, 4)), columns=list('ABCD'))
y = pd.DataFrame(np.random.randint(0,2,size=(10000, 1)), columns=['Label'])

numeric_features = X.select_dtypes(include=['int64']).columns
numeric_transformer = sklearn.pipeline.Pipeline(steps=[('scaler', StandardScaler())])

## set up preprocessor as StandardScaler
preprocessor = ColumnTransformer(
    transformers=[
        ('num', numeric_transformer, numeric_features),
        ])

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)

pipeline = dm.Pipeline('base_pipeline')
#pipeline = dm.Pipeline('fit_'+datetime.now().strftime("%Y-%m-%d_%H:%M"))
node_a = dm.EstimatorNode('preprocess', preprocessor).get_node()
node_b1 = dm.EstimatorNode('minmaxscaler', MinMaxScaler()).get_node()
node_b2 = dm.EstimatorNode('standardscaler', StandardScaler()).get_node()
node_c = dm.AndNode('feature_union', feature_union).get_node()
node_d = dm.EstimatorNode('decisiontree', DecisionTreeClassifier(max_depth=3)).get_node()



pipeline.add_edge(node_a, node_b1)
pipeline.add_edge(node_a, node_b2)
pipeline.add_edge(node_b1, node_c)
pipeline.add_edge(node_b2, node_c)
pipeline.add_edge(node_c, node_d)

# create a fit pipeline from the base_pipeline

pipeline_input_fit = (X_train, y_train, ExecutionType.FIT)
(X_out, y_out, fit) = pipeline.execute_pipeline(pipeline_input_fit)

print("\n\n output after FIT: ", X_out.shape, y_out.shape, fit)
print("\n\n")

# run fit again with a different input data
# (X_train, X_test, y_train, y_test) = train_test_split(X, y, test_size=0.15)
# pipeline_input_fit = (X_train, y_train, ExecutionType.FIT)
# (X_out, y_out, fit) = pipeline.execute_pipeline(pipeline_input_fit)
#
# print("\n\n output after 2nd FIT: ", X_out.shape, y_out.shape, fit)
# print("\n\n")

#define a wrapper class for deployment

serve.start()

"""
Construct a Deployment. CONSTRUCTOR SHOULDN'T BE USED DIRECTLY.

Deployments should be created, retrieved, and updated using
`@serve.deployment`, `serve.get_deployment`, and `Deployment.options`,
respectively.
"""

class DeploymentRouter:
    def __init__(self):
        self.currentDeployments = {}

    def get_or_create_deployment(self, deployment_name: str, pipeline_name:str):
        assert deployment_name is not None
        if deployment_name in self.currentDeployments.keys():
            return self.currentDeployments[deployment_name]
        else:
            #create a deployment with the name and add it to self.currentDeployment
            @serve.deployment(name = deployment_name)
            class MLPipeline:
                def __init__(self, pipeline_name):
                    import nest_asyncio
                    nest_asyncio.apply()
                    storage = "/tmp/ray/workflow_data/"
                    workflow.init()
                    self.pipeline = Pipeline(pipeline_name)

                def __call__(self, pipeline_input):
                    (X_out, y_out, predict) = self.pipeline.execute_pipeline(pipeline_input)
                    return (X_out, y_out, predict)

            MLPipeline.deploy(pipeline_name)
            self.currentDeployments[deployment_name] = MLPipeline
            return MLPipeline

    def assign_to_deployment(self, deployment_name: str, input):
        assert deployment_name is not None
        if deployment_name in self.currentDeployments.keys():
            myDeployment = self.currentDeployments[deployment_name]
        else:
            raise RuntimeError(f"deployment name: {deployment_name} does not exist")
        handle = myDeployment.get_handle()
        (X_out, y_out, predict) = ray.get(handle.remote(input))
        return (X_out, y_out, predict)


myRouter = DeploymentRouter()
myRouter.get_or_create_deployment('p0', 'base_pipeline')
myRouter.get_or_create_deployment('p1', 'base_pipeline')

X_train_1, X_test_1, y_train_1, y_test_1 = train_test_split(X, y, test_size=0.1)
input_0 = (X_test, y_test, ExecutionType.PREDICT)
input_1 = (X_test_1, y_test_1, ExecutionType.PREDICT)
(X_out, y_out, predict) = myRouter.assign_to_deployment('p0', input_0)
print(f"\n\n output from deployment p0, {X_out.shape}, {y_out.shape}\n\n")
(X_out, y_out, predict) = myRouter.assign_to_deployment('p1', input_1)
print(f"\n\n output from deployment p1, {X_out.shape}, {y_out.shape}\n\n")
# for i in range(4):
#     @serve.deployment(name = 'p'+str(i))
#     class MLPipeline:
#         def __init__(self, pipeline_name):
#             import nest_asyncio
#             nest_asyncio.apply()
#             #storage = "/tmp/ray/workflow_data/"
#             #workflow.init()
#             self.pipeline = Pipeline(pipeline_name)
#
#         def __call__(self, pipeline_input):
#             (X_out, y_out, predict) = self.pipeline.execute_pipeline(pipeline_input)
#             return (X_out, y_out, predict)
#
#     MLPipeline.deploy('base_pipeline')
#
# myDeployment = serve.get_deployment(name = 'p0')
#
# handle = myDeployment.get_handle()
#
# pipeline_input_predict = (X_test, y_test, ExecutionType.PREDICT)
# #data_input_pred = {node_a:{0:pipeline_input_predict}}
#
# (X_out, y_out, predict) = ray.get(handle.remote(pipeline_input_predict))
#
# print("\n\n output after PREDICT: ", X_out.shape, y_out.shape, predict)
# print("\n\n")
