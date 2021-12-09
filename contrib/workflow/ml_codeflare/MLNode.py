import ray
from ray import workflow
from ray import serve
import contrib.workflow.graph as contrib_workflow
from contrib.workflow.graph.dag import DAG
import shutil
from enum import Enum
import sys
import inspect
from copy import deepcopy

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

from time import time

@serve.deployment(num_replicas = 3)
class WorkflowRouterDirector:
    def __init__(self, *args):
        import nest_asyncio
        nest_asyncio.apply()
        workflow.init()

        self.currentSessions = dict()
        self.template = args[0]
        self.pipeline_name = args[0].get_pipeline_name()

    def clone_pipeline(self, oldpipeline, newsessionid):
        suffix = newsessionid
        cloned_pipeline = dm.Pipeline(newsessionid)
        pipeline_actor = workflow.get_actor(oldpipeline)
        ray.get(pipeline_actor.ready())

        tdag = ray.get(pipeline_actor.get_dag.run_async())
        edges = tdag.get_edges()
        for edge in edges:
            from_va_name = edge[0].get_vaname()
            from_node_actor = workflow.get_actor(from_va_name)
            ray.get(from_node_actor.ready())
            from_fitted_est = from_node_actor.__getstate__.run()
            cloned_from_node = dm.EstimatorNode(from_va_name+'_'+suffix, from_fitted_est).get_node()

            to_va_name = edge[1].get_vaname()
            to_node_actor = workflow.get_actor(to_va_name)
            ray.get(to_node_actor.ready())
            to_fitted_est = to_node_actor.__getstate__.run()
            cloned_to_node = dm.EstimatorNode(to_va_name+'_'+suffix, to_fitted_est).get_node()
            cloned_pipeline.add_edge(cloned_from_node, cloned_to_node)

        cloned_ml_pipeline = deepcopy(self.template)
        cloned_ml_pipeline.set_pipeline(newsessionid, cloned_pipeline)
        return cloned_ml_pipeline

    def __call__(self, *args):
        session_id = args[0][0]
        input = args[0][1:]
        print(f"\n{session_id}: is routed to replica: {serve.get_replica_context().replica_tag}\n")
        if session_id not in self.currentSessions.keys():
            print(f"\n\n{session_id}, NOT FOUND within currentSessions\n\n")
            confirmsessionexist = True
            try:
                known_session = workflow.get_actor(session_id)
                ray.get(known_session.ready())
                cloned_ml_pipeline = deepcopy(self.template)
                cloned_ml_pipeline.set_pipeline(session_id, dm.Pipeline(session_id))
                self.currentSessions[session_id] = cloned_ml_pipeline
            except ray.workflow.storage.base.KeyNotFoundError:
                confirmsessionexist = False
                print(f"\n\n{session_id}, NOT FOUND\n\n")
            if confirmsessionexist is False:
                newpipeline = self.clone_pipeline(self.pipeline_name, session_id)
                self.currentSessions[session_id] = newpipeline

        (X_out, y_out, predict) = self.currentSessions[session_id].execute(input)
        return (X_out, y_out, predict)

    def __str__(self):
        return ','.join(self.currentSessions.keys())

def workflowrouter(obj):
    def wrapper(*args, **kwargs):
        stateful = obj(args[0])
        WorkflowRouterDirector.deploy(stateful)
        statefulrouter = WorkflowRouterDirector.get_handle()
        return statefulrouter
    return wrapper

@workflowrouter
class MLPipeline:
    def __init__(self, pipeline_name):
        # import nest_asyncio
        # nest_asyncio.apply()
        # localstorage = "/tmp/ray/workflow_data/"
        # workflow.init(localstorage)
        self.pipeline_name = pipeline_name

    def set_pipeline(self, pipeline_name, pipeline):
        self.pipeline_name = pipeline_name
        self.pipeline = pipeline

    def get_pipeline_name(self):
        return self.pipeline_name

    def execute(self, pipeline_input):
        (X_out, y_out, predict) = self.pipeline.execute_pipeline(pipeline_input)
        return (X_out, y_out, predict)

    def __reset__(self, pipeline_name):
        self.pipeline_name = pipeline_name

    def __str__(self):
        return self.pipeline_name
