import ray
from ray import workflow
import contrib
import pandas as pd
import contrib.workflow.graph as contrib_workflow
from contrib.workflow.graph.dag import DAG
from contrib.workflow.graph.node import Node
import contrib.workflow.ml_codeflare.Datamodel as dm
from contrib.workflow.ml_codeflare.Exceptions import *
from contrib.workflow.ml_codeflare.Datamodel import *


from sklearn.model_selection import KFold
from sklearn.model_selection import BaseCrossValidator
from sklearn.base import clone

from abc import ABC, abstractmethod
from enum import Enum
from sklearn.base import BaseEstimator
import sklearn.base as base
from sklearn.utils.validation import check_is_fitted
from sklearn.exceptions import NotFittedError
from datetime import datetime


def split(cross_validator: BaseCrossValidator, X, y):
    split_input = []
    for train_index, test_index in cross_validator.split(X, y):
        if isinstance(X, pd.DataFrame) or isinstance(X, pd.Series):
            X_train, X_test = X.iloc[train_index], X.iloc[test_index]
        else:
            X_train, X_test = X[train_index], X[test_index]
            
        if isinstance(y, pd.DataFrame) or isinstance(y, pd.Series):
            y_train, y_test = y.iloc[train_index], y.iloc[test_index]
        else:
            y_train, y_test = y[train_index], y[test_index]
            
        split_input.append((X_train, X_test, y_train, y_test))
    return split_input
 
def check_pipeline_is_legit(num_nodes, candidate_edges):
    assert candidate_edges is not None
    nodes = {}
    for edge in candidate_edges:
        from_node_name = edge[0].get_name()
        to_node_name = edge[1].get_name()
        if from_node_name in nodes.keys():
            pass
        else:
            nodes[from_node_name] = 1
        if to_node_name in nodes.keys():
            pass
        else:
            nodes[to_node_name] = 1
    return (num_nodes == len(nodes))

def grid_search_cv(cv, pipeline, pipeline_input, pipeline_param):
  
    # Loop through pipeline_param.get_all_params() and generate a new pipeline.
    # For each pipeline, run through with every splitted input, each with a score.
    # This implementation is needed because there is only one output from a DAG.
    # Once a DAG allows multiple output nodes, we can implement a
    # get_parameterized_pipeline and store the entire expanded DAG with multiple output
    # nodes and do a single DAG.execute() with each splitted data from
    # get_paramterized_pipeline_input
    
    pipeline_params = pipeline_param.get_all_params()
    parameterized_nodes = {}
    for node_name, params in pipeline_params.items():
        node_name_part, num = node_name.split('__', 1)
        if node_name_part not in parameterized_nodes.keys():
            parameterized_nodes[node_name_part] = []
        # get the estimator from the virtual node in the original pipeline
        # The assumption: the node_name in the original pipeline has appended '_vf'
        # To get the estimator virtual actor id we need to strip this '_vf' at the end
        # print("\n\n node_name_part: ", node_name_part)
        v_actor = workflow.get_actor(node_name_part[:-3])
        estimator = v_actor.get_model.run_async()
        new_estimator = clone(ray.get(estimator))
        new_estimator = new_estimator.set_params(**params)
        new_estimator_name = node_name_part[:-3]+'__'+num
        new_v_node = dm.EstimatorNode(new_estimator_name, new_estimator).get_node()
        parameterized_nodes[node_name_part].append(new_v_node)
        
    # update parameterized nodes with missing non-parameterized nodes to hold the
    # the original dag nodes
    
    all_nodes = pipeline.get_dag().get_nodes()
    for node_name, node in all_nodes.items():
        # print("\n\n all nodes: ", node_name)
        if node_name not in parameterized_nodes.keys():
            parameterized_nodes[node_name] = [node]
        
    # contructing a set of new edges for each original edge with parameterized_nodes
    all_old_edges = pipeline.get_dag().get_edges()
    
    all_new_edges = []
    for old_edge in all_old_edges:
        from_node_name = old_edge[0].get_name()
        to_node_name = old_edge[1].get_name()
        expanded_from_nodes = parameterized_nodes[from_node_name]
        expanded_to_nodes = parameterized_nodes[to_node_name]
        new_edges = []
        for expanded_from_node in expanded_from_nodes:
            for expanded_to_node in expanded_to_nodes:
                new_edges.append([expanded_from_node, expanded_to_node])
        all_new_edges.append(new_edges)
        
    # constructing all potential pipeline_edges from all_new_edges
    link_level_0 = all_new_edges[0]
    pipeline_edges = [[edge] for edge in link_level_0]
    for link_level_i in all_new_edges[1:]:
        new_pipeline_edges = [edge_1.append(edge_2) for edge_1 in pipeline_edges for edge_2 in link_level_i]
        pipeline_edges = new_pipeline_edges
    
    #print("\n\n total num of candidate pipelines: \n\n", len(pipeline_edges))
    
    total_num_nodes = len(pipeline.get_dag().get_nodes().keys())
    i = 0
    all_pipelines = []
    for p_edges in pipeline_edges:
        if check_pipeline_is_legit(total_num_nodes, p_edges):
            my_pipeline = dm.Pipeline('pipleine_'+str(i))
            for edge in p_edges:
                from_node = edge[0]
                to_node = edge[1]
                my_pipeline.add_edge(from_node, to_node)
            all_pipelines.append(my_pipeline)
        i = i+1
    
    # print("\n\n total num of legit pipelines\n\n", len(all_pipelines))
        
    # Now, for each pipeline in all_pipelines, feed split data and collect results
    results = []
    # split the pipeline_input based on cv
    (X, y) = pipeline_input
    split_inputs = split(cv, X, y)
    for my_pipeline in all_pipelines:
        # prepare input data
        input_nodes = my_pipeline.get_dag().get_input_nodes()
        if len(input_nodes) > 1:
            raise PipelineException("Currently only support a single input node")
        input_node = input_nodes[0]
        cv_scores = []
        cv_estimators = {}
        # for each pipeline, run through cv() with split input & collect score
        for X_train, X_test, y_train, y_test in split_inputs:
            pipeline_input_fit = (X_train, y_train, ExecutionType.FIT)
            data_input_fit = {input_node: {0:pipeline_input_fit}}
            (X_out, y_out, e_mode) = my_pipeline.execute_pipeline(data_input_fit)
            pipeline_input_score = (X_test, y_test, ExecutionType.SCORE)
            data_input_score = {input_node: {0:pipeline_input_score}}
            (X_junk, score, e_mode) = my_pipeline.execute_pipeline(data_input_score)
            cv_scores.append(score)
        # grab all the estimators and append them into cv_estimators
        all_nodes = my_pipeline.get_dag().get_nodes()
        for node_name, node in all_nodes.items():
        #    check if a node is a virtual actor
            if "_vf" in node_name:
                #print("\n\n node_name: ", node_name)
                v_actor = workflow.get_actor(node_name[:-3])
                estimator = v_actor.get_model.run_async()
                cv_estimators[node_name[:-3]] = ray.get(estimator)
        results.append([cv_estimators, cv_scores])
        #print("\n\n finished pipeline: ", my_pipeline.get_name())
        
    return results
