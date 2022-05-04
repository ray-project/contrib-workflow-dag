import ray
from ray import workflow
from ray import serve
from ray.workflow.step_function import WorkflowStepFunction
from typing import *
import logging
from filelock import FileLock
from enum import Enum
from pathlib import Path
import uuid
import requests
import shutil
from ray.util.queue import Queue, Empty, Full
from ray.workflow.common import asyncio_run

Path("/tmp/contract-log").unlink(missing_ok=True)



def logging(msg):
    with FileLock("/tmp/log-lock"):
        with Path("/tmp/contract-log").open("a") as f:
            f.write(msg + "\n")
            f.flush()
import random
random.seed(10)

storage = "/tmp/ray/workflow_data/"
shutil.rmtree(storage, ignore_errors=True)

ray.init(address='auto')
workflow.init()


from datetime import datetime
from time import sleep
from ray.workflow.event_listener import TimerListener
import asyncio

@workflow.step(checkpoint=False)
def updated_contract(contract):
    (contract_id, httpend, approved_list) = contract
    new_contract_id = "[new]" +  contract_id
    logging(f"Update contract from: {contract_id} to {new_contract_id}")
    new_contract = (new_contract_id, httpend, {})
    return new_contract


@workflow.step
def wait_for_event(node_name, contract, email):
    (contract_id, httpend, approved_list) = contract
    myQueueID = f"{contract_id}.{node_name}.{email}"
    from ray import serve
    handle = ray.serve.get_deployment(httpend).get_handle(sync=True)
    logging(f"waiting for approval from: {contract_id}.{node_name}.{email}")
    return handle.wait_for_event.remote(myQueueID)


def eventnode(node_name, contract, emails: List[str]):
    (contract_id, httpend, approved_list) = contract
    if 'rejected' in approved_list.values():
        logging(f"On {node_name}: {contract_id} was rejected already")
        approved_list[node_name] = 'rejected'
        return (contract_id, httpend, approved_list)
    approval_requests = [wait_for_event.step(node_name, contract, e) for e in emails]
    logging(f"{contract_id} is asking for approvals from {emails}")

    @workflow.step
    def wait_for_approval(contract, *approval_results):
        (contract_id, httpend, approved_list) = contract
        rejected = False
        for (email, result) in approval_results:
            if result == 'REJECTED':
                logging(f"{email} rejected it")
                rejected = True
        if rejected:
            logging(f"{contract_id} is rejected")
            approved_list[node_name] = 'rejected'
        else:
            approved_list[node_name] = 'approved'
            logging(f"{contract_id} is approved by {emails}")
        contract = (contract_id, httpend, approved_list)
        return contract

    return wait_for_approval.step(contract, *approval_requests)

def chain(node_list, upstream):
    wf = workflow.step(node_list[0]).step(upstream)
    for e in node_list[1:]:
        newwf = workflow.step(e).step(wf)
        wf = newwf
    return wf

@workflow.step
def merge(*approved_forms):
    #merging approved_list from all branches
    approved_list = {}
    logging(f"MERGE, {approved_forms}")
    contract_id = approved_forms[0][0][0]
    httpend = approved_forms[0][0][1]
    for i in range(len(approved_forms[0])):
        approved_list = dict(list(approved_list.items()) + list(approved_forms[0][i][2].items()))
    return (contract_id, httpend, approved_list)

@workflow.step
def fork(downstreams, upstream):
    wf = []
    for ds in downstreams:
        if type(ds) == WorkflowStepFunction:
            wf.append(ds.step(upstream))
        else:
            wf.append(workflow.step(ds).step(upstream))
    return gather.step(*wf)

@workflow.step
def gather(*args):
    return args

node_submit_draft = lambda form: eventnode('submission', form, ['self@email'])
node_first_line_mgmt_review = lambda form: eventnode('first_line', form, ['firstline@email','firstlinealt@email'])
node_second_line_mgmt_review = lambda form: eventnode('second_line', form, ['secondline@email','secondlinealt@email'])
node_intellectual_property_review = lambda form: eventnode('intellectual_property', form, ['ipapprover@email'])
node_export_control_review = lambda form: eventnode('export_control', form, ['ecapprover@email'])
node_business_control_review = lambda form: eventnode('business_control', form, ['bcapprover@email'])
node_contract_control_review = lambda form: eventnode('contract_control', form, ['contractapprover@email'])

@workflow.step
def start_contract_approval_process(contract):
    approval = contract_wf.step(contract)

    @workflow.step
    def final_step(approval):
        (contract_id, httpend, approved_list) = approval
        if 'rejected' not in approved_list.values():
            logging(f"Final step: {contract_id} is approved")
            # clean up all the queues in serve
            from ray import serve
            handle = ray.serve.get_deployment(httpend).get_handle(sync=True)
            handle.removeQueues.remote()
            logging(f"removing all queues in serve before exiting")
            return contract_id
        else:
            logging(f"Final step: {contract_id} is rejected. Start a new one.")
            new_contract = updated_contract.step(approval)
            return start_contract_approval_process.step(new_contract)

    return final_step.step(approval)


@workflow.step
def contract_wf(contract):
    return workflow.step(node_contract_control_review).step( \
    workflow.step(node_business_control_review).step( \
    merge.step(fork.step([lambda upstream: chain([node_first_line_mgmt_review, node_second_line_mgmt_review],upstream), \
    lambda upstream: chain([node_intellectual_property_review],upstream), \
    lambda upstream: chain([node_export_control_review],upstream)],contract))))

serve.start(detached=True)

@serve.deployment(name="myEventProvider", route_prefix="/event")
class Approval_Event_Providers():
    def __init__(self):
        import nest_asyncio
        nest_asyncio.apply()
        # storage = "/tmp/ray/workflow_data/"
        # workflow.init()
        self.q = {}

    def __call__(self, request):
        parsed = request.query_params.get("approval")
        if parsed is not None:
            (contract_id, node_name, email, approval_status) = eval(parsed)
            logging(f"{contract_id}.{node_name}.{email} deposited {approval_status}")
            myQueueID = f"{contract_id}.{node_name}.{email}"
            if myQueueID not in self.q.keys():
                self.q[myQueueID] = Queue()
            # [TODO] before putting the (email, approval_status) into the queue
            # do an event checkpointing
            self.q[myQueueID].put((email, approval_status))
            # putting a REJECTED approval_status into pending queues
            # with the same contract_id.  This is to unblock the wait_for_event().
            if approval_status == 'REJECTED':
                for queueID in self.q.keys():
                    if contract_id == queueID.split('.')[0]:
                        email = queueID.split('.')[2]
                        self.q[queueID].put((email, approval_status))
            return True

    async def wait_for_event(self, myQueueID):
        if myQueueID not in self.q.keys():
            self.q[myQueueID] = Queue()
        return await self.q[myQueueID].get_async()

    # def get_or_create_Queue(self, myQueueID):
    #     if myQueueID not in self.q.keys():
    #         self.q[myQueueID] = Queue()
    #     return self.q[myQueueID]

    def removeQueues(self):
        for queueID in self.q.keys():
            self.q[queueID].shutdown()


Approval_Event_Providers.deploy()
#http_endpoint = serve.list_deployments()['Approval_Event_Providers'].url
submitted_form = ('Million-dollar contract', 'myEventProvider', {})
#total_approvers = 7
#final_step.step(start_contract_approval_process.step(submitted_form)).run()
start_contract_approval_process.step(submitted_form).run()


#gather.step(*[contract_wf]).run()
print(Path("/tmp/contract-log").read_text())
