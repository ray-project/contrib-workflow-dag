import requests
from time import sleep

reply = dict()
# reply[0] = "('Million-dollar contract', 'first_line', 'firstline@email', 'APPROVED')"
# reply[1] = "('Million-dollar contract', 'first_line', 'firstlinealt@email', 'APPROVED')"
# reply[2] = "('Million-dollar contract', 'second_line', 'secondline@email', 'APPROVED')"
# reply[3] = "('Million-dollar contract', 'second_line', 'secondlinealt@email', 'APPROVED')"
# reply[4] = "('Million-dollar contract', 'export_control', 'ecapprover@email', 'APPROVED')"
# reply[5] = "('Million-dollar contract', 'intellectual_property', 'ipapprover@email', 'APPROVED')"
# reply[6] = "('Million-dollar contract', 'business_control', 'bcapprover@email', 'APPROVED')"
# reply[7] = "('Million-dollar contract', 'contract_control', 'contractapprover@email', 'APPROVED')"
reply[0] = "('Million-dollar contract', 'first_line', 'firstline@email', 'APPROVED')"
reply[1] = "('Million-dollar contract', 'first_line', 'firstlinealt@email', 'APPROVED')"
reply[2] = "('Million-dollar contract', 'second_line', 'secondline@email', 'APPROVED')"
reply[3] = "('Million-dollar contract', 'second_line', 'secondlinealt@email', 'APPROVED')"
reply[4] = "('Million-dollar contract', 'export_control', 'ecapprover@email', 'REJECTED')"
reply[5] = "('[new]Million-dollar contract', 'first_line', 'firstline@email', 'APPROVED')"
reply[6] = "('[new]Million-dollar contract', 'first_line', 'firstlinealt@email', 'REJECTED')"
reply[7] = "('[new]Million-dollar contract', 'export_control', 'ecapprover@email', 'APPROVED')"
reply[8] = "('[new][new]Million-dollar contract', 'first_line', 'firstline@email', 'APPROVED')"
reply[9] = "('[new][new]Million-dollar contract', 'first_line', 'firstlinealt@email', 'APPROVED')"
reply[10] = "('[new][new]Million-dollar contract', 'second_line', 'secondline@email', 'APPROVED')"
reply[11] = "('[new][new]Million-dollar contract', 'second_line', 'secondlinealt@email', 'APPROVED')"
reply[12] = "('[new][new]Million-dollar contract', 'export_control', 'ecapprover@email', 'APPROVED')"
reply[13] = "('[new][new]Million-dollar contract', 'intellectual_property', 'ipapprover@email', 'APPROVED')"
reply[14] = "('[new][new]Million-dollar contract', 'business_control', 'bcapprover@email', 'APPROVED')"
reply[15] = "('[new][new]Million-dollar contract', 'contract_control', 'contractapprover@email', 'APPROVED')"



for i in range(len(reply.keys())):
    resp = requests.get("http://127.0.0.1:8000/event", params={"approval":reply[i]})
    print(resp.text)
    sleep(4)
