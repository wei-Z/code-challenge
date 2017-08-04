import collections
import json
from functions import shutterfly
import os
"""
data structure that will be used
{
customer_id (string): {
                                    last_name: string, customer's last name
                                    adr_city: string, customer's city location
                                    adr_state: string, customer's state location
                                    start_date: string, customer's first event time
                                    end_date: string, customer's last event time
                                    weeks: int, weeks count based on duration from start_date to end_date
                                    visits: int, visits count
                                    order_list: {}, order_id as key, total_amount in float as value
                                    spending: float, accumulated order total amount
                                    ce_per_visit: int, spending / visits
                                    sv_per_week: int, visits / weeks
                                    LTV: float, 52*(ce_per_visit * sv_per_week)* lifespan, where lifespan = 10 in our case
                                   }
}
"""
# initiate a data structure to store results
res = collections.defaultdict(dict)
directory = "../input/"
for filename in os.listdir(directory):
    if filename.endswith(".txt"):
        # print filename
        with open(os.path.join(directory, filename), 'r') as f:
            data = json.load(f)
            print data
            # presort the data with type, verb and event_time
            sorteddata = shutterfly.presort(data)
            # ingest each event into result data
            for event in sorteddata:
                shutterfly.ingest(event, res)
                
# finalize the result by creating weeks, customer expenditures per visit(ce_per_visit), number of site visits per week(sv_per_week)  
# then calculate Customer Lifetime Value for each customer    
shutterfly().finalize(res)
# return the top x customer id with the highest Simple Lifetime Value from result
x = 3 # for instance
topX = shutterfly.topXSimpleLTVCustomers(x, res)

# write result into output
output = open('../output/output.txt', 'w')
output.write('The resulted data structure is as follows: \n')
output.write(json.dumps(res))
output.write('\n')
output.write('___________________________________________________________\n')
output.write('The top %d customers with the highest Simple Lifetime Value are: \n' % x)
for item in topX:
    output.write('%s\n' % item)
output.close()
                                

