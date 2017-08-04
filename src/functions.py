from datetime import datetime
import math

class shutterfly:
    
    # pre-sort the data source to enable output message for unususal event cases 
    @staticmethod
    def presort(data):
        return sorted(data, key = lambda v: (v['type'], v['verb'], v['event_time']) )      
        
    @staticmethod   
    def ingest(event, res):
        if event['type'] =='CUSTOMER':
            # 
            if event['verb'] == 'NEW':
                res[event['key']] = {'last_name': event['last_name'], 'adr_city': event['adr_city'], 'adr_state': event['adr_state'],
                                            'start_date': event['event_time'], 'end_date': event['event_time'], 'visits': 1, 'spending': 0, 'order_dict': {}}
            elif event['verb'] == 'UPDATE':
                if event['key'] not in res:
                    res[event['key']] = {'last_name': event['last_name'], 'adr_city': event['adr_city'], 'adr_state': event['adr_state'],
                                            'start_date': event['event_time'], 'end_date': event['event_time'], 'visits': 1, 'spending': 0, 'order_dict': {}}
                    print 'Error, the Customer update does not have a previous record', "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n", event, "\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n"
                else:
                    res[event['key']] = {'last_name': event['last_name'], 'adr_city': event['adr_city'], 'adr_state': event['adr_state'],
                                                'start_date': res[event['key']]['start_date'], 'end_date': event['event_time'], 
                                                'visits': res[event['key']]['visits']+1, 'spending': 0, 'order_dict': {}}
        elif event['type'] == 'IMAGE':
            if event['customer_id'] not in res:
                res[event['customer_id']] = {'start_date': event['event_time'], 'end_date': event['event_time'], 'visits': 1, 'spending': 0, 'order_dict': {}}
                print 'Error, the customer upload image without previous customer event'
            else:
                res[event['customer_id']]['start_date'] = event['event_time'] if event['event_time'] < res[event['customer_id']]['start_date'] \
                                                                            else res[event['customer_id']]['start_date']
                res[event['customer_id']]['end_date'] = event['event_time'] if event['event_time'] > res[event['customer_id']]['end_date'] \
                                                                            else res[event['customer_id']]['end_date']
                res[event['customer_id']]['visits'] += 1
        elif event['type'] == 'SITE_VISIT':
            if event['customer_id'] not in res:
                res[event['customer_id']] = {'start_date': event['event_time'], 'end_date': event['event_time'], 'visits': 1, 'spending': 0, 'order_dict': {}}
                print 'error, the customer site visit event without previous customer event', "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n", event, "\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n"
            else:
                res[event['customer_id']]['start_date'] = event['event_time'] if event['event_time'] < res[event['customer_id']]['start_date'] \
                                                                            else res[event['customer_id']]['start_date']
                res[event['customer_id']]['end_date'] = event['event_time'] if event['event_time'] > res[event['customer_id']]['end_date'] \
                                                                            else res[event['customer_id']]['end_date']
                res[event['customer_id']]['visits'] += 1
        elif event['type'] == 'ORDER':
            if event['customer_id'] not in res: # no customer's previous information
                res[event['customer_id']] = {'start_date': event['event_time'], 'end_date': event['event_time'], 'visits': 1, 'spending': float(event['total_amount'].split()[0]), 
                                                            'order_dict': {event['key']: float(event['total_amount'].split()[0])}}
                print 'Error, order event without previous event \n', "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n", event, "\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n"
            else: # if there is customer's previous information
                if event['verb'] == 'NEW':
                    if event['key'] not  in res[event['customer_id']]['order_dict']: # regular situation
                        res[event['customer_id']]['order_dict'][event['key']] = float(event['total_amount'].split()[0])
                        res[event['customer_id']]['spending'] += float(event['total_amount'].split()[0])
                        res[event['customer_id']]['visits'] += 1
                        res[event['customer_id']]['start_date'] = event['event_time'] if event['event_time'] < res[event['customer_id']]['start_date'] \
                                                                            else res[event['customer_id']]['start_date']
                        res[event['customer_id']]['end_date'] = event['event_time'] if event['event_time'] > res[event['customer_id']]['end_date'] \
                                                                            else res[event['customer_id']]['end_date']
                    else:
                        print 'Error, duplicates new orders with the same order id \n', "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n", event, "\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n"
    
                elif event['verb'] == 'UPDATE':
                    if event['key'] not  in res[event['customer_id']]['order_dict']:
                        res[event['customer_id']]['order_dict'][event['key']] = float(event['total_amount'].split()[0])
                        res[event['customer_id']]['spending'] += float(event['total_amount'].split()[0])
                        res[event['customer_id']]['visits'] += 1
                        res[event['customer_id']]['start_date'] = event['event_time'] if event['event_time'] < res[event['customer_id']]['start_date'] \
                                                                            else res[event['customer_id']]['start_date']
                        res[event['customer_id']]['end_date'] = event['event_time'] if event['event_time'] > res[event['customer_id']]['end_date'] \
                                                                            else res[event['customer_id']]['end_date']
                        print "Error, update order without previous event \n", "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n", event, "\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n"
                    else: # regular situation
                        res[event['customer_id']]['spending'] = res[event['customer_id']]['spending'] -res[event['customer_id']]['order_dict'][event['key']]+ float(event['total_amount'].split()[0])
                        res[event['customer_id']]['order_dict'][event['key']] = float(event['total_amount'].split()[0])
                        res[event['customer_id']]['visits'] += 1
                        res[event['customer_id']]['start_date'] = event['event_time'] if event['event_time'] < res[event['customer_id']]['start_date'] \
                                                                            else res[event['customer_id']]['start_date']
                        res[event['customer_id']]['end_date'] = event['event_time'] if event['event_time'] > res[event['customer_id']]['end_date'] \
                                                                            else res[event['customer_id']]['end_date']
        else:
            print "Error, unknow event type \n", "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n", event, "\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n"
                    
    
                    
    # ref:  https://stackoverflow.com/questions/14191832/how-to-calculate-difference-between-two-dates-in-weeks-in-python                                
    def weeks(self, d1, d2):
        datetime.strptime(d1, '%Y-%m-%d:%H:%M:%S.%fZ')
        start = datetime.strptime(d1, '%Y-%m-%d:%H:%M:%S.%fZ')
        end = datetime.strptime(d2, '%Y-%m-%d:%H:%M:%S.%fZ')
        return int(math.ceil((end-start).days / 7.0))
        
    # to add other keys and values
    def finalize(self, res):
        for key in res.keys():
            res[key]['weeks'] = max(1, self.weeks(res[key]['start_date'], res[key]['end_date']))
            res[key]['ce_per_visit'] = res[key]['spending'] / res[key]['visits']
            res[key]['sv_per_week'] = res[key]['visits'] / res[key]['weeks']
            lifespan = 10
            res[key]['LTV'] = 52 *  (res[key]['ce_per_visit'] * res[key]['sv_per_week']) * lifespan
            
    # Return the top x customers with the highest Simple Lifetime Value       
    @staticmethod
    def topXSimpleLTVCustomers(x, res):
        if x > len(res):
            if len(res)>1:
                print 'Since there are only %d customers....' % (len(res))
            else:
                print 'Since there is only %d customer....' % (len(res))
        x = min(x, len(res))
        pairs = sorted(res.iteritems(), key = lambda (k, v): -v['LTV'])[:x] 
        return [item[0] for item in pairs]   