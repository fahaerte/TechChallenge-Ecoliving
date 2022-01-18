from Constant import DATABASE_NAME, TABLE_NAME, ONE_GB_IN_BYTES

class Querymanager:

    def __init__(self, client):
        self.client = client
        self.paginator = client.get_paginator('query')


    def get_all_data(self):
        
        # See records ingested into this table so far
        SELECT_CO2 = "SELECT time, measure_value::bigint as CO2_level FROM {DATABASE_NAME}.{TABLE_NAME} WHERE measure_name='CO2_level' ORDER BY time DESC".format(DATABASE_NAME=DATABASE_NAME, TABLE_NAME=TABLE_NAME)
        SELECT_Hum = "SELECT time, measure_value::double as humidity FROM {DATABASE_NAME}.{TABLE_NAME} WHERE measure_name='humidity' ORDER BY time DESC".format(DATABASE_NAME=DATABASE_NAME, TABLE_NAME=TABLE_NAME)
        SELECT_Temp = "SELECT time, measure_value::double as temperature FROM {DATABASE_NAME}.{TABLE_NAME} WHERE measure_name='temperature' ORDER BY time DESC".format(DATABASE_NAME=DATABASE_NAME, TABLE_NAME=TABLE_NAME)
        
        data_co2 = self.run_query(SELECT_CO2)[0]
        data_hum = self.run_query(SELECT_Hum)[0]
        data_temp = self.run_query(SELECT_Temp)[0]
        data = []
        
        for i in range(len(data_co2)):
            data.append(self._merge_dicts(data_co2[i], data_hum[i], data_temp[i]))
        
        return data
        
    def get_last_entry(self):
        SELECT_last_CO2 = "SELECT time, measure_value::bigint as CO2_level FROM {DATABASE_NAME}.{TABLE_NAME} WHERE measure_name='CO2_level' ORDER BY time DESC LIMIT 1".format(DATABASE_NAME=DATABASE_NAME, TABLE_NAME=TABLE_NAME)
        SELECT_last_Hum = "SELECT time, measure_value::double as humidity FROM {DATABASE_NAME}.{TABLE_NAME} WHERE measure_name='humidity' ORDER BY time DESC LIMIT 1".format(DATABASE_NAME=DATABASE_NAME, TABLE_NAME=TABLE_NAME)
        SELECT_last_Temp = "SELECT time, measure_value::double as temperature FROM {DATABASE_NAME}.{TABLE_NAME} WHERE measure_name='temperature' ORDER BY time DESC LIMIT 1".format(DATABASE_NAME=DATABASE_NAME, TABLE_NAME=TABLE_NAME)
        
        data_co2 = self.run_query(SELECT_last_CO2)[0][0]
        data_hum = self.run_query(SELECT_last_Hum)[0][0]
        data_temp = self.run_query(SELECT_last_Temp)[0][0]
        
        # Merge dicts
        data = self._merge_dicts(data_co2, data_hum, data_temp)
        
        return data

    def run_query(self, query_string):
        print("Running query: [%s]" % (query_string))
        data = []
        try:
            page_iterator = self.paginator.paginate(QueryString=query_string)
            for page in page_iterator:
                data.append(self._parse_query_result(page))
        except Exception as err:
            print("Exception while running query:", err)
        
        return data
            
    
    # Returns query_result as dataframe
    def _parse_query_result(self, query_result):
        column_info = query_result['ColumnInfo']

        print("Metadata: %s" % column_info)
        
        query_data = []
        for row in query_result['Rows']:
            data = self._parse_row(column_info, row)
            query_data.append(data)
        
        return query_data
            

    def _parse_row(self, column_info, row):
        row_data = row['Data']
        row_output = {}
        for j in range(len(row_data)):
            data = row_data[j]['ScalarValue']
            measure_name = str(column_info[j]['Name'])
            row_output[measure_name] = str(data)

        return row_output
        
    def _merge_dicts(self, d1, d2, d3):
        res = d1.copy()   # start with keys and values of x
        res.update(d2) # modifies z with keys and values of y
        res.update(d3) 
        return res
