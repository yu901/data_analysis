
import pm4py as pm
from src.main.python.utils import *

class Process():
    def __init__(self, data, case, activity, timestamp):
        self.case = case
        self.activity = activity
        self.timestamp = timestamp
        self.attrs = list(set(data.columns) - {case, activity, timestamp})
        self.data = self.sort_timestamp(data)
        self.xes = self.get_xes()
        self.time_unit = 'ms'
        
    def sort_timestamp(self, df):
        df = df.sort_values(self.timestamp).reset_index(drop=True)
        return df

    def get_xes(self):
        xes = pm.format_dataframe(self.data, case_id=self.case, activity_key=self.activity, timestamp_key=self.timestamp)
        return xes
    
    def set_timeunit(self, time_unit):
        self.time_unit = time_unit
    
    # case attribute
    def get_caseattrs(self):
        df = self.data.copy()
        df = df.groupby(self.case)[self.attrs].value_counts()
        df.name = 'activity_count'
        df = df.reset_index()
        return df

    # lead time
    def get_casetime(self):
        df = self.data.copy()
        df = df.groupby(self.case)[self.timestamp].agg(['min', 'max'])
        df.columns = ['case_start', 'case_end']
        df['leadtime'] = (df['case_end'] - df['case_start']) / np.timedelta64(1, self.time_unit)
        df = df.reset_index()
        return df
    
    # varient
    def get_varient(self):
        df = self.data.copy()
        df = df.groupby(self.case)[self.activity].apply('>'.join)
        df.name = 'trace'
        df = df.reset_index()
        return df
    
    def get_pathtable(self):
        df = self.data.copy()
        df['to_activity'] = df[self.activity]
        df['to_timestamp'] = df[self.timestamp]
        df['from_activity'] = df.groupby(self.case)[self.activity].shift(1)
        df['from_timestamp'] = df.groupby(self.case)[self.timestamp].shift(1)
        df['duration'] = (df['to_timestamp'] - df['from_timestamp']) / np.timedelta64(1, self.time_unit)
        df = df.dropna(subset = ['from_activity'])
        df['path'] = df['from_activity'] + '>' + df['to_activity']
        df = df[['path', 'from_activity', 'to_activity', 'from_timestamp', 'to_timestamp', 'duration']]
        return df
    
    def get_casetable(self):
        caseattrs = self.get_caseattrs()
        casetime = self.get_casetime()
        varient = self.get_varient()
        df = pd.merge(caseattrs, casetime, on=self.case)
        df = pd.merge(df, varient, on=self.case)
        return df

    def save_bpmn(self, file_path):
        # BPMN 모델
        process_tree = pm.discover_process_tree_inductive(self.xes)
        bpmn_model = pm.convert_to_bpmn(process_tree)
        pm.view_bpmn(bpmn_model)
        pm.save_vis_bpmn(bpmn_model, file_path)
        print(f'{file_path} is saved.')

    def save_dfg(self, file_path):
        # process map 모델
        dfg, start_activities, end_activities = pm.discover_dfg(self.xes)
        pm.save_vis_dfg(dfg, start_activities, end_activities, file_path)
        pm.view_dfg(dfg, start_activities, end_activities)
        print(f'{file_path} is saved.')


if __name__ == "__main__":
    data = load_csv('./data/Insurance_claims_event_log.csv', timestamp_cols=['timestamp'])
    case = 'case_id'
    activity = 'activity_name'
    timestamp = 'timestamp'
    process = Process(data, case, activity, timestamp)
    process.set_timeunit('h')
    path_table = process.get_pathtable()
    print(path_table.head(20))
    # process.save_bpmn('./result/bpmn.png')
    # process.save_dfg('./result/dfg.png')