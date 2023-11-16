
from utils import *

class Process():
    def __init__(self, data, case, activity, timestamp):
        self.case = case
        self.activity = activity
        self.timestamp = timestamp
        self.attrs = list(set(data.columns) - {case, activity, timestamp})
        self.data = self.sort_timestamp(data)
        
    def sort_timestamp(self, df):
        df = df.sort_values(self.timestamp).reset_index(drop=True)
        return df

    # case attribute
    def get_caseattrs(self):
        df = self.data.copy()
        df = df.groupby(self.case)[self.attrs].value_counts()
        df.name = 'activity_count'
        df = df.reset_index()
        return df

    # lead time
    def get_casetime(self, time_unit='D'):
        df = self.data.copy()
        df = df.groupby(self.case)[self.timestamp].agg(['min', 'max'])
        df.columns = ['case_start', 'case_end']
        df['leadtime'] = (df['case_end'] - df['case_start']) / np.timedelta64(1, time_unit)
        df = df.reset_index()
        return df
    
    # varient
    def get_varient(self):
        df = self.data.copy()
        df = df.groupby(self.case)[self.activity].apply('>'.join)
        df.name = 'trace'
        df = df.reset_index()
        return df
    
    def get_casetable(self):
        caseattrs = self.get_caseattrs()
        casetime = self.get_casetime()
        varient = self.get_varient()
        df = pd.merge(caseattrs, casetime, on=self.case)
        df = pd.merge(df, varient, on=self.case)
        return df


if __name__ == "__main__":
    data = load_csv('./data/Insurance_claims_event_log.csv', timestamp_cols=['timestamp'])
    case = 'case_id'
    activity = 'activity_name'
    timestamp = 'timestamp'
    process = Process(data, case, activity, timestamp)
    casetable = process.get_casetable()
    print('casetable:\n', casetable.head(3))