import pandas as pd

data = {
    'bill_id': ['BILL6651468', 'BILL8554086', 'BILL7781614', 'BILL8062854', 'BILL9167319'],
    'customer_id': ['CUST000001', 'CUST000001', 'CUST000002', 'CUST000002', 'CUST000003'],
    'month': ['2023-01', '2023-02', '2023-01', '2023-02', '2023-01'],
    'amount_due': [270.37, 190.41, 126.54, 254.34, 54.91],
    'amount_paid': [270.37, 165.16, 126.54, 254.34, 54.91],
    'payment_date': ['2023-01-01', '2023-02-08', '2023-01-01', '2023-02-01', '2023-01-01']
}

billing_df = pd.DataFrame(data)
print("=== Original DataFrame ===")
print(billing_df)


class Transformer:
    def __init__(self):
        pass
    
    def transform_billing_fully_paid(self, dataframe):

        dataframe['fully_paid'] = dataframe['amount_due'] == dataframe['amount_paid']
        dataframe['payment_status'] = dataframe['fully_paid'].apply(lambda x: 'Paid' if x == True else 'Unpaid')
        dataframe.drop(columns=['fully_paid'], inplace=True)
        print(dataframe)
        return dataframe
    
    def transform_billing_dept(self, dataframe):
        dataframe['debt'] = dataframe['amount_due'] - dataframe['amount_paid']
        print(dataframe)
        return dataframe
    
    def transform_billing_late_days(self, dataframe):
        days = dataframe["payment_date"].str.split("-").str[2].astype(int)
        dataframe['late_days'] = (days - 1)
        dataframe['fine'] = dataframe['late_days'] * 5.15
        dataframe['fine'] = dataframe['fine'].round(2)
        print(dataframe)
        return dataframe
    
    def transform_billing_total(self, dataframe):
        dataframe['total'] = dataframe['amount_due'] + dataframe['fine']
        print(dataframe)
        return dataframe

transformer = Transformer()
transformed_df = transformer.transform_billing_late_days(billing_df)