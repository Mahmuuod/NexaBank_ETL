from ValidationCheck.SchemaCheck import SchemaCheck

class CustomerCheck(SchemaCheck):

    def check(self):
        if self.df.empty:
            print("DataFrame is empty")
            return False
        
        expected_columns = ['customer_id', 'name' , 'gender' , 'age', 'city', 'account_open_date', 'product_type', 'customer_tier']
        if not all(col in self.df.columns for col in expected_columns):
            print("DataFrame does not have the expected columns")
            return False
        
        if len(self.df) < 1:
            print("DataFrame does not have the expected number of rows")
            return False
        expected_dtypes = {
            'customer_id': 'int64',
            'name': 'object',
            'gender': 'object',
            'age': 'int64',
            'city': 'object',
            'account_open_date': 'datetime64[ns]',
            'product_type': 'object',
            'customer_tier': 'object'
        }
        for col, dtype in expected_dtypes.items():
            if self.df[col].dtype != dtype:
                print(f"DataFrame column {col} does not have the expected data type {dtype}")
                return False
        return super().check()