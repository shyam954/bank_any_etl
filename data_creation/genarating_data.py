from faker import Faker
import pandas as pd
import random

fake = Faker()
Faker.seed(42)

def create_customers(num_customers=1000):
    customers = []
    branch_codes = ['B001', 'B002', 'B003']

    for i in range(1, num_customers + 1):
        customers.append({
            'customer_id': 1000 + i,
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'date_of_birth': fake.date_of_birth(minimum_age=18, maximum_age=90).strftime('%Y-%m-%d'),
            'email': fake.unique.email(),
            'account_open_date': fake.date_between(start_date='-10y', end_date='today').strftime('%Y-%m-%d'),
            'kyc_status': random.choice(['VERIFIED', 'PENDING']),
            'branch_code': random.choice(branch_codes)
        })
    return pd.DataFrame(customers)

customers_df = create_customers(1000)
customers_df.to_csv('customers.csv', index=False)


def create_transactions(customers, num_txns=5000):
    txns = []
    txn_types = ['DEPOSIT', 'WITHDRAWAL', 'TRANSFER']
    statuses = ['COMPLETED', 'FAILED', 'PENDING']
    for i in range(num_txns):
        cust = customers.sample(1).iloc[0]
        txns.append({
            'txn_id': f'TXN{str(i+1).zfill(5)}',
            'customer_id': cust['customer_id'],
            'txn_timestamp': fake.date_time_between(start_date='-365d', end_date='now').strftime('%Y-%m-%d %H:%M:%S'),
            'txn_amount': round(random.uniform(100, 10000), 2),
            'txn_type': random.choice(txn_types),
            'device_id': fake.uuid4(),
            'location': fake.city() + ', ' + fake.country_code(),
            'status': random.choice(statuses)
        })
    return pd.DataFrame(txns)

transactions_df = create_transactions(customers_df, 5000)
transactions_df.to_csv('transactions.csv', index=False)


def create_loans(customers, num_loans=500):
    loans = []
    statuses = ['ACTIVE', 'CLOSED', 'DEFAULTED']
    for i in range(num_loans):
        cust = customers.sample(1).iloc[0]
        loans.append({
            'loan_id': f'L{str(i+1).zfill(4)}',
            'customer_id': cust['customer_id'],
            'opening_date': fake.date_between(start_date='-5y', end_date='today').strftime('%Y-%m-%d'),
            'principal_amount': random.randint(50000, 1000000),
            'interest_rate': round(random.uniform(6.5, 15.0), 2),
            'status': random.choice(statuses)
        })
    return pd.DataFrame(loans)

loans_df = create_loans(customers_df, 500)
loans_df.to_csv('loans.csv', index=False)