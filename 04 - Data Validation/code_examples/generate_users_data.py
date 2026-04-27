import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# Generate synthetic users data
np.random.seed(42)
n = 200

df = pd.DataFrame(
    {
        'user_id': range(1, n + 1),
        'name': ['User_' + str(i) for i in range(1, n + 1)],
        'email': ['user' + str(i) + '@email.com' for i in range(1, n + 1)],
        'age': np.random.randint(18, 80, n),
        'signup_date': pd.date_range(start='2022-01-01', periods=n, freq='D'),
        'country': np.random.choice(['USA', 'UK', 'Canada', 'Australia', 'Germany'], n),
    }
)

# age:
# np.random.randint(low, high, size), high is exclusive

# pd.date_range():
# generates a fixed-frequency sequence of dates/timestamps.
# Think of it as Python's range() but for dates.

# signup_date:
# pd.date_range(start, periods, freq) generates a sequence of n dates
# beginning at 2022-01-01, spaced 1 Day apart (freq='D')

# country:
# np.random.choice(array, size) randomly picks n values from the provided list,
# with equal probability for each country


# Intentionally inject some issues to make validation interesting
df.loc[5, 'email'] = None  # missing email
df.loc[10, 'age'] = -5  # invalid age
df.loc[15, 'country'] = 'Egypt'  # invalid country
df.loc[1, 'user_id'] = 1  # duplicate user_id

df.to_csv('users.csv', index=False)
# index=False means the DataFrame's row index (0, 1, 2...) is not written as a column in the file.

print(df.dtypes)
# `df.dtypes` is a **Series attribute** (not a method) that returns the data type of each column.
