import pandas as pd
import sqlite3
import sqlalchemy
conn = sqlite3.connect('\Users\noahz\Downloads\fda_purple_orange_books.csv')
def read_data() -> pd.DataFrame:
    """Read in the data from the SQL database"""
    engine = sqlalchemy.create_engine(
        "sqlite:////Users\noahz\Downloads\fda_purple_orange_books.csv")

    # Have to account for SQLAlchemy v2
    with engine.connect() as conn:
        df = pd.read_sql(sqlalchemy.text(
            'SELECT id, name FROM companies'), conn)
    return df
cursor = conn.cursor()
cursor.execute('''
    CREATE TABLE
  IF NOT EXISTS fda_purple_orange(
    id INTEGER PRIMARY KEY,
    position VARCHAR(250) NOT NULL,
    zip INT
  )
CREATE TABLE
  IF NOT EXISTS company(
    id INTEGER PRIMARY KEY,
    id_numbers VARCHAR(250) NOT NULL,
    applicant VARCHAR(250) NOT NULL,
    propritary_name VARCHAR(250) NOT NULL,
    ZIP INT
  )
  CREATE TABLE drug(
    id INTEGER PRIMARY KEY, 
  
  CREATE TABLE
  SELECT DISTINCT applicant
  FROM company
  ON CONFLICT DO NOTHING ''')
def read_data() -> pd.DataFrame:
    """Read in the data from the SQL database"""
    conn = sqlalchemy.create_engine(
        "sqlite:////Users\noahz\Downloads\fda_purple_orange_books.csv")

    # Have to account for SQLAlchemy v2
    with conn.connect() as conn:
        df = pd.read_sql(sqlalchemy.text(
            'SELECT id, name FROM companies'), conn)
    return df


def dedup(df: pd.DataFrame) -> pd.DataFrame:
    """Deduplicate companies, creating a bridge table
    Args:
        df (pd.DataFrame): company table
    Returns:
        pd.DataFrame: bridge table
    """
    out = []
    for i, row in df.iterrows():
        for _, row2 in df.iloc[i+1:].iterrows():
            if row['name'] in row2['name']:
                out.append(row['id'], row2['id'])
            elif row2['name'] in row['name']:
                out.append(row2['id'], row['id'])
    return pd.DataFrame(out, columns=['parent_company_id', 'child_company_id'])


def dedup_dedup(co_df: pd.DataFrame, bridge_df: pd.DataFrame) -> pd.DataFrame:
    def sort_apply():
        pass

    comb = bridge_df.merge(co_df[['id', 'name']].rename(
        columns={'name': 'parent_name'}), how='left', left_on='parent_company_id', right_on='id')
    comb = comb.merge(co_df[['id', 'name']].rename(
        columns={'name': 'child_name'}), how='left', left_on='child_company_id', right_on='id')

    comb['keep'] = comb.groupby('child_id').apply(sort_apply, axis=1)


def send_dedup_to_sql(df: pd.DataFrame):
    """Send the dedup dataframe to sql
    Args:
        df (pd.DataFrame): bridge table
    """
    engine = sqlalchemy.create_engine(
        'sqlite:////Users/arthur/teaching/duq_ds3_2023/data/live_test_sqlite.db')

    # Have to account for SQLAlchemy v2
    with engine.connect() as conn:
        df[['parent_company_id', 'child_company_id']].to_sql(
            'matched_company', conn, index=False, if_exists='append')