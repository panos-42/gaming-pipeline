#!/usr/bin/env python3
"""
Casino ETL Pipeline

Usage: 
  python casino_etl_simple.py                           # Process all data
  python casino_etl_simple.py 2025-03-10                # Process single date
  python casino_etl_simple.py 2025-03-01 2025-03-10     # Process date range

"""

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta
import sys


def create_target_table(conn):
    """Create target table if it doesn't exist"""
    with conn.cursor() as cur:
        cur.execute("""
            create table if not exists gold_summary (
                id serial primary key,
                date timestamp,
                country varchar(255),
                sex char(1),
                agegroup varchar(20),
                vipstatus varchar(255),
                casinomanufacturername varchar(255),
                casinoprovidername varchar(255),
                ggr_eur float,
                returns_eur float,
                created_at timestamp default current_timestamp,
                updated_at timestamp default current_timestamp,
                -- Add a unique constraint for natural key
                unique(date, country, sex, agegroup, vipstatus, 
                       casinomanufacturername, casinoprovidername, ggr_eur, returns_eur)
            )
        """)
        
        # Create indexes for better Tableau performance
        # Single column indexes for common filters
        cur.execute("create index if not exists idx_gold_summary_date on gold_summary(date)")
        cur.execute("create index if not exists idx_gold_summary_country on gold_summary(country)")
        cur.execute("create index if not exists idx_gold_summary_vipstatus on gold_summary(vipstatus)")
        cur.execute("create index if not exists idx_gold_summary_agegroup on gold_summary(agegroup)")
        cur.execute("create index if not exists idx_gold_summary_manufacturer on gold_summary(casinomanufacturername)")
        cur.execute("create index if not exists idx_gold_summary_provider on gold_summary(casinoprovidername)")
        
        # Composite indexes for common query patterns
        cur.execute("create index if not exists idx_gold_summary_date_country on gold_summary(date, country)")
        cur.execute("create index if not exists idx_gold_summary_date_vip on gold_summary(date, vipstatus)")
        cur.execute("create index if not exists idx_gold_summary_date_age on gold_summary(date, agegroup)")
        
        conn.commit()
    print("Target table ready")


def check_data_exists(conn, process_date):
    """Check if data already exists for the given date"""
    with conn.cursor() as cur:
        cur.execute("""
            select count(*) 
            from gold_summary 
            where date::date = %s::date
        """, (process_date,))
        count = cur.fetchone()[0]
    return count > 0


def extract_data(conn, start_date=None, end_date=None):
    """Extract data from database"""
    if start_date and end_date:
        print(f"Processing date range: {start_date} to {end_date}")
        date_filter = "where cd.date::date >= %s::date and cd.date::date <= %s::date"
        params = (start_date, end_date)
    elif start_date:
        print(f"Processing date: {start_date}")
        date_filter = "where cd.date::date = %s::date"
        params = (start_date,)
    else:
        print("Processing all data")
        date_filter = ""
        params = None
    
    # Single query to get all data with joins
    query = f"""
    with latest_manufacturers as (
        select casinomanufacturerid::int, casinomanufacturername
        from casinomanufacturers
        where latestflag::int = 1
    )
    select 
        cd.date::date as date,
        u.country,
        upper(trim(u.sex)) as sex,
        u.birthdate::date as birthdate,
        trim(u.vipstatus) as vipstatus,
        cd.ggr::numeric as ggr,
        cd.returns::numeric as returns,
        cr.eurorate::numeric as eurorate,
        lm.casinomanufacturername,
        cp.casinoprovidername,
        cd.currencyid::int as currencyid
    from casinodaily cd
    join users u on cd.userid = u.user_id  -- Fixed: userid joins with user_id
    left join latest_manufacturers lm on cd.casinomanufacturerid::int = lm.casinomanufacturerid::int
    left join casinoproviders cp on cd.casinoproviderid::int = cp.casinoproviderid::int
    left join currencyrates cr on cd.date::date = cr.date::date 
        and cd.currencyid::int = cr.tocurrencyid::int  -- Match currency for correct rate
    {date_filter}
    order by cd.date
    """
    
    # Read data
    if params:
        df = pd.read_sql_query(query, conn, params=params)
    else:
        df = pd.read_sql_query(query, conn)
        
    print(f"Extracted {len(df)} records")
    
    return df


def transform_data(df):
    """Transform the extracted data"""
    if df.empty:
        print("No data found to transform")
        return pd.DataFrame()
    
    # Convert date columns to datetime
    df['date'] = pd.to_datetime(df['date'])
    df['birthdate'] = pd.to_datetime(df['birthdate'])
    
    # Calculate age and age groups
    df['age'] = (df['date'] - df['birthdate']).dt.days / 365.25
    df['agegroup'] = pd.cut(
        df['age'],
        bins=[0, 18, 21, 27, 33, 41, 50, 999],
        labels=['Under 18', '18-20', '21-26', '27-32', '33-40', '41-50', '50+'],
        right=False
    )
    
    # Normalize VIP status
    df['vipstatus'] = df['vipstatus'].str.strip().str.upper()  # Normalize first
    
    vip_mapping = {
        'NOT VIP': 'Not VIP',
        'POTENTIAL': 'Potential',
        'BRONZ E': 'Bronze',
        'ELI T E': 'Elite'
    }
    df['vipstatus'] = df['vipstatus'].replace(vip_mapping)
    
    # Calculate EUR values
    df['ggr_eur'] = df['ggr'] * df['eurorate']
    df['returns_eur'] = df['returns'] * df['eurorate']
    
    # Fill missing values
    df['casinomanufacturername'] = df['casinomanufacturername'].fillna('Unknown')
    df['casinoprovidername'] = df['casinoprovidername'].fillna('Unknown')
    df['ggr_eur'] = df['ggr_eur'].fillna(0)
    df['returns_eur'] = df['returns_eur'].fillna(0)
    
    # Select final columns
    final_columns = [
        'date', 'country', 'sex', 'agegroup', 'vipstatus',
        'casinomanufacturername', 'casinoprovidername', 
        'ggr_eur', 'returns_eur'
    ]
    
    print("Transformations complete")
    return df[final_columns]


def load_data(conn, df):
    """Load data into target table with upsert logic"""
    if df.empty:
        return
    
    with conn.cursor() as cur:
        # Convert DataFrame to list of tuples
        records = []
        for _, row in df.iterrows():
            records.append((
                row['date'],
                row['country'],
                row['sex'],
                row['agegroup'],
                row['vipstatus'],
                row['casinomanufacturername'],
                row['casinoprovidername'],
                float(row['ggr_eur']),
                float(row['returns_eur'])
            ))
        
        # Bulk upsert - insert new records, skip duplicates
        execute_values(
            cur,
            """
            insert into gold_summary 
            (date, country, sex, agegroup, vipstatus, 
             casinomanufacturername, casinoprovidername, 
             ggr_eur, returns_eur)
            values %s
            on conflict (date, country, sex, agegroup, vipstatus, 
                        casinomanufacturername, casinoprovidername, 
                        ggr_eur, returns_eur)
            do update set
                updated_at = current_timestamp
            """,
            records,
            page_size=1000
        )
        
        conn.commit()
        print(f"Loaded/Updated {len(records)} records")


def show_summary(conn, start_date=None, end_date=None):
    """Show summary statistics for the processed date or range"""
    if start_date and end_date:
        query = """
        select 
            count(*) as total_records,
            count(distinct country) as countries,
            round(sum(ggr_eur)::numeric, 2) as total_ggr_eur,
            round(sum(returns_eur)::numeric, 2) as total_returns_eur
        from gold_summary
        where date::date >= %s::date and date::date <= %s::date
        """
        params = (start_date, end_date)
    elif start_date:
        query = """
        select 
            count(*) as total_records,
            count(distinct country) as countries,
            round(sum(ggr_eur)::numeric, 2) as total_ggr_eur,
            round(sum(returns_eur)::numeric, 2) as total_returns_eur
        from gold_summary
        where date::date = %s::date
        """
        params = (start_date,)
    else:
        query = """
        select 
            count(*) as total_records,
            count(distinct country) as countries,
            round(sum(ggr_eur)::numeric, 2) as total_ggr_eur,
            round(sum(returns_eur)::numeric, 2) as total_returns_eur
        from gold_summary
        """
        params = None
    
    with conn.cursor() as cur:
        if params:
            cur.execute(query, params)
        else:
            cur.execute(query)
        result = cur.fetchone()
        
    print("\nSummary Statistics:")
    if start_date and end_date:
        print(f"   Date Range: {start_date} to {end_date}")
    elif start_date:
        print(f"   Date: {start_date}")
    else:
        print(f"   All Data")
    print(f"   Total Records: {result[0]:,}")
    print(f"   Countries: {result[1]}")
    print(f"   Total GGR (EUR): {result[2]:,.2f}")
    print(f"   Total Returns (EUR): {result[3]:,.2f}")


def main():
    """Main ETL function"""
    # Parse command line arguments
    start_date = None
    end_date = None
    
    if len(sys.argv) == 2:
        # Single date provided
        start_date = sys.argv[1]
        try:
            datetime.strptime(start_date, '%Y-%m-%d')
        except ValueError:
            print("Error: Invalid date format. Use YYYY-MM-DD")
            sys.exit(1)
    elif len(sys.argv) == 3:
        # Date range provided
        start_date = sys.argv[1]
        end_date = sys.argv[2]
        try:
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            if start_dt > end_dt:
                print("Error: Start date must be before or equal to end date")
                sys.exit(1)
        except ValueError:
            print("Error: Invalid date format. Use YYYY-MM-DD")
            sys.exit(1)
    elif len(sys.argv) > 3:
        print("Error: Too many arguments. Usage: python casino_etl_simple.py [start_date] [end_date]")
        sys.exit(1)
    
    print(f"\nCasino ETL Pipeline - Simplified Version")
    print(f"{'='*50}")
    
    # Update this connection string with your database details
    CONNECTION_STRING = "postgresql://postgres:evangelion42@localhost:5432/postgres"
    
    conn = None
    try:
        # Connect to database
        conn = psycopg2.connect(CONNECTION_STRING)
        print("Connected to database")
        
        # Create target table
        create_target_table(conn)
        
        # Extract data
        df = extract_data(conn, start_date, end_date)
        
        # Transform data
        df_transformed = transform_data(df)
        
        # Load data
        if not df_transformed.empty:
            load_data(conn, df_transformed)
            show_summary(conn, start_date, end_date)
        else:
            print("No data found to process")
        
        print(f"\nETL completed successfully!")
        
    except Exception as e:
        print(f"\nETL failed: {e}")
        sys.exit(1)
        
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()