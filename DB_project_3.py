import random
import datetime
import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text

# 1. Generate Date Dimension
def generate_date_dimension(start_date, end_date):
    rows = []
    dateid_counter = 1
    
    current = start_date
    while current <= end_date:
        dayofweek = current.weekday()  # Monday=0, Sunday=6
        rows.append({
            "dateid": dateid_counter,
            "fulldate": current,
            "year": current.year,
            "month": current.month,
            "dayofmonth": current.day,
            "dayofweek": dayofweek,
            "hourofday": None,
            "isweekend": 'Y' if dayofweek in (5, 6) else 'N',
            "week_of_year": int(current.strftime("%W"))  # 0..53
        })
        dateid_counter += 1
        current += datetime.timedelta(days=1)
    
    return pd.DataFrame(rows)

# 2. Generate Cinema Dimension
def generate_cinema_dimension(n_cinemas=50):
    states = ["Riyadh", "Jeddah", "Dubai", "Abu Dhabi", "Cairo"]
    # Use 'Medium' if your DB constraint requires it
    hall_sizes = ["Small", "Medium", "Large"]
    
    rows = []
    for i in range(1, n_cinemas + 1):
        city = random.choice(states)
        rows.append({
            "cinemaid": i,
            "name": f"Cinema_{i}",
            "address": f"Address_{i}",
            "state": f"State_{random.randint(1,5)}",
            "hallsize": random.choice(hall_sizes),
            "city": city
        })
    return pd.DataFrame(rows)

# 3. Generate Customer Dimension
def generate_customer_dimension(n_customers=100000):
    genders = ["M", "F"]
    rows = []
    for i in range(1, n_customers + 1):
        dob_year = random.randint(1950, 2010)
        dob_month = random.randint(1, 12)
        dob_day = random.randint(1, 28)
        dob = datetime.date(dob_year, dob_month, dob_day)
        
        rows.append({
            "customerid": i,
            "name": f"Customer_{i}",
            "dob": dob,
            "gender": random.choice(genders),
            "address": f"CustomerAddress_{i}"
        })
    return pd.DataFrame(rows)

# 4. Generate Movie Dimension
def generate_movie_dimension(n_movies=1000):
    genres = ["Action", "Comedy", "Drama", "Sci-Fi", "Horror"]
    directors = ["Director_A", "Director_B", "Director_C"]
    stars = ["Star_X", "Star_Y", "Star_Z"]
    
    rows = []
    for i in range(1, n_movies + 1):
        release_year = random.randint(2000, 2024)
        release_month = random.randint(1, 12)
        release_day = random.randint(1, 28)
        release_date = datetime.date(release_year, release_month, release_day)
        
        rows.append({
            "movieid": i,
            "title": f"Movie_{i}",
            "genre": random.choice(genres),
            "director": random.choice(directors),
            "star": random.choice(stars),
            "releasedate": release_date
        })
    return pd.DataFrame(rows)

# 5. Generate Promotion Dimension
def generate_promotion_dimension():
    data = [
        (1, "Discount 10%", 10.00, datetime.date(2022,1,1), datetime.date(2022,12,31), "Discount"),
        (2, "Buy1Get1",     50.00, datetime.date(2023,1,1), datetime.date(2023,6,30),  "BOGO"),
        (3, "No Promotion", 0.00,  None,                    None,                     "None"),
    ]
    rows = []
    for row in data:
        rows.append({
            "promotionid": row[0],
            "description": row[1],
            "discount": row[2],
            "startdate": row[3],
            "enddate": row[4],
            "promotiontype": row[5]
        })
    return pd.DataFrame(rows)

# 6. Generate Type Dimension
def generate_type_dimension():
    # each tuple: (type, onoff_status, browser_name)
    types_data = [
        ("Online-Chrome",  "Online",  "Chrome"),
        ("Online-Firefox", "Online",  "Firefox"),
        ("Online-Safari",  "Online",  "Safari"),
        ("Offline",        "Offline", None)
    ]
    rows = []
    for t in types_data:
        rows.append({
            "type": t[0],
            "onoff_status": t[1],
            "browser_name": t[2]
        })
    return pd.DataFrame(rows)

# 7. Generate Fact Table (TransactionRecordFT)
def generate_transaction_records(date_df, customer_df, type_df, cinema_df, 
                                 promotion_df, movie_df, n_facts=1000000):
    date_list = date_df.to_dict('records')
    customer_list = customer_df.to_dict('records')
    type_list = type_df.to_dict('records')
    cinema_list = cinema_df.to_dict('records')
    promo_list = promotion_df.to_dict('records')
    movie_list = movie_df.to_dict('records')
    
    rows = []
    for i in range(1, n_facts + 1):
        drow = random.choice(date_list)
        crow = random.choice(customer_list)
        trow = random.choice(type_list)
        cirow = random.choice(cinema_list)
        prow = random.choice(promo_list)
        mrow = random.choice(movie_list)
        
        num_tix = random.randint(1, 5)
        price_per_ticket = random.uniform(8.0, 20.0)
        total_price = round(num_tix * price_per_ticket, 2)
        
        rows.append({
            "transactionid": i,
            "dateid": drow["dateid"],
            "customerid": crow["customerid"],
            "type": trow["type"],
            "cinemaid": cirow["cinemaid"],
            "promotionid": prow["promotionid"],
            "movieid": mrow["movieid"],
            "totalprice": total_price,
            "numberofticket": num_tix,
            "paymethod": random.choice(["Cash", "CreditCard", "DebitCard", "PayPal"])
        })
    
    return pd.DataFrame(rows)


if __name__ == "__main__":
    # 1) Generate DataFrames for all dimensions & fact table
    date_dim = generate_date_dimension(
        start_date=datetime.date(2014,1,1),
        end_date=datetime.date(2024,12,31)
    )
    cinema_dim = generate_cinema_dimension(n_cinemas=50)
    customer_dim = generate_customer_dimension(n_customers=100000)
    movie_dim = generate_movie_dimension(n_movies=1000)
    promotion_dim = generate_promotion_dimension()
    type_dim = generate_type_dimension()
    
    fact_df = generate_transaction_records(
        date_dim,
        customer_dim,
        type_dim,
        cinema_dim,
        promotion_dim,
        movie_dim,
        n_facts=1000000
    )
    
    # 2) Print shapes for a quick sanity check
    print("DateDT shape:", date_dim.shape)
    print("CinemaDT shape:", cinema_dim.shape)
    print("CustomerDT shape:", customer_dim.shape)
    print("MovieDT shape:", movie_dim.shape)
    print("PromotionDT shape:", promotion_dim.shape)
    print("TypeDT shape:", type_dim.shape)
    print("Fact table shape:", fact_df.shape)
    
    # 3) Connect to PostgreSQL (adjust user/password/host/dbname as needed)
    engine = create_engine("postgresql://admin:admin123@localhost:5432/postgres")
    
    # 4) Table names to truncate
    table_names = [
        "datedt",
        "cinemadt",
        "customerdt",
        "moviedt",
        "promotiondt",
        "typedt",
        "transactionrecordft"
    ]
    
    # 5) Truncate existing data using text(...) to handle raw SQL in SQLAlchemy 2.x
    with engine.begin() as conn:
        for tbl in table_names:
            # If a table doesn't exist or uses a different name, adjust accordingly
            conn.execute(text(f"TRUNCATE TABLE {tbl} CASCADE;"))
    
    # 6) Insert new data (append to now-empty tables)
    date_dim.to_sql("datedt", engine, if_exists="append", index=False)
    cinema_dim.to_sql("cinemadt", engine, if_exists="append", index=False)
    customer_dim.to_sql("customerdt", engine, if_exists="append", index=False)
    movie_dim.to_sql("moviedt", engine, if_exists="append", index=False)
    promotion_dim.to_sql("promotiondt", engine, if_exists="append", index=False)
    type_dim.to_sql("typedt", engine, if_exists="append", index=False)
    fact_df.to_sql("transactionrecordft", engine, if_exists="append", index=False)

    print("Data load complete!")