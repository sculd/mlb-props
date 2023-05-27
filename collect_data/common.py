def get_end_date(year):
    end_date = f"{year+1}-03-01"
    if year == 2022:
        end_date = f"{year}-12-01"
    elif year == 2023:
        end_date = (datetime.today() - timedelta(days = 1)).strftime("%Y-%m-%d")
    return end_date
