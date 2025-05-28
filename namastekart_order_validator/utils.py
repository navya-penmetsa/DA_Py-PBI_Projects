import pandas as pd
from datetime import datetime


# Loads the product master CSV and returns a DataFrame
def load_product_data(file_path):
    try:
        df = pd.read_csv(file_path)
        return df
    except Exception as e:
        print(f"Error loading product master: {e}")
        return pd.DataFrame()


# Returns True if product_id exists in the product master DataFrame
def validate_product_id(product_id, product_df):
    return product_id in product_df['product_id'].values


# Returns True if sales amount matches: product price × quantity
def validate_sales_amount(row, product_df):
    product_id = row['product_id']
    quantity = row['quantity']
    amount = row['sales']

    price = product_df.loc[product_df['product_id'] == product_id, 'price']
    if not price.empty:
        expected = float(price.values[0]) * float(quantity)
        return round(expected, 2) == round(float(amount), 2)
    return False


# Returns True if order date is today or earlier, False if it's in the future or invalid.
def validate_order_date(date_str):
    try:
        order_date = datetime.strptime(date_str.strip(), '%d-%m-%Y').date()
        today_date = datetime.now().date()
        return order_date <= today_date
    except ValueError:
        return False  # Invalid date format


# Returns True if all fields in the row are non-empty, False otherwise
def validate_non_empty(row):
    return row.notnull().all() and not (row.astype(str).str.strip() == '').any()


# Returns True if city is Mumbai or Bangalore (case-insensitive), False otherwise
def validate_city(city):
    if not isinstance(city, str):  # Prevents issues if city is missing (NaN or None)
        return False
    return city.strip().lower() in ["mumbai", "bangalore"]


# Master validation function, validates all functions
def validate_row(row, product_df):
    errors = []

    if not validate_product_id(row['product_id'], product_df):
        errors.append("Invalid product_id")

    if not validate_sales_amount(row, product_df):
        errors.append("Incorrect sales amount")

    if not validate_order_date(row['order_date']):
        errors.append("Future order date")

    if not validate_non_empty(row):
        errors.append("Empty fields present")

    if not validate_city(row['city']):
        errors.append("Invalid city")

    return errors  # Empty list means the row is valid
