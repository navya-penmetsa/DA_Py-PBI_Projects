import os
import pandas as pd
from datetime import datetime
from utils import load_product_data, validate_row


# Load product master
product_df = load_product_data(r'E:\NamasteSQL\Python\Projects\NamasteKart\data\product_master.csv')
print(product_df.head())


# Define base paths
base_dir = os.path.dirname(os.path.abspath(__name__))  # Automatically use the folder where the script is located
today = datetime.today().strftime('%Y%m%d')
incoming_folder = os.path.join(base_dir, 'incoming_files', today)

success_path = os.path.join(base_dir, 'success_files', today)
rejected_path = os.path.join(base_dir, 'rejected_files', today)

os.makedirs(success_path, exist_ok=True)
os.makedirs(rejected_path, exist_ok=True)


# Load incoming order files
orders = {}      # dict: filename -> DataFrame

if os.path.exists(incoming_folder):
    for fname in os.listdir(incoming_folder):
        if fname.endswith('.csv'):
            file_path = os.path.join(incoming_folder, fname)
            try:
                df = pd.read_csv(file_path)
                orders[fname] = df
            except Exception as e:
                print(f"Error reading {fname}: {e}")
else:
    print(f"No incoming folder found for today: {incoming_folder}")


# Validate each file
success_files = []
rejected_files = []

for filename, df in orders.items():
    error_rows = []

    for index, row in df.iterrows():
        errors = validate_row(row, product_df)
        if errors:
            row_copy = row.copy()
            row_copy['rejection_reason'] = '; '.join(errors)
            error_rows.append(row_copy)

    if error_rows:
        # File is rejected
        rejected_files.append((filename, df, error_rows))
    else:
        # File is valid
        success_files.append((filename, df))


# Save successful files
for filename, df in success_files:
    df.to_csv(os.path.join(success_path, filename), index=False)


# Save rejected files + error file
for filename, original_df, error_rows in rejected_files:
    # Save original rejected file
    original_path = os.path.join(rejected_path, filename)
    original_df.to_csv(original_path, index=False)

    # Save error file with rejection reasons
    error_filename = f"error_{filename}"
    error_df = pd.DataFrame(error_rows)
    error_path = os.path.join(rejected_path, error_filename)
    error_df.to_csv(error_path, index=False)


# Generate and print email summary
def get_validation_summary(total, success, rejected):
    today_str = datetime.today().strftime('%Y-%m-%d')
    subject = f"validation email {today_str}"
    body = f"Total {total} incoming files, {success} successful files and {rejected} rejected files for that day."
    return subject, body

total = len(orders)
success = len(success_files)
rejected = len(rejected_files)

subject, body = get_validation_summary(total, success, rejected)

print("\nEMAIL TO BUSINESS")
print(f"Subject: {subject}")
print(f"Body: {body}")