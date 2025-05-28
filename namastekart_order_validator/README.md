# NamasteKart Order Validator
## Description
A Python automation project that validates daily e-commerce order files for NamasteKart (operating in Mumbai and Bangalore). It checks for data integrity, business rule violations, and separates valid and invalid files. Rejected orders are flagged with reasons, and a daily summary is generated in email format.

## Key Features

- Validates against a product master (product_id, pricing)

- Checks for:

  - Empty fields

  - Invalid cities (only Mumbai & Bangalore allowed)

  - Incorrect amounts (price × quantity mismatch)

  - Future order dates

- Moves files to success or rejected folders

- Generates error reports with rejection reasons

- Outputs a validation summary (suitable for email)
