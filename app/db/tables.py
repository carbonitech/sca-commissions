"""A dictionary of table schemas keyed by their names"""

TABLES = {
    "customers": (
        "id SERIAL PRIMARY KEY",
        "name TEXT"),
    "customer_branches":
        ("id SERIAL PRIMARY KEY",
        "customer_id INTEGER NOT NULL",
        "city TEXT", "state TEXT",
        "zip INTEGER"),
    "map_customer_name":
        ("id SERIAL PRIMARY KEY",
        "recorded_name TEXT",
        "standard_name TEXT"),
    "map_rep":
        ("id SERIAL PRIMARY KEY",
        "rep_id INTEGER NOT NULL",
        "customer_branch_id INTEGER NOT NULL"),
    "map_city_names":
        ("id SERIAL PRIMARY KEY",
        "recorded_name TEXT",
        "standard_name TEXT"),
    "manufacturers":
        ("id SERIAL PRIMARY KEY",
        "name TEXT"),
    "manufacturers_reports":
        ("id SERIAL PRIMARY KEY",
        "manufacturer_id INTEGER NOT NULL",
        "report_name TEXT",
        "yearly_frequency INTEGER", 
        "POS_report BOOLEAN"),
    "report_submissions_log":
        ("id SERIAL PRIMARY KEY",
        "submission_date TIMESTAMP",
        "reporting_month INTEGER",
        "reporting_year INTEGER",
        "report_id INTEGER"),
    "final_commission_data":    # TODO: make this table customizable instead of hard-coded
        ("id SERIAL PRIMARY KEY",
        "Year INTEGER",
        "Month TEXT",
        "Manufacturer TEXT",
        "Salesman TEXT",
        "Customer_Name TEXT",
        "City TEXT",
        "State TEXT",
        "Inv_Amt REAL",
        "Comm_Amt REAL"),
    "report_processing_steps_log":
        ("id SERIAL PRIMARY KEY",
        "submission_id INTEGER",
        "step_num INTEGER",
        "description TEXT"),
    "current_errors":
        ("id SERIAL PRIMARY KEY",
        "submission_id INTEGER",
        "field TEXT",
        "value_type TEXT",
        "value_content TEXT",
        "reason TEXT")
}