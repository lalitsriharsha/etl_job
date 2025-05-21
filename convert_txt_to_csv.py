# import pandas as pd
import re
# def convert_txt_to_csv(txt_path, csv_path, delimiter="\t"):
#     df = pd.read_csv(txt_path, delimiter=delimiter)
#     df.to_csv(csv_path, index=False)
#     print(f"Converted {txt_path} to {csv_path}")

# convert_txt_to_csv("resources/raw_data/patients.txt", "resources/raw_data/patients.csv")


def validate_phone(phone):
    return phone if re.match(r"^\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}$", str(phone)) else None

print(validate_phone('402+617+5817'))
