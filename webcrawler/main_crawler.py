from webcrawler import Snail
import requests
import re
import pandas as pd

"""
Note: 
Webcrawler worked on 15.6.2024 for HS 19 and HS 18. 
Due to changes on the website, the webcrawler might not work anymore.
"""

snail = Snail()

for room in ["HS 18", "HS 19"]:
    df_courses, df_dates = snail.get_courses_by_room(room)

    snail.export_to_csv(df_courses, f"data/raw/{room}_courses.csv")
    snail.export_to_csv(df_dates, f"data/raw/{room}_dates.csv")



