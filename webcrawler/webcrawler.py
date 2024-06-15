import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
from tqdm import tqdm

class Snail():
    """
    Snail that crawls KUSSS and collects data
    """
    base_url = "https://www.kusss.jku.at/kusss/"
    
    def __init__(self):
        
        # get course catalogue
        self.course_catalogue = self.get_detailed_course_catalogue()
        # prepare general search
        self.action, self.payload = self.prepare_catalogue_search(self.course_catalogue)
        
        # get all rooms
        dropdown_entries = self.search_html(self.course_catalogue, "select", {"name":"room"}, all=False)\
                                            .find_all("option")
        self.room_dict = {x.text.strip():x["value"] for x in dropdown_entries}                                    
    
    ####### Basic Methods #######
    def crawl(self, url, parameters=None, parse=True):
        """
        Crawls the given URL and extracts the data
        """
        if parameters is None:
            parameters = {}
            
        response = requests.get(url, params=parameters)
        
        if parse:
            response = BeautifulSoup(response.content, "html.parser")
        return response
    
    def search_html(self, soup, tag, attributes=None, all=True):
        """
        Searches the given soup for the given tag and class_name
        """
        if all:
            return soup.find_all(tag, attrs=attributes)
        else:
            return soup.find(tag, attrs=attributes)

    def filter_by_room(self, dates_dataframe, room):
        """
        Filters the dates_dataframe by the given room
        """
        df = dates_dataframe.copy()
        return df[df["Ort"].str.contains(room)].reset_index(drop=True)
    
    def export_to_csv(self, dataframe, filename):
        """
        Exports the dataframe to a csv file
        """
        try:
            dataframe.to_csv(filename, index=False)
            print("Dataframe successfully exported to", filename)
        except:
            print("Error exporting dataframe to", filename)    
    
    ######### Methods for KUSSS #########
    def get_detailed_course_catalogue(self):
        """
        Returns the detailed course catalog
        """
        #coursecatalogue-start.action?advanced=true
        url = self.base_url + "coursecatalogue-start.action" + "?advanced=true"
        soup = self.crawl(url)
        return soup
    
    def prepare_catalogue_search(self, soup):
        """
        Prepares the search for the course catalog
        """
        #print(soup)
        details_form = self.search_html(soup, "form", {"method":"get"}, all=True)[-1]

        action = details_form["action"]
        
        payload = {}
        
        input_fields = []
        input_fields += self.search_html(details_form, "input", {"class":"inputfields"}, all=True)
        input_fields += self.search_html(details_form, "input", {"type":"hidden"}, all=True)
        for field in input_fields:
            payload[field["name"]] = field["value"]
            
        select_fields = self.search_html(details_form, "select", all=True)
        for field in select_fields:
            payload[field["name"]] = "all"
            
        return action, payload
        
    def get_search_result_table(self, action, payload):
        """
        Returns the search results
        """
        result_html = self.crawl(self.base_url + action, payload)
        
        tables = self.search_html(result_html, "table", all=True)
        result_table = self.search_html(tables[-1], "tr", all=True)
        
        return result_table

    def extract_search_results(self, result_table):
        """
        Extracts the search results
        """
        format = result_table[0]
        columns = [x.text.strip() for x in self.search_html(format, "th", all=True)]
        
        link_dict = {}
        dataframe = pd.DataFrame(columns=columns)

        for row in result_table[1:]:
            
            cells = self.search_html(row, "td", all=True)
            
            row = []
            for cell in cells:
                
                text = cell.text.strip()
                
                # clean text
                x = [x for x in re.split("\n|\t", text) if x != ""]
                text = "\n".join(x)
                text = x[0]
                
                link = cell.a
                if link is not None:
                    link = link["href"]
                    link_dict[x[0]] = link

                row .append(text)
                
            dataframe.loc[len(dataframe)] = row

        dataframe["max_students"] = None
        dataframe["registered_students"] = None
        
        return dataframe, link_dict

    def clean_string_dates(self, string):
        return re.split("\n|\t|–", string)
    
    def get_lva_details_and_dates(self, lva_url):

        # harvest all the information from the lva overview page
        lva_page = self.crawl(self.base_url + lva_url)        
        
        ############# Study Handbook #############
        # get link to study handbook
        studyhandbook_link = self.search_html(lva_page, "a", {"title":"Studienhandbuch"}, all=False)["href"]
        studyhandbook_page = self.crawl(studyhandbook_link)
        
        handbook_info_dict = dict()

        curriculum_info = self.search_html(studyhandbook_page, "li", {"class":"bread-crumb-trail"}, all=True)
        # new info
        if curriculum_info == []:
            studienfach = "Not available"
        else:
            studienfach = curriculum_info[1].get_text(strip=True)
        
        # harvest header table
        first_table = self.search_html(studyhandbook_page, "table", {"cellpadding":"3", "cellspacing":"0"}, all=False)
        if first_table != None:
            # get header info
            for header_element in first_table.find_all("th"):
                handbook_info_dict[header_element.get_text(strip=True)] = None
            # get content info
            keys = list(handbook_info_dict.keys())    
            for i, header_element in enumerate(first_table.find_all("td")):
                handbook_info_dict[keys[i%len(keys)]]= header_element.get_text(strip=True)

        # harvest table "Detailinformation"
        details_table = self.search_html(studyhandbook_page, "table", all=True)[-2]
        
        if not ("Diese Lehrveranstaltung konnte leider nicht gefunden werden!" in details_table.get_text()):
            for row in details_table.find_all("tr")[1:]:
                row_elements = list(filter(None, row.get_text().split("\n")))
                handbook_info_dict[row_elements[0]] = "\n".join(row_elements[1:])
                
        handbook_info_dict["Studienfach"] = studienfach
            
        ############# LVA ############# 
        # get some lva details
        info = self.search_html(lva_page, "tr", attributes={"class":"priorityhighlighted"}, all=False).find_all("td")
        # crawl some basic info
        lva_number = info[0].get_text(strip=True)
        max_students = int(info[-4].get_text(strip=True))
        registered_students = int(info[-2].get_text(strip=True))
        
        # crawl more information from second table "subinfo"
        subinfo_table = self.search_html(lva_page, "table", {"class":"subinfo"}, all=False)
        subinfo_header = self.search_html(subinfo_table, "td", {"valign":"top"}, all=True)[0]
        # store the infromation in dictionary
        subinfo_dict = dict()       
        for row in subinfo_header.get_text().split("\n"):
            row_elements = row.split(":")
            if len(row_elements) == 2:
                subinfo_dict[row_elements[0].strip()] = row_elements[1].strip()
        
        # extract the dates of the lva
        summary = f"Übersicht aller Termine der Lehrveranstaltung {lva_number}"
        dates_table = self.search_html(lva_page, "table", attributes={"summary":summary}, all=True)[1]
        
        
        # dataframe to store information
        dates_dataframe = pd.DataFrame(columns=["LVA-Nummer", "Wochentag", "Datum", "Startzeit", "Endzeit", "Ort", "Anmerkung"])

        #lva_dates = []
        date_list_uncleaned = dates_table.find_all("tr")[1:]
        
        # filter out all dates not in the desired room
        for idx in range(len(date_list_uncleaned)-1):
            
            date_info = [x.strip() for x in self.clean_string_dates(date_list_uncleaned[idx].get_text()) if x!=""]
                
            if idx%2==0:
                if len(date_info) < 5:
                    helper = [" "] * (5 - len(date_info))
                    date_info += helper

                dates_dataframe.loc[len(dates_dataframe)] =   [lva_number, date_info[0], date_info[1], date_info[2], date_info[3], date_info[4], ""]
                #lva_dates.append([date_info[0], date_info[1], date_info[2], date_info[3], date_info[4], ""])

            else:
                if len(date_info) != 0:
                    dates_dataframe.loc[idx//2, "Anmerkung"] = " ".join(date_info)


        return max_students, registered_students, dates_dataframe, subinfo_dict, handbook_info_dict

    ######### Application #########
    def validate_room(self, room_name):
        try:
            self.payload["room"] = self.room_dict[room_name]
        except KeyError:
            print("Room not found")
            return False
        return True
    
    def derive_exam_dates(self, dates_dataframe):
        # conaints exam, prüfung, klausur, test, quiz
        df = dates_dataframe.copy()
        df["exam"] = df["Anmerkung"].str.contains("Prüfung|Klausur|TK|Exam|NK", case=False)
        df["test"] = df["Anmerkung"].str.contains("Test|Quiz", case=False)
        return df
    
    def derive_tutorium_dates(self, dates_dataframe):
        df = dates_dataframe.copy()
        df["tutorium"] = df["Anmerkung"].str.contains("Tutorium|Fragestunde|Sprechstunde", case=False)
        return df
              
    def derive_regularity(self, dates_dataframe):
        
        df_dates = dates_dataframe.copy()
        
        df_dates = df_dates[~df_dates["exam"] & ~df_dates["tutorium"]]
        
        # correct the dates
        dates_total = df_dates["Datum"].apply(lambda x: pd.to_datetime(x, format="%d.%m.%y").date())
        # filter out dates after end of semester
        #dates_semester = dates_total[dates_total < pd.to_datetime("2024-07-01").date()]
        #if dates_semester.empty:
        #    regularity = "out of semester"
        #    return regularity
        
        ## get the min and max date
        #min_date = dates_semester.min()
        #max_date = dates_semester.max()
        #weeks_between = (max_date - min_date).days/7
        
        no_dates_total = len(dates_total)
        return no_dates_total
        #if weeks_between >= 10: 
        #    ratio = no_dates_total / weeks_between
        #    if 0.7 <= ratio <= 1.2:
        #        regularity = "weekly"
                
        #    elif 1.3 <= ratio <= 2.1:
        #        regularity = "twice a week"
                
        #    elif 2.1 < ratio:
        #        regularity = "more than 2 a week"
                

        #    elif 0.5 <= ratio < 0.7:
        #        regularity = "biweekly"
        #    else:
        #        # 0.428 -> irregular
        #        regularity = "irregular"
        #        print("irregular")
        #        print(no_dates_total / weeks_between, weeks_between)
        #        print()
                
                
        #else:
        #    # weeks_between 2 & ratio=1.5 -> blocked course
        #    regularity = "irregular"
        #    print("irregular")
        #    print(no_dates_total / weeks_between, weeks_between)
        #    print()
            
            # twice a week
            
    def accumulate_course_dates(self, dataframe_courses, link_dict, room):
        
        df_courses = dataframe_courses.copy()
        
        dates_list = []
        for i,row in tqdm(df_courses.iterrows(), total=len(df_courses)):
            # extract the lva number and action link
            lva_number = row["LVA-Nr."]
            action = link_dict[lva_number]
            
            # get the details and dates of the lva
            max_students, registered_students, df_dates, subinfo_dict, studyhandbook_dict = self.get_lva_details_and_dates(action)
            
            df_dates = self.derive_exam_dates(df_dates)
            df_dates = self.derive_tutorium_dates(df_dates)
            # derive regularity
            no_dates_total = self.derive_regularity(df_dates)
            df_courses.loc[i, "no_dates_total"] = no_dates_total
            # filter the dates by the room            
            df_dates = self.filter_by_room(df_dates, room)
            
            # store the dates
            dates_list.append(df_dates)
            # store the max and registered students
            df_courses.loc[i, "max_students"] = max_students
            df_courses.loc[i, "registered_students"] = registered_students
            
            # store infromation from dictionaries
            for key in subinfo_dict.keys():
                if key == "Abhaltungs-Sprache":
                    df_courses.loc[i, "Abhaltungssprache_subinfo"] = subinfo_dict[key]
                else:
                    df_courses.loc[i, key] = subinfo_dict[key]
            
            # all features
            # ['Workload', 'Ausbildungslevel', 'Studienfachbereich', 
            #  'VerantwortlicheR', 'Semesterstunden', 'Anbietende Uni', 
            #  'Quellcurriculum', 'Ziele', 'Lehrinhalte', 
            #  'Beurteilungskriterien', 'Lehrmethoden', 
            #  'Abhaltungssprache', 'Literatur', 'Lehrinhalte wechselnd?', 
            #  'Sonstige Informationen', 'Äquivalenzen', 'Studienfach']
                
            interesting_features = ['Ausbildungslevel', 'Studienfachbereich', 'Anbietende Uni', 
             'Quellcurriculum', 'Beurteilungskriterien', 'Lehrmethoden', 
             'Abhaltungssprache', 'Literatur', 'Lehrinhalte wechselnd?', 
             'Sonstige Informationen', 'Studienfach']

            for key in studyhandbook_dict.keys():
                if key in interesting_features:
                    if key == "Abhaltungssprache":
                        df_courses.loc[i, "Abhaltungssprache_studyhandbook"] = studyhandbook_dict[key]
                    else:
                        df_courses.loc[i, key] = studyhandbook_dict[key]

        df_dates = pd.concat(dates_list).reset_index(drop=True)
        
        return df_courses, df_dates
    
    def get_courses_by_room(self, room_name):
        
        if self.validate_room(room_name):
            
            # get the search results
            result_table = self.get_search_result_table(self.action, self.payload)
            # extract the search results
            df_courses, link_dict = self.extract_search_results(result_table)
            
            df_courses, df_dates = self.accumulate_course_dates(df_courses, link_dict, room_name)
            
            return df_courses, df_dates 
        
        else:
            return None, None