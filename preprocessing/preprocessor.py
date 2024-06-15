import os
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

class Preprocessor:
    
    time_format = "%a %b %d %H:%M:%S %Y"
    date_format = "%Y-%m-%d"
    
    def __init__(self, room_to_id, door_to_id):
        self.room_to_id = room_to_id
        self.door_to_id = door_to_id
           
    #######  File I/O Helper Methods ########
    def read_from_csv(self, path_to_file):
        data = pd.read_csv(path_to_file)
        return data
    
    def save_to_csv(self, dataframe, path_to_file, file_name):
        # store the data in the data directory
        dataframe.to_csv(os.path.join(path_to_file, file_name) + ".csv", index=False)
        # store the datatypes in the data directory
        dataframe.dtypes.to_csv(os.path.join(path_to_file, file_name) + "_dtypes.csv", index=False)
        return True
    
    ####### Basic File Management Methods ########
    def get_all_sub_directories(self, path_to_dir):
        sub_dirs = sorted(list(os.walk(path_to_dir))[0][1])
        return sub_dirs
    
    def get_all_sub_files(self, path_to_dir): 
        sub_files = sorted(list(os.walk(path_to_dir))[0][2])
        return sub_files
    
    def get_and_filter_sub_files(self, path_to_files, name_pattern):
        sub_files = [x for x in self.get_all_sub_files(path_to_files) if name_pattern in x]
        return sub_files
    
    ####### Basic Data Manipulation ########
    def change_time_format(self, dataframe, column_name, format_str=time_format):
        dataframe[column_name] = pd.to_datetime(dataframe[column_name], format=format_str)
        return dataframe
    
    def rename_columns(self, dataframe, old_names, new_names):
        df = dataframe.copy(deep=True)
        df = df.rename(columns=dict(zip(old_names, new_names)))
        return df

# class CoursePreprocessor that inherits from Preprocessor
class CoursePreprocessor(Preprocessor):
    room_capacities = {0:164, 1:152}

    def __init__(self, path_to_raw_courses, room_to_id, door_to_id):
        super().__init__(room_to_id=room_to_id, door_to_id=door_to_id)
        self.path_to_raw_courses = path_to_raw_courses
        self.time_format = "%d.%m.%y %H:%M"
        
        ### Call the methods to read the data
        self.raw_course_dates = self.read_course_dates_data(self.path_to_raw_courses)
        self.raw_course_info = self.read_course_info_data(self.path_to_raw_courses)

    ########## Read/Load Data ##########
    def read_and_assign_room(self, file, path_to_raw_data):
        
        df = self.read_from_csv(os.path.join(path_to_raw_data, file))
        room_identifier = file.split("_")[0]
        df["room_id"] = self.room_to_id[room_identifier]
        
        return df
    
    # The two functions below could be refactored into one method
    # However, the data is stored in different files and the 
    # data is structured differently so i decided to keep them separate
    def read_course_info_data(self, path_to_raw_data):
        # get all subfiles in the directory and filter them by the name pattern "courses"
        sub_files = self.get_and_filter_sub_files(path_to_raw_data, "courses")
        
        # read all the files and assign the room identifier stored in the file name
        dataframes = []
        for file in sub_files:
            df = self.read_and_assign_room(file, path_to_raw_data)
            dataframes.append(df)
        # concatenate all the dataframes
        return pd.concat(dataframes, axis=0).reset_index(drop=True)
    
    def read_course_dates_data(self, path_to_raw_data):
        # get all subfiles in the directory and filter them by the name pattern "dates"
        sub_files = self.get_and_filter_sub_files(path_to_raw_data, "dates")
        
        # read all the files and assign the room identifier stored in the file name
        dataframes = []
        for file in sub_files:
            df = self.read_and_assign_room(file, path_to_raw_data)
            dataframes.append(df)
        # concatenate all the dataframes
        return pd.concat(dataframes, axis=0).reset_index(drop=True)
    
    ########## Clean Data ##########
    def format_course_number(self, course_number):
        if type(course_number) == str:
            return course_number
        else:
            return "{:.3f}".format(course_number)
            
    def derive_timestamps(self, dataframe, col_name_in, col_name_out):
        
        #df_dates["start_time"] = df_dates.apply(lambda x: x["Datum"] + " " + x["Startzeit"], axis=1)
        
        # might be a soruce for a bug
        dataframe["Datum"] = dataframe["Datum"].astype(str)
        dataframe[col_name_in] = dataframe[col_name_in].astype(str)
        dataframe[col_name_out] = dataframe["Datum"] + " " + dataframe[col_name_in]
        
        dataframe = self.change_time_format(dataframe, col_name_out, self.time_format)
        
        return dataframe
    
    def format_dates(self, dataframe):
        
        dataframe = self.derive_timestamps(dataframe, "Startzeit", "start_time")
        dataframe = self.derive_timestamps(dataframe, "Endzeit", "end_time")

        dataframe.drop(["Datum", "Startzeit", "Endzeit"], axis=1, inplace=True)
        
        return dataframe
            
    def clean_raw_course_info(self, dataframe):   
        
        df = dataframe.copy()
        # drop unneccessary columns
        df.drop(["Nächster Termin", "SSt.", "Abhaltungssprache_subinfo", "Literatur", "Lehrinhalte wechselnd?"], axis=1, inplace=True)
        
        df = self.rename_columns(df, 
                                              ["LVA-Nr.", "LVA-Titel", "Typ", "Art", "LeiterIn", "Sem.", "ECTS"], 
                                              ["course_number", "course_name", "type", "kind", "lecturer", "semester", "ects"])
        
        df = self.rename_columns(df,
                                         ["Institut", "E-Mail", "Ausbildungslevel", "Studienfachbereich", "Anbietende Uni", "Quellcurriculum", "Beurteilungskriterien", "Lehrmethoden", "Abhaltungssprache_studyhandbook", "Studienfach", "Sonstige Informationen"],
                                         ["instute", "email", "level", "study_area", "university", "curriculum", "assessment_criteria", "teaching_methods", "language", "study_subject", "other_information"])
        
        df["course_number"] = df["course_number"].apply(lambda x: self.format_course_number(x))
        
        df = df.drop_duplicates().reset_index(drop=True)
        return df
    
    def clean_raw_course_dates(self, dataframe):
        df = dataframe.copy()
        
        # format the start and end times
        df = self.format_dates(df)
        
        # rename columns
        df = self.rename_columns(df,
                                        ["LVA-Nummer", "Wochentag", "Ort", "Anmerkung"],
                                        ["course_number", "weekday", "room", "note"])

        # fill in empty notes with empty string
        df["note"] = df["note"].fillna("")  

        # format the course number
        df["course_number"] = df["course_number"].apply(lambda x: self.format_course_number(x))
        
        # drop potential duplicates and reset index
        df = df.drop_duplicates().reset_index(drop=True)
        return df
    
    #######  Data Enhancement Methods ########
    def add_room_capcity(self, dataframe):
        dataframe["room_capacity"] = dataframe["room_id"].apply(lambda x: self.room_capacities[x])
        return dataframe
    
    def add_calendar_week(self, dataframe, col_name):
        #df["calendar_week"] = df[time_column].apply(lambda x: x.date().isocalendar().week)
        dataframe["calendar_week"] = dataframe[col_name].dt.isocalendar().week
        return dataframe
    
    def enhance_dataset(self, dataframe):
        dataframe = self.add_room_capcity(dataframe)
        dataframe = self.add_calendar_week(dataframe, "start_time")
        return dataframe
    
    def correct_curriculum_row(self, row):
        word_list = row.split(" ")
        # filter out all words that cointains numbers
        filtered = [x for x in word_list if not any(c.isdigit() for c in x)]
        
        return " ".join(filtered)
        
    def correct_curriculum(self, dataframe):  
        # check if curriculum is nan
        mask = ~dataframe["curriculum"].isna()
        dataframe.loc[mask, "curriculum"] = dataframe.loc[mask, "curriculum"].apply(lambda x: self.correct_curriculum_row(x))
        return dataframe
    #######  Preprocessing Application ########
    def apply_preprocessing(self):     
        # clean the raw dates 
        cleaned_dates = self.clean_raw_course_dates(self.raw_course_dates)
        cleaned_dates = self.enhance_dataset(cleaned_dates)
        # clean the raw course info
        cleaned_courses = self.clean_raw_course_info(self.raw_course_info)
        cleaned_courses = self.correct_curriculum(cleaned_courses)
        
        return cleaned_courses, cleaned_dates
    
# class SignalPreprocessor that inherits from Preprocessor
class SignalPreprocessor(Preprocessor):
    
    def __init__(self, path_to_data, room_to_id, door_to_id):
        
        # initialize the parent class
        super().__init__(room_to_id=room_to_id, door_to_id=door_to_id)

        self.date_lowerbound_signal = datetime.strptime("2024-04-07", self.date_format)
        self.raw_data_format_signal = ['Entering', 'Time', 
                       'People_IN', 'People_OUT', 
                       'IN_Support_Count', 'OUT_Support_Count', 
                       'One_Count_1', 'One_Count_2']
    
        self.path_to_data = path_to_data 
        
        # get all subdirectories in the data directory
        self.list_dirs = self.get_list_of_data_dirs()
        # extract all the raw data
        self.raw_uncleaned_data = self.accumulate_raw_data(self.list_dirs)
    
    #######  Data Extraction Helper Methods ########
    def filter_directories(self, directories:list):
        filtered_dirs = []
        for x in directories:
            day = datetime.strptime(x.split("_")[2], self.date_format)
            if self.date_lowerbound_signal < day:
                filtered_dirs.append(x)
        return filtered_dirs
      
    def get_list_of_data_dirs(self):
        path = os.path.join(self.path_to_data)
        sub_dirs = self.get_all_sub_directories(path)
        filtered = self.filter_directories(sub_dirs)
        return filtered
    
    #######  Data Extraction Methods ######## 
    def accumulate_raw_data(self, data_directories):
        
        accumulated_format = self.raw_data_format_signal + ["Room_ID", "Door_ID"]
        df_accumulated = pd.DataFrame(columns=self.raw_data_format_signal)
        samples = 0
        for data_dir_name in data_directories:

            path = os.path.join(self.path_to_data, data_dir_name)
            file_list = self.get_all_sub_files(path)
            
            # sanity check
            # check if the directory contains the correct files
            #if "door1.csv", "door2.csv", "format.csv":
            # delete all other files            
            file_list = [x for x in file_list if x in ["door1.csv", "door2.csv", "format.csv"]]
            
            if not "door1.csv" in file_list or not "door2.csv" in file_list or not "format.csv" in file_list:
                print(path)
                print(file_list)
                raise ValueError("Data directory does not contain the correct files")
            
            room_name = data_dir_name.split("_")[1]
            room_id = self.room_to_id[room_name]
            
            for x in file_list[:-1]:
                
                door_name = x.split(".")[0]
                door_id = self.door_to_id[door_name]
                
                file_path = os.path.join(path, x) 

        
                df = pd.read_csv(file_path, names=self.raw_data_format_signal)
                
                df = self.change_time_format(df, "Time", self.time_format).sort_values(by="Time", ascending=False)
                df["Room_ID"] = room_id
                df["Door_ID"] = door_id

                samples += len(df)
                df_accumulated = pd.concat([df_accumulated, df], axis=0)
        
        return df_accumulated.reset_index(drop=True)
      
    #######  Data Cleaning Methods ########    
    def discard_samples(self, dataframe, lb, ub):
        df = dataframe.copy()
        mask  = df.apply(lambda x: (x["time"].time() >= lb) & (x["time"].time() <= ub), axis=1)
        df = df[mask].reset_index(drop=True)
        return df

    def df_room_door_dict(self, df:pd.DataFrame):
        room_door_dict = {}
        for room in df["room_id"].unique():
            room_dict = {}
            for door in df["door_id"].unique():
                mask = (df["room_id"] == room) & (df["door_id"] == door)
                if mask.sum() == 0:
                    continue
                else:
                    room_dict[door] = df[mask].reset_index(drop=True)
            room_door_dict[room] = room_dict
        return room_door_dict
    
    def event_type_majority_vote_closest(self, dataframe, reference_time, n, target_removed):
        df = dataframe
        
        # Calculate the absolute difference between times
        abs_diff = np.abs(df["time"] - reference_time)
        
        # Get the indices of the n smallest differences
        if target_removed:
            idx = abs_diff.nsmallest(n).index
        else:
            idx = abs_diff.nsmallest(n + 1).index[1:n + 1]
        
        # Filter the dataframe based on these indices
        filtered = df.loc[idx]
        
        # Get the most common event type
        common_event_type = filtered["event_type"].mode().iloc[0]
        
        return common_event_type
    
    def get_neighborhood(self, dataframe, x, k):
        n_samples = len(dataframe)
        i1 = x-k
        if i1 < 0:
            i1 = 0
        i2 = x+k
        if i2 > n_samples:
            i2 = n_samples
        rows = dataframe.loc[i1:i2]
        return rows
      
    def filter_discard(self, dataframe, lb_in, lb_out, **kwargs):
        df = dataframe.copy()
        
        event_types = [0,1]
        df = df[df["event_type"].isin(event_types)].reset_index(drop=True)

        # drop all samples with low support count
        df = df[(df["in_support_count"] >= lb_in) | (df["out_support_count"] >= lb_out)]
        
        return df
    
    def filter_data_n_closest(self, dataframe, k, nm, lb_in, lb_out, handle_5, handle_6, **kwargs):
        df = dataframe.copy()
        
        event_list = [0,1]
        if handle_5:
            event_list.append(5)
        if handle_6:
            event_list.append(6)
            
        df = df[df["event_type"].isin(event_list)].sort_values(by="time", ascending=True).reset_index(drop=True)
         
        dict_df_room_door = self.df_room_door_dict(df)
        df_return = pd.DataFrame(columns=list(df.columns))
        df_return = df_return.astype(df.dtypes)
        
        for room, room_dict in dict_df_room_door.items():
            for door, df_room_door in room_dict.items():  
                
                # deal with events with low directional support! 
                df_test = df_room_door.copy()
            
                # filter out samples with low support count
                df_test = df_test[(df_test["in_support_count"] < lb_in) 
                                & (df_test["out_support_count"] < lb_out)]
            
        
                for x in df_test.index:
                    # use index to get row
                    x_row = df_room_door.loc[x]
                    x_time = x_row["time"]
                    #x_door = x_row["door_id"]
                    #x_room = x_row["room_id"]
                    # select neighborhood of sample
                    rows = self.get_neighborhood(df_room_door, x, k)
                    # from the neighborhood select the n with the closest time stamp 
                    common_event_type = self.event_type_majority_vote_closest(rows, x_time, nm, targte_removed=False)
                    df_room_door.loc[x, "event_type"] = common_event_type
                    
                df_return = pd.concat([df_return, df_room_door], axis=0)
            
        return df_return
            
    def filter_data_time_window(self, dataframe, k, ns, nm, s, lb_in, lb_out, handle_5, handle_6, **kwargs):
        df = dataframe.copy()
        
        event_list = [0,1]
        if handle_5:
            event_list.append(5)
        if handle_6:
            event_list.append(6)
            

        df = df[df["event_type"].isin(event_list)].sort_values(by="time", ascending=True).reset_index(drop=True)
         
        dict_df_room_door = self.df_room_door_dict(df)
        df_return = pd.DataFrame(columns=list(df.columns))
        df_return = df_return.astype(df.dtypes)
       
        
        for room, room_dict in dict_df_room_door.items():
            for door, df_room_door in room_dict.items():  
                
                # deal with events with low directional support!
                df_test = df_room_door.copy()
            
                # filter out samples with low support count
                df_test = df_test[(df_test["in_support_count"] < lb_in) 
                                & (df_test["out_support_count"] < lb_out)]
                
                #handle the samples with low support count
                for x in df_test.index:
                    # use index to get row
                    x_row = df_room_door.loc[x]
                    x_time = x_row["time"]
            
                    # select neighborhood of sample
                    rows = self.get_neighborhood(df_room_door, x, k)
            
                    # try time filter first -> more reliable
                    x_time_lb = x_time - timedelta(seconds=s)
                    x_time_ub = x_time + timedelta(seconds=s)
                    rows_time_filtered = rows[(rows["time"] >= x_time_lb) & (rows["time"] <= x_time_ub)]
            
                    # if only one sample in time window
                    if len(rows_time_filtered) == 1:
                        # select make majority vote with the n closeste neighbors
                        common_event_type = self.event_type_majority_vote_closest(rows, x_time, nm, target_removed=False)
                    # if more than one sample in time window     
                    else:
                        # make majority vote with the samples in the time window
                        common_event_type = self.event_type_majority_vote_closest(rows_time_filtered, x_time, ns, target_removed=False)
                
                    df_room_door.loc[x, "event_type"] = common_event_type
                    
                df_return = pd.concat([df_return, df_room_door], axis=0)

        return df_return

    def handle_event_type_5_6(self, dataframe, k, s, m, ns, nm):
        df = dataframe.copy().reset_index(drop=True)
        mask = ((df["event_type"] == 6) | (df["event_type"] == 5))
        
        for x in df[mask].index:
            
            x_row = df.loc[x]
            x_time = x_row["time"]

            rows = self.get_neighborhood(df, x, k)
            #print(rows)
            
            x_time_lb = x_time - timedelta(seconds=s)
            x_time_ub = x_time + timedelta(seconds=s)
            rows_time_filtered = rows[(rows["time"] >= x_time_lb) & (rows["time"] <= x_time_ub)]
            rows_time_filtered = rows_time_filtered[rows_time_filtered["event_type"].isin([0,1])]
            
            if len(rows_time_filtered) > 0:
                if len(rows_time_filtered) == 1:
                    df.loc[x, "event_type"] = rows_time_filtered["event_type"].values[0]
                else:
                    common_event_type = self.event_type_majority_vote_closest(rows_time_filtered, x_time, ns, target_removed=True)
                    df.loc[x, "event_type"] = common_event_type
            
            else:
                x_time_lb = x_time - timedelta(minutes=m)
                x_time_ub = x_time + timedelta(minutes=m)
                rows_time_filtered = rows[(rows["time"] >= x_time_lb) & (rows["time"] <= x_time_ub)]
                rows_time_filtered = rows_time_filtered[rows_time_filtered["event_type"].isin([0,1])]
                if len(rows_time_filtered) == 0:
                    # mark as invalid and discard later
                    df.loc[x, "event_type"] = -1
                else:
                    
                    common_event_type = self.event_type_majority_vote_closest(rows_time_filtered, x_time, nm, target_removed=True)
                    df.loc[x, "event_type"] = common_event_type
                    
        # discard invalid samples
        df = df[df["event_type"] != -1].reset_index(drop=True)   
                
        return df
    
    def filter_event_type_5_6(self, dataframe, k, s, m, ns, nm, handle_5, handle_6, **kwargs):
        df = dataframe.copy()
        
        event_types = [0,1]
        if handle_5:
            event_types.append(5)
        if handle_6:
            event_types.append(6)
            
        df = df[df["event_type"].isin(event_types)].sort_values(by="time", ascending=True).reset_index(drop=True)
        
        #print("Take care of data: \n 14.05.2024, Event Type 5, HS18 Door1")
        dict_df_room_door = self.df_room_door_dict(df)
        df_return = pd.DataFrame(columns=list(df.columns))
        df_return = df_return.astype(df.dtypes)
        
        for room, room_dict in dict_df_room_door.items():
            for door, df_room_door in room_dict.items():  
                
                # deal with event type 4
                # deal with event type 5 and 6
                if handle_5 or handle_6:
                    df_room_door = self.handle_event_type_5_6(df_room_door, k=k, s=s, m=m, ns=ns, nm=nm)
                    
                df_return = pd.concat([df_return, df_room_door], axis=0)

        return df_return       
        
    def basic_cleaning_and_data_type_correction(self, dataframe:pd.DataFrame):
        # make copy of dataframe
        df = dataframe.copy()
        # drop nan values
        #print(df[df["Entering"].isna()])
        df.dropna(subset=["Entering"], inplace=True)
        
        # delete hidden file in folder data_HS19_2024-04-25
        df.loc[df["Entering"] == 'True', "Entering"] = 1
        df.loc[df["Entering"] == 'False', "Entering"] = 0
        
        df["event_type"] = pd.to_numeric(df["Entering"])
        
        # correct the data types
        numeric_cols = df.columns[2:]
        df[numeric_cols] = df[numeric_cols].astype(int)
        
        # delete hidden file in folder data_HS19_2024-04-25
        #df["event_type"] = df["Entering"].apply(lambda x: self.correct_entering_column(x))
        # convert columnnames to lowercase
        df.columns = df.columns.str.lower()
        
        # rename columns
        df = self.rename_columns(df, ["one_count_1", "one_count_2"], 
                                 ["sensor_one_support_count", "sensor_two_support_count"])


        # drop unneccessary columns
        df = df.drop(columns=["entering", "people_in", "people_out"])   
        return df
    
    def clean_raw_data(self, dataframe:pd.DataFrame, params:dict):
        
        # do basic cleaning and data type correction
        
    
        df = self.basic_cleaning_and_data_type_correction(dataframe) # 0.1sec
        raw_data = df.copy()
        
        filtering_params = params["filtering_params"]
        
        ## check if samples should be discarded or not
        #if filtering_params["discard_samples"]:  # 0.8sec
        #    # discard samples between 22:00 and 07:30
        #    lb = time(hour=7, minute=40, second=0)
        #    ub = time(hour=22, minute=00, second=0)
        #    df = self.discard_samples(df, lb, ub)
            

        if filtering_params["apply_filter"]:
            filter_mode = filtering_params["filter_mode"]
            if filter_mode == "discard":
                
                if filtering_params["handle_5"] or filtering_params["handle_6"]:
                    df = self.filter_event_type_5_6(dataframe=df, 
                                                    handle_5=filtering_params["handle_5"], handle_6=filtering_params["handle_6"],
                                                    **params["handle_56_params"])
                    
                df = self.filter_discard(dataframe=df, **filtering_params)
            
            elif filter_mode == "n_closest":
                
                if filtering_params["handle_5"] or filtering_params["handle_6"]:
                    df = self.filter_event_type_5_6(dataframe=df, 
                                                    handle_5=filtering_params["handle_5"], handle_6=filtering_params["handle_6"],
                                                    **params["handle_56_params"])
                
                df = self.filter_data_n_closest(dataframe=df, **filtering_params) # most basic filterings
                

            elif filter_mode == "time_window":
                
                if filtering_params["handle_5"] or filtering_params["handle_6"]:
                    df = self.filter_event_type_5_6(dataframe=df, 
                                                    handle_5=filtering_params["handle_5"], handle_6=filtering_params["handle_6"],
                                                    **params["handle_56_params"])

                df = self.filter_data_time_window(dataframe=df, **filtering_params)
                
            else:
                raise ValueError("Filter mode not supported") 
        else:
            event_types = [0,1]
            df = df[df["event_type"].isin(event_types)].reset_index(drop=True)
                
        df = df.sort_values(by="time", ascending=True).reset_index(drop=True)
    
        return df, raw_data

    ###### Preprocessing Application ########
    def apply_preprocessing(self, params:dict):
        cleaned_data, raw_data = self.clean_raw_data(self.raw_uncleaned_data.copy(), params)
        return cleaned_data, raw_data       
    
    
    
    
    
    #def event_type_majority_vote_closest(self, dataframe, refernce_time, n, targte_removed):
    #    df = dataframe.copy()
        
    #    if targte_removed:
    #        idx = abs(df["time"] - refernce_time).sort_values().index[:n]
    #    else:
    #        idx = abs(df["time"] - refernce_time).sort_values().index[1:n+1]
            
    #    filtered = df.loc[idx]
    #    value_counts = filtered["event_type"].value_counts()
    #    common_event_type = value_counts.idxmax()
        
    #    return common_event_type   
    
    #def correct_entering_column(self, entry):
    #    if entry == "True":
    #        return 1
    #    elif entry == "False":
    #        return 0
    #    else:
    #        return int(entry)
    
    #def filter_data_3(self, dataframe, k, ns, nm, s, ub, handle_5, handle_6, m):
    #    df = dataframe.copy()
        
    #    event_types = [0,1]
    #    if handle_5:
    #        event_types.append(5)
    #    if handle_6:
    #        event_types.append(6)
            
    #    df = df[df["event_type"].isin(event_types)].sort_values(by="time", ascending=True).reset_index(drop=True)
        
    #    print("Take care of data: \n 14.05.2024, Event Type 5, HS18 Door1")
    #    dict_df_room_door = self.df_room_door_dict(df)
    #    df_return = pd.DataFrame(columns=list(df.columns))
    #    df_return = df_return.astype(df.dtypes)
        
    #    for room, room_dict in dict_df_room_door.items():
    #        for door, df_room_door in room_dict.items():  
                
    #            # deal with event type 4
    #            # deal with event type 5 and 6
    #            if handle_5 or handle_6:
    #                df_room_door = self.handle_event_type_5_6(df_room_door,k=k, s=s, m=m, ns=ns, nm=nm)

    #            # deal with events with low directional support! 
    #            df_test = df_room_door.copy()
                
    #            # filter out samples with low support count
    #            df_test = df_test[(df_test["in_support_count"] < ub) 
    #                            & (df_test["out_support_count"] < ub)]
            
    #            for x in df_test.index:
    #                # use index to get row
    #                x_row = df_room_door.loc[x]
    #                x_time = x_row["time"]

    #                # select neighborhood of sample
    #                rows = self.get_neighborhood(df_room_door, x, k)
        
    #                # try time filter first -> more reliable
    #                x_time_lb = x_time - timedelta(seconds=s)
    #                x_time_ub = x_time + timedelta(seconds=s)
    #                rows_time_filtered = rows[(rows["time"] >= x_time_lb) & (rows["time"] <= x_time_ub)]
            
    #                # if only one sample in time window
    #                if len(rows_time_filtered) == 1:
    #                    # select make majority vote with the n closestest neighbors
    #                    common_event_type = self.event_type_majority_vote_closest(rows, x_time, nm, targte_removed=False)
                    
    #                # if more than one sample in time window     
    #                else:
    #                    # make majority vote with the samples in the time window
    #                    common_event_type = self.event_type_majority_vote_closest(rows_time_filtered, x_time, ns, targte_removed=False)
                        
    #                df_room_door.loc[x, "event_type"] = common_event_type
                    
    #            df_return = pd.concat([df_return, df_room_door], axis=0)

    #    return df_return