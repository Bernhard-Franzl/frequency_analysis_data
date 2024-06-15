from preprocessing.preprocessor import SignalPreprocessor, CoursePreprocessor
import json

room_to_id ={"HS18":0, "HS 18":0, "HS19":1, "HS 19": 1}
door_to_id = {"door1":0, "door2":1}


################ Signal Preprocessing ################
# Due to its size the raw light gate data is not included in the repository.
# The preprocessed data can be found in the data folder.
data_path = "/home/berni/data_06_06/archive"
# load parameters
path_to_json = "parameters/preprocessing_parameters.json"
params = json.load(open(path_to_json, "r"))
# apply preprocessing
preprocessor = SignalPreprocessor(data_path, room_to_id, door_to_id)
cleaned_data, raw_data = preprocessor.apply_preprocessing(params)
# save data
preprocessor.save_to_csv(cleaned_data, "data", "frequency_data")


################ Course Preprocessing ################
path_to_raw_courses = "data/raw"
preprocessor = CoursePreprocessor(path_to_raw_courses, 
                                  room_to_id=room_to_id, door_to_id=door_to_id)

cleaned_course_info, cleaned_course_dates = preprocessor.apply_preprocessing()

preprocessor.save_to_csv(cleaned_course_info, "data", "course_info")
preprocessor.save_to_csv(cleaned_course_dates, "data", "course_dates")
