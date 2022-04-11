#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import argparse
import xmltodict
import glob
import re
from multiprocessing import Pool
import numpy as np



def get_path_of_all_documents(dataset_path):

    docs_list = []
    for f in glob.glob(dataset_path+"/**/**/*.xml", recursive=True):
        docs_list.append(f)
    
    return docs_list

def clean_sentence(sentence):
    return " ".join(re.sub(r'\r\n', '', sentence).split())

def clean_inclusion_exclusion(str_words, inclusion_word, exclusion_word, threshold_count = 5):
    inclusion_index = [n for n in np.arange(len(str_words)) if str_words.find(inclusion_word, n) == n]
    exclusion_index = [n for n in np.arange(len(str_words)) if str_words.find(exclusion_word, n) == n]

    ii_counter = 0
    ei_counter = 0

    inclusion = ""
    exclusion = ""

    while ii_counter < len(inclusion_index) or ei_counter < len(exclusion_index):

        if (ii_counter <= ei_counter and ii_counter < len(inclusion_index) and ei_counter < len(exclusion_index)):
            if (inclusion_index[ii_counter] + len(inclusion_word) < exclusion_index[ei_counter]):
                inclusions = str_words[inclusion_index[ii_counter] + len(inclusion_word):exclusion_index[ei_counter]]
                if (len(inclusions.split(" ")) > threshold_count):
                    inclusion += " " + inclusions
                ii_counter += 1
            else:
                ei_counter += 1
        else:
            if (ii_counter == len(inclusion_index) and ei_counter < len(exclusion_index)):
                exclusions =  str_words[exclusion_index[ei_counter] + len(exclusion_word):]
                if (len(exclusions.split(" ")) > threshold_count):
                    exclusion += " " + exclusions
                break
            elif (ei_counter == len(exclusion_index)):
                exclusions =  str_words[exclusion_index[ei_counter-1] + len(exclusion_word):]
                if (len(exclusions.split(" ")) > threshold_count):
                    exclusion += " " + exclusions
                break
            else:
                exclusions =  str_words[exclusion_index[ei_counter] + len(exclusion_word):inclusion_index[ii_counter]]
                if (len(exclusions.split(" ")) > threshold_count):
                    exclusion += " " + exclusions
            ei_counter += 1
    
    return (inclusion, exclusion)

def get_criteria(criteria):
    eligibility_dict = {}
    clean_criteria = clean_sentence(criteria).lower()

    try:
        if ("inclusion criteria:" in clean_criteria) and ("exclusion criteria:" in clean_criteria):
            inclusion_word = "inclusion criteria:"
            exclusion_word = "exclusion criteria:"
            inclusion_exclusion = clean_inclusion_exclusion(clean_criteria, inclusion_word=inclusion_word, exclusion_word=exclusion_word)
            eligibility_dict["inclusion"] = inclusion_exclusion[0][2:]
            eligibility_dict["exclusion"] = inclusion_exclusion[1][2:]
        elif ("inclusion criteria" in clean_criteria) and ("exclusion criteria" in clean_criteria):
            inclusion_word = "inclusion criteria"
            exclusion_word = "exclusion criteria"
            inclusion_exclusion = clean_inclusion_exclusion(clean_criteria, inclusion_word=inclusion_word, exclusion_word=exclusion_word)
            eligibility_dict["inclusion"] = inclusion_exclusion[0][2:]
            eligibility_dict["exclusion"] = inclusion_exclusion[1][2:]
        elif ("inclusion:" in clean_criteria) and ("exclusion:" in clean_criteria):
            inclusion_word = "inclusion:"
            exclusion_word = "exclusion:"
            inclusion_exclusion = clean_inclusion_exclusion(clean_criteria, inclusion_word=inclusion_word, exclusion_word=exclusion_word)
            eligibility_dict["inclusion"] = inclusion_exclusion[0][2:]
            eligibility_dict["exclusion"] = inclusion_exclusion[1][2:]
        elif ("inclusion" in clean_criteria) and ("exclusion" in clean_criteria):
            inclusion_word = "inclusion"
            exclusion_word = "exclusion"
            inclusion_exclusion = clean_inclusion_exclusion(clean_criteria, inclusion_word=inclusion_word, exclusion_word=exclusion_word)
            eligibility_dict["inclusion"] = inclusion_exclusion[0][2:]
            eligibility_dict["exclusion"] = inclusion_exclusion[1][2:]
        else:
            eligibility_dict["inclusion"] = clean_criteria
            eligibility_dict["exclusion"] = ""
    except:
        print(clean_criteria)

    return eligibility_dict

def check_eligibility(eligibility):
    eligibility_dict = {}
    eligibility_dict["gender"] = 0 # default is all
    eligibility_dict["min_age"] = 0
    eligibility_dict["max_age"] = 999
    eligibility_dict["healthy"] = 0 # default is No

    if (eligibility["gender"] == "Female"):
        eligibility_dict["gender"] = 1
    elif (eligibility["gender"] == "Male" ):
        eligibility_dict["gender"] = -1
    
    min_age = eligibility["minimum_age"].split(" ")[0]
    max_age = eligibility["maximum_age"].split(" ")[0]

    if (min_age != "N/A"):
        eligibility_dict["min_age"] = int(min_age)
    else:
        eligibility_dict["min_age"] = min_age
    

    if (max_age != "N/A"):
        eligibility_dict["max_age"] = int(max_age)
    else:
        eligibility_dict["max_age"] = max_age

    if ("healthy_volunteers" in eligibility and eligibility["healthy_volunteers"] == "Yes"):
         eligibility_dict["healthy"] = 1

    # eligibility_dict["inclusion"] = criteria["inclusion"]
    # eligibility_dict["exclusion"] = criteria["exclusion"]
    

    return eligibility_dict

def filter_feature_in_doc(raw_doc):
    feature_doc = {}
    feature_doc["id"] = raw_doc["clinical_study"]["id_info"]["nct_id"]
    feature_doc["contents"] = ""

    # sparse index 
    # if ("brief_summary" in raw_doc["clinical_study"]):
    #     feature_doc["contents"] += clean_sentence(raw_doc["clinical_study"]["brief_summary"]["textblock"]) + " "
    # if ("detailed_description" in raw_doc["clinical_study"]):
    #     feature_doc["contents"] += clean_sentence(raw_doc["clinical_study"]["detailed_description"]["textblock"])
    # if ("eligibility" in raw_doc["clinical_study"]):
    #     feature_doc["eligibility"] = check_eligibility(raw_doc["clinical_study"]["eligibility"])
    #     critera_dict = {}
    #     if ("criteria" in raw_doc["clinical_study"]["eligibility"]):
    #         critera_dict = get_criteria(raw_doc["clinical_study"]["eligibility"]["criteria"]["textblock"])
    #         inclusion_criteria = critera_dict["inclusion"]
    #         exclusion_criteria = critera_dict["exclusion"]
    #         feature_doc["inclusion"] = inclusion_criteria
    #         feature_doc["exclusion"] = exclusion_criteria
    #     else:
    #         feature_doc["inclusion"] = ""
    #         feature_doc["exclusion"] = ""
    # else:
    #     feature_doc["eligibility"] = {}
    #     feature_doc["inclusion"] = ""
    #     feature_doc["exclusion"] = ""
    

    # dense index
    if ("brief_summary" in raw_doc["clinical_study"]):
        feature_doc["contents"] += clean_sentence(raw_doc["clinical_study"]["brief_summary"]["textblock"]) + " "
    if ("detailed_description" in raw_doc["clinical_study"]):
        feature_doc["contents"] += clean_sentence(raw_doc["clinical_study"]["detailed_description"]["textblock"])
    if ("eligibility" in raw_doc["clinical_study"]):
        feature_doc["eligibility"] = check_eligibility(raw_doc["clinical_study"]["eligibility"])
        critera_dict = {}
        if ("criteria" in raw_doc["clinical_study"]["eligibility"]):
            critera_dict = get_criteria(raw_doc["clinical_study"]["eligibility"]["criteria"]["textblock"])
            inclusion_criteria = critera_dict["inclusion"]
            exclusion_criteria = critera_dict["exclusion"]
            feature_doc["contents"] += " inclusion: " + inclusion_criteria + " exclusion:" + exclusion_criteria
    else:
        feature_doc["eligibility"] = {}

    return feature_doc

def save_json(document_path):
    
    with open(document_path, encoding='utf-8') as xml_file:
        raw_doc = xmltodict.parse(xml_file.read())

        tmp_doc = {}
        tmp_doc["id"] = raw_doc["clinical_study"]["id_info"]["nct_id"]
        # filter documents without summary/description/eligibility because they are not approaved by FDA
        if ("brief_summary" not in raw_doc["clinical_study"] and "detailed_description" not in raw_doc["clinical_study"]):
            tmp_doc["contents"] = clean_sentence(raw_doc["clinical_study"]["brief_title"])
            # tmp_json_str = json.dumps(tmp_doc)
            # tmp_json_file = open("./dense_data/no_description_doc/"+tmp_doc["id"]+".json", "w")
            # tmp_json_file.write(tmp_json_str)
            # tmp_json_file.close()
        else:
            feature_doc = filter_feature_in_doc(raw_doc)

            doc_json_str = json.dumps(feature_doc)
            doc_json_file = open("./dense_data/"+feature_doc["id"]+".json", "w")
            doc_json_file.write(doc_json_str)
            doc_json_file.close()

def main(args):

    documents_path = get_path_of_all_documents(args.dataset)

    # convert a xml document into json format
    # doc_dict = {}
    # test_file1 = r".\dataset\ClinicalTrials.2021-04-27.part1\NCT0078xxxx\NCT00783718.xml"
    # test_file2 = r".\dataset\ClinicalTrials.2021-04-27.part1\NCT0078xxxx\NCT00787644.xml"
    # test_file3 = r".\dataset\ClinicalTrials.2021-04-27.part1\NCT0078xxxx\NCT00784927.xml"
    # test_file4 = r".\dataset\ClinicalTrials.2021-04-27.part1\NCT0078xxxx\NCT00789997.xml"
    # test_file5 = r".\dataset\ClinicalTrials.2021-04-27.part1\NCT0000xxxx\NCT00000139.xml"

    # save_json(test_file5)

    pool = Pool(16)
    _ = pool.map(save_json, documents_path)

    # for d in documents_path:
    #     save_json(d)
    
    pool.close()
    pool.join()

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description = "A module is used to parse and process documents.")
    parser.add_argument("-d", "--dataset", help = "The path to the dataset; the preprocessed result will also be stored at the same directory")
    args = parser.parse_args()
    main(args)