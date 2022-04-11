
import string
import xmltodict
import re
import json

def clean_sentence(sentence):
    return " ".join(re.sub(r'\r\n', '', sentence).split())

def find_gender(text):
    gender_text_list = ["man", "boy", "he", "him", "woman", "she", "her", "gril"]
    gender_count = {}
    gender_count["male"] = 0
    gender_count["female"] = 0
    for i in range(len(gender_text_list)):
        count = text.count(gender_text_list[i])

        if (i <= 3):
            gender_count["male"] += count
        else:
            gender_count["female"] += count
    
    if gender_count["male"] > gender_count["female"]:
        return -1
    elif gender_count["male"] < gender_count["female"]:
        return 1
    else:
        return 0

def find_age(text):
    first_sentence = re.split("\.|, ", text)[0]
    age = -1
    if ("-year-old" in first_sentence or "year-old" in first_sentence or "year old" in first_sentence or "year" in first_sentence):
        index = first_sentence.find("-year-old")
        if (index != -1):
            age = int(first_sentence[index - 2:index])
        else:
            index = first_sentence.find("year-old")
            if (index != -1):
                age = int(first_sentence[index - 3:index-1])
            else:
                index = first_sentence.find("year old")
                if (index != -1):
                    age = int(first_sentence[index - 3:index-1])
                else:
                    index = first_sentence.find("year")
                    age = int(first_sentence[index - 3:index-1])
    elif ("yo" in first_sentence or "y/o" in first_sentence):
        index = first_sentence.find("yo")
        if (index != -1):
            if (index - 3 >= 0):
                age = int(first_sentence[index - 3:index].strip())
            else:
                age = int(first_sentence[index - 2:index].strip())
        else:
            index = first_sentence.find("y/o")
            age = int(first_sentence[index - 3:index-1])
    else:
        # some extreme cases, manually processing
        if ("48 m with" in first_sentence):
            age = 4
        elif ("74m hx of" in first_sentence):
            age = 6
        else:
            age = 0
    
    return age

def find_health(text):
    # all patients are unhealthy
    return 0

def main():
    with open("./topics2021.xml", encoding='utf-8') as topic_file:
        topics_list = xmltodict.parse(topic_file.read())["topics"]['topic']

        topics = []
        for topic in topics_list:
            topic_dict = {}
            topic_dict["num"] = topic['@number']
            topic_dict['query'] = clean_sentence(topic['#text']).lower()
            topic_dict["gender"] =  find_gender(topic_dict['query'])# default is all
            topic_dict["age"] = find_age(topic_dict['query'])
            topic_dict["healthy"] = find_health(topic_dict['query'])# default is No

            topics.append(topic_dict)
            # print("gender: {}, age: {}".format(topic_dict["gender"], topic_dict["age"]))
        topic_str = json.dumps(topics)
        topic_json = open("./topic.json", "w")
        topic_json.write(topic_str)
        topic_json.close()

if __name__ == "__main__":
    main()