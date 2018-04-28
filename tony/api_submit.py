# coding: utf-8

import requests

files={'files': open('sample_submission.csv','rb')}

data = {
    "user_id": "tnlin",   #user_id is your username which can be found on the top-right corner on our website when you logged in.
    "team_token": "157911850c1d57e0ac45ef110ed2f3a823e1e962300f6f1e8e1abe7c254431dc", #your team_token.
    "description": 'Baseline model',  #no more than 40 chars.
    "filename": "submission.csv", #your filename
}

url = 'https://biendata.com/competition/kdd_2018_submit/'

response = requests.post(url, files=files, data=data)

print(response.text)


