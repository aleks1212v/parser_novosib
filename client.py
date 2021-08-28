# -*- coding: utf-8 -*-
"""
Created on Fri Aug 27 21:36:03 2021

@author: aleks1212
"""

import requests
import json
import pandas as pd
import matplotlib.pyplot as plt

body = None
keys= None
r = requests.post("http://localhost:5000/", params = keys, data = body)

response = json.loads(r.content.decode('utf-8'))

addrframe = pd.DataFrame(response['addr'], columns = ['addr'])
print(addrframe)
for i in range( addrframe.shape[0]):
    priceframe = pd.DataFrame(response['price'][i].items(), columns = ['date', 'price'])
    print(priceframe)
    plt.plot(priceframe['date'], priceframe['price'], label = addrframe.loc[i][0])
    plt.legend()
plt.show()
