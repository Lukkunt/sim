import pandas as pd
import click
import requests
r = requests.get('https://api.github.com/events')
print(r)
