from bs4 import BeautifulSoup
import requests
import pandas as pd
from io import StringIO
from tqdm import tqdm
import nltk

url = 'https://microdata.worldbank.org/index.php/catalog/3902/data-dictionary'
response = requests.get(url)
soup = BeautifulSoup(response.content, features="html.parser")
response = requests.get(url)
df = pd.read_html(response.content)[0]

# get raw text (needed for module descriptions)
for script in soup(["script", "style"]):
    script.extract()
text = soup.get_text() # get text
lines = (line.strip() for line in text.splitlines()) # break into lines and remove leading and trailing space on each
chunks = (phrase.strip() for line in lines for phrase in line.split("  ")) # break multi-headlines into a line each
text = '\n'.join(chunk for chunk in chunks if chunk) # drop blank lines
text = text.splitlines()

# Extract module names from df
print("Extracting module names...")

df["module_name"] = df["Data file"].apply(lambda x: x.split()[0])
# Extract module descriptions from raw text - if preceding line is an element of module_names, add string to
# module_descriptions. (must do this since there are line breaks that df doesn't recognize)
print("Extracting module descriptions...")

module_descriptions = []
focus_terms = text[text.index('Data file'):text.index('Back to Catalog')]
previous_line = 0
print("focus terms:", focus_terms)
for i in range(1, len(focus_terms)):
    if focus_terms[previous_line] in df["module_name"].values:
        if focus_terms[i].isdigit(): # note: there is no description for module AGSEC8B
            module_descriptions.append("None")
        else:
            module_descriptions.append(focus_terms[i])
    previous_line += 1
df["module_description"] = module_descriptions

# get rid of the Data File column in df because it is separated into module name and description now.
df.drop(columns=["Data file"], inplace=True)
main_df = df.copy()
print(main_df["module_name"])
# For every module in LSMS, extract variable_name, variable_description, data_type, and unique_id
all_dictionaries = []
for i in range(len(main_df)):
    all_dictionaries.append({"module_name": main_df["module_name"][i], "module_description": main_df["module_description"][i], "included": "", "reason": ""})

pd.DataFrame(all_dictionaries).to_csv("~/Downloads/uganda_modules.csv", index=False)