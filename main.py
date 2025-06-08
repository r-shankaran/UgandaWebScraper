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

# Get level of observation
level_of_observation_columns = []
previous_line = 0
for i in range(1, len(focus_terms)):
    if focus_terms[previous_line] == "AGSEC8B":
        level_of_observation_columns.append("type of animal")
    elif focus_terms[previous_line] in module_descriptions:
        # special cases in which level of observation is not explicit
        if focus_terms[previous_line] == "Extension Services (NAADS) Household level":
            level_of_observation_columns.append("household level")
        elif focus_terms[previous_line] == "Farm Implements and Machinery Implement item":
            level_of_observation_columns.append("item")
        elif focus_terms[previous_line] in ["Identification Particulars", "Land use and planning", "Deliveries at the facility", "Government safety net programmes", "Community characteristics"]:
            level_of_observation_columns.append("ea")
        elif focus_terms[previous_line] == "Consumption aggregate dataset":
            level_of_observation_columns.append("household level")
        else: # regular instances
            level_of_observation_columns.append(focus_terms[i].lower())
    previous_line += 1
level_of_observation_columns.append("household level")
# Clean up level of observation entries to be uniform
for i in range(1, len(level_of_observation_columns)):
    if "level of observation:" in level_of_observation_columns[i]:
        level_of_observation_columns[i] = level_of_observation_columns[i].replace("level of observation:", "")
    elif "level of observation: " in level_of_observation_columns[i]:
        level_of_observation_columns[i] = level_of_observation_columns[i].replace("level of observation:", "")
    level_of_observation_columns[i] = level_of_observation_columns[i].lstrip()
print("level of obs:", level_of_observation_columns)

# get rid of the Data File column in df because it is separated into module name and description now.
df.drop(columns=["Data file"], inplace=True)
main_df = df.copy()
print(main_df["module_name"])
# For every module in LSMS, extract variable_name, variable_description, data_type, and unique_id
all_dictionaries = []
print("Extracting variable names, descriptions, and types...")
for i in range(len(main_df)):
    print("Extracting from module {}...".format(main_df["module_name"][i]))
    current_module = main_df["module_name"][i]
    current_module_description = main_df["module_description"][i]
    # url to a specific module of the Uganda LSMS
    if i == 31:
        url_index = i + 1
    elif 32 <= i < 39:
        url_index = i + 2
    elif 39 <= i < 57:
        url_index = i + 3
    elif i >= 57:
        url_index = i + 5
    else:
        url_index = i
    module_url = url + "/F{}?file_name={}".format(url_index + 1, current_module)
    print(module_url)
    response = requests.get(module_url)
    soup = BeautifulSoup(response.content, 'html.parser')
    # Search for containers that have the info we want -- which is variable name and link to variable webpage
    var_ids = soup.find_all("a", class_="var-id text-break")
    list_of_dictionaries = []

    # get unique id
    if level_of_observation_columns[i] in ["household level", "household", "type of animal"]:
        unique_id = var_ids[0].text
    elif level_of_observation_columns[i] == "parcel level":
        unique_id = var_ids[1].text
    elif level_of_observation_columns[i] == "parcel-plot level":
        if current_module == "AGSEC3B_1":
            unique_id = var_ids[3].text
        else:
            unique_id = var_ids[1].text
    elif level_of_observation_columns[i] in ["parcel-plot-crop level", "livestock type", "livestock input type", "livestock product (meat)", "livestock product (eggs)", "extension source"]:
        unique_id = var_ids[1].text
    elif level_of_observation_columns[i] in ["service type", "consumption item", "type of contraception", "type of help with the delivery", "reason for not being able to get pregnant", "type of food"]:
        unique_id = var_ids[2].text
    elif level_of_observation_columns[i] == "ea":
        unique_id = var_ids[0].text
    elif level_of_observation_columns[i] == "facility type":
        unique_id = var_ids[2].text
    elif level_of_observation_columns[i] == "roster title":
        unique_id = var_ids[1].text
    elif level_of_observation_columns[i] in ["water facility type", "payment id", "year", "type of meeting", "drug supplies", "staffing position", "supervisor/monitor", "problem type", "class", "data element, period", "limiting factor", "positions", "ea/type of initiative", "item type", "group code", "ngo", "item", "resource"]:
        unique_id = var_ids[2].text
    elif level_of_observation_columns[i] in ["individual", "fuel type", "enterprise", "asset type", "shock type", "item"]:
        unique_id = var_ids[1].text
    else:
        unique_id = "Error"
    current_level_of_observation = level_of_observation_columns[i]
    print(current_level_of_observation)
    print(unique_id)


    # For every variable in the module, extract variable description and variable type by going to the variable webpage
    for i in tqdm(range(len(var_ids))):
        var_link = var_ids[i]["href"]
        var_response = requests.get(var_link)
        var_soup = BeautifulSoup(var_response.content, 'html.parser')
        var_data = var_soup.find_all("div", class_="variable-container")
        # find variable description and data type
        var_description = var_data[0].find("h2").text.strip()
        data_type = var_data[0].find("div", class_="fld-inline sum-stat sum-stat-var_intrvl").text.split(":")[1].strip()

        list_of_dictionaries.append({"variable_name": var_ids[i].text, "variable_description": var_description, "module_name": current_module, "module_description": current_module_description, "data_type": data_type, "level_of_observation": current_level_of_observation, "unique_id": unique_id})


    all_dictionaries += list_of_dictionaries

pd.DataFrame(all_dictionaries).to_csv("~/Downloads/uganda_metadata.csv", index=False)