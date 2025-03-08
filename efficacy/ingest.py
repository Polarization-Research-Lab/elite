'''
DOWNLOAD
'''
import os, zipfile
import io 
import urllib.request
import dotenv

url = f"https://s3.amazonaws.com/pp-projects-static/congress/bills/{os.environ['CURRENT_CONGRESS']}.zip"
output_folder = '.tmp/'

# Download the ZIP file into memory
with urllib.request.urlopen(url) as response:
    zip_content = io.BytesIO(response.read())

# Unzip the content directly from memory
with zipfile.ZipFile(zip_content, 'r') as zip_ref:
    # Create the output folder if it doesn't exist
    os.makedirs(output_folder, exist_ok=True)

    # Extract all files from the ZIP and save them
    zip_ref.extractall(output_folder)

'''
PROCESS
'''
import os
import pandas as pd
import numpy as np
import xml.etree.ElementTree as ET

# data from: https://www.propublica.org/datastore/dataset/congressional-data-bulk-legislation-bills

# Bill Status
bill_status = []

# Cosponsorship
bill_sponsors = []

# Define a function to process XML files and select child nodes
def process_single_xml(xml_file_path):
    try:
        tree = ET.parse(xml_file_path)
        root = tree.getroot()
        global bill_sponsors
        global bill_status

        # Get the bill number
        bill_element = root.find('./bill/number')
        if bill_element is not None:
            billnumber = bill_element.text
            # print(f"Bill Number: {billnumber}")

        # Get the bill type
        bill_element = root.find('./bill/type')
        if bill_element is not None:
            billtype = bill_element.text
            # print(f"Bill Type: {billtype}")

        bill_id = billtype + billnumber
        #print(bill_id)
        policy_area = None
        # Get policy areas
        items = root.findall('./bill/policyArea/name')
        for policy_element in items:
            # Do something with the action element, e.g., print its text content
            policy_area = policy_element.text
            # print(f"Policy Areas: {policyArea}")

        # Get the Sponspor
        bioguideid_element = root.find('./bill/sponsors/item/bioguideId')
        if bioguideid_element is not None:
            bioguideid = bioguideid_element.text
            # print(f"Sponsor: {bioguideid}")
            row = {'bill_id': bill_id, "bioguide_id": bioguideid, "sponsor_type": "sponsor"}
            bill_sponsors.append(row)

        # Get cosponsors
        items = root.findall('./bill/cosponsors/item/bioguideId')
        for cosponsor_element in items:
            # Do something with the cosponsor element, e.g., print its text content
            cobioguideid = cosponsor_element.text
            # print(f"CoSponsor: {cobioguideid}")
            row = {'bill_id': bill_id, "bioguide_id": cobioguideid, "sponsor_type": "cosponsor"}
            bill_sponsors.append(row)

        # Get action steps
        passed_house = 'No'
        passed_senate = 'No'
        to_president = "No"
        signed = "No"

        items = root.findall('./bill/actions/item/actionCode')
        for action_element in items:
            # Do something with the action element, e.g., print its text content
            action_code = action_element.text

            if action_code == 'E30000' or action_code == 'E40000' or action_code == '36000':
                signed = 'Yes'
            if action_code == '8000':
                passed_house = 'Yes'
            if action_code == '17000':
                passed_senate = 'Yes'
            if action_code == 'E20000' or action_code == '28000':
                to_president = 'Yes'

        row = {'bill_id': bill_id, "action_code": action_code, "introduced": "Yes", "passed_house": passed_house, "passed_senate": passed_senate, "to_president": to_president, "signed": signed, "policy_area": policy_area}
        bill_status.append(row)

    except ET.ParseError as e:
        print(f"Error parsing {xml_file_path}: {e}")
    except Exception as e:
        print(f"Error processing {xml_file_path}: {e}")


# Get the current directory
for root, _, files in os.walk(f".tmp/congress/data/{os.environ['CURRENT_CONGRESS']}/bills/"):
    for filename in files:
        if filename.endswith('.xml'):
            xml_file_path = os.path.join(root, filename)
            process_single_xml(xml_file_path)

columns = ['bill_id', "action_code", "introduced", "passed_house", "passed_senate", "to_president", "signed", "policy_area"]
bill_status = pd.DataFrame(bill_status, columns=columns)

# Cosponsorship
columns = ["bill_id", "bioguide_id", "sponsor_type"]
bill_sponsors =pd.DataFrame(bill_sponsors, columns=columns)

merged = pd.merge(bill_sponsors, bill_status, how='left', left_on='bill_id', right_on='bill_id')
merged.to_csv('.tmp/bills.csv', index = False)











