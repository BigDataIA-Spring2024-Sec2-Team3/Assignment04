import requests
from pathlib import Path
from lxml import etree
import re
import os
from dotenv import load_dotenv
import json

load_dotenv(override=True)
# Parse PDF with GROBID
file_path = os.getenv('AIRFLOW_FILES_PATH')
def parse_pdf_with_grobid(pdf_file):

    grobid_url = 'http://grobid_image:8070/api/processFulltextDocument'

    files = {'input': open(pdf_file, 'rb')}
    response: requests.Response = requests.post(grobid_url, files=files)

    xml_content = response.content
    # current_directory = Path(__file__).parent  # Get the current directory of this script
    # files_directory = current_directory.parent / 'files'  # Navigate to the 'files' directory at the same level as the current directory
    # files_directory.mkdir(exist_ok=True)  # Create the 'files' directory if it does not exist
    
    # Extract the base name of the PDF file to use for the XML file name
    pdf_file_name = Path(pdf_file).stem
    if file_path is not None:
        xml_file_path = file_path + '/' + f"{pdf_file_name}_content.xml" 

    with open(xml_file_path, 'wb') as file:
        file.write(xml_content)

    root = etree.fromstring(xml_content) # type: ignore

    text_content = root.xpath('//text()')
    parsed_text = ' '.join(text_content)

    return parsed_text, root
 
def extract_element_text(root, element_path, namespaces):
    element = root.find(element_path, namespaces=namespaces)
    return element.text.strip() if element is not None and element.text is not None else ''
 
def extract_metadata(root, filename):
    try:
        ns = {"tei": "http://www.tei-c.org/ns/1.0", "xml": "http://www.w3.org/XML/1998/namespace"}
 
        metadata = {
            'Filename': filename,
            'Title': extract_element_text(root, ".//tei:title[@type='main']", ns),
            'Header': extract_element_text(root, ".//tei:head", ns),
            'Paragraph': extract_element_text(root, ".//tei:p", ns),
            'Idno': extract_element_text(root, ".//tei:idno", ns),
            'Application': extract_element_text(root, ".//tei:desc", ns)
        }
 
        return metadata
 
    except etree.XMLSyntaxError as e:
        print(f"Error parsing XML: {str(e)}")
        return None
 
def save_metadata_to_json(metadata, pdf_file_path):
    import json
    print('file name-----',pdf_file_path)
    output_file_path = f"{pdf_file_path}_metadata.json"
    print('file_path -----',output_file_path)
    with open(output_file_path, 'w', encoding='utf-8') as file:
        json.dump(metadata, file, indent=2)
 
def save_metadata_to_xml(metadata, pdf_file_path):  
    print('file name-----',pdf_file_path)
    output_file_path = f"{pdf_file_path}_metadata.xml"
    print('file_path -----',output_file_path)
    root = etree.Element("metadata") # type: ignore
    for key, value in metadata.items():
        tag_name = key.replace(' ', '_')
        element = etree.SubElement(root, tag_name) # type: ignore
        element.text = value
    tree = etree.ElementTree(root)
    tree.write(output_file_path, encoding="utf-8", xml_declaration=True)
 
def PDF_XML_function(pdf_files): 
    # Process PDF files
    file_list =[]
    pdf_files = pdf_files.replace("'", '"')
    print('pdf FILES: ------', pdf_files)
    print('pdf type',type(pdf_files))
    try:
        file_list = json.loads(pdf_files)
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")

    # Dictionary to store all metadata            
    all_metadata = []

    for pdf_file in file_list:
        print(f"Processing {pdf_file}")

        if file_path is not None:
            _path = file_path + '/' + pdf_file

        print('path to file:----', _path)

        # Parse PDFs
        parsed_text, root = parse_pdf_with_grobid(_path)
        
        # Extract and save metadata
        metadata = extract_metadata(root, _path)
        pdf_file_name = Path(pdf_file).stem
        if file_path is not None:
            pdf_file_path = file_path + '/' + pdf_file_name
        if metadata:
            all_metadata.append(metadata)
            # file_prefix = f"{_path.split('-')[0]}_{_path.split('-')[1].split('.')[0]}"
            save_metadata_to_json(metadata,pdf_file_path)
            save_metadata_to_xml(metadata,pdf_file_path)