import csv,os,sys

utils_path = os.path.abspath(os.path.join(os.getcwd(), '..\..'))
sys.path.append(utils_path)
from Utils.URLclass import Article

def instantiate_from_csv(csv_filepath: str) -> list[Article]:
    Articles = []
    with open(csv_filepath, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            # Convert each row to a dictionary
            # row_data = dict(row)

            # Instantiate your Pydantic model and add it to the list
            try:
                article_instance = Article(**row)
                Articles.append(article_instance)
            except Exception as e:
                print('Validation Error:', e)
    print(len(Articles))
    return Articles


def write_models_to_csv(Articles: list[Article], csv_filepath: str):
    """Function to write model instances to a new CSV file"""
    # Check if models list is not empty
    if not Articles:
        print("No models to write.")
        return

    # Collect data from Pydantic models
    data = [Article.model_dump() for Article in Articles]
    
    # Get the headers from the first model (assuming all models have the same fields)
    headers = data[0].keys()

    with open(csv_filepath, mode='w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers)
        
        # Write the header
        writer.writeheader()
        
        # Write each model's data
        for row in data:
            writer.writerow(row)


csv_file = "../../data/items.csv"
articles = instantiate_from_csv(csv_file)

csv_file_path = '../../data/Validation_data.csv'
write_models_to_csv(articles, csv_file_path)

