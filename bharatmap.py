from bs4 import BeautifulSoup
import csv

# Step 1: Read the HTML content from a file
with open('input.html', 'r') as file:
    html_content = file.read()

# Step 2: Parse the HTML content with BeautifulSoup
soup = BeautifulSoup(html_content, 'html.parser')

# Step 3: Find all rows in the table
rows = soup.find_all('tr')

# Step 4: Open a CSV file to write to
with open('output.csv', 'w', newline='') as file:
    writer = csv.writer(file)

    # Step 5: Loop through each row
    for row in rows:
        # Extract all <div> values inside each row
        cols = [div.get_text() for div in row.find_all('div')]
        # Write the extracted values to the CSV file
        writer.writerow(cols)

print("CSV file has been written successfully.")
