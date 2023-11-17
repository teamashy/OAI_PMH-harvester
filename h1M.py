import requests
import time
import re

base_url = 'http://127.0.0.1:8000/oai-pmh'
filename_prefix = 'metadata'
resumption_token = ''
counter = 1

session = requests.Session()

tm1 = time.perf_counter()
while True:
    req_url = base_url + f'?verb=ListRecords&{"metadataPrefix=oai_dc" if resumption_token == "" else f"resumptionToken={resumption_token}"}'
    print(f"Harvesting {req_url}")

    state = 0
    response = None

    while state == 0 or (response is not None and response.status_code == 503):
        response = session.get(req_url)

        if response.status_code == 503:
            sleep = int(response.headers.get('Retry-After', 0))

            if sleep <= 0 or sleep > 86400:
                state = 1
            else:
                print(f"Sleeping for {sleep} seconds")
                time.sleep(sleep)
        else:
            state = 1

    content = response.content.decode('utf-8')
    records = content.count('<metadata>')
    filename = f"{filename_prefix}_{counter}.xml"  # Generate the filename with the counter

    print(f"Saving response with {records} records to {filename}")
    with open(filename, "w", encoding="utf-8") as file:
        file.write(content)


    counter += 1  # Increment the counter

    resumption_token = ''

    pattern = r"<resumptionToken[^>]*>([^<]+)</resumptionToken>"
    match = re.search(pattern, content)

    if match:
       resumption_token = match.group(1)

       print(resumption_token)

    else:
        resumption_token = ''
        print("the resumption token is empty")
        break

tm2 = time.perf_counter()
print(f" full harvest Finished in {(tm2 - tm1)/60} mins")

