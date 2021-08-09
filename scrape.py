import datetime
import csv
import io

import requests_cache
requests_cache.install_cache('cache')
from requests_html import HTMLSession
from sqlite_utils import Database
import tqdm

session = HTMLSession()

CCNI_CHARITY_URL = "https://www.charitycommissionni.org.uk/charity-details/?regId={}&subId=0"
CCNI_EXPORT_URL = "https://www.charitycommissionni.org.uk/umbraco/api/charityApi/ExportSearchResultsToCsv/?include=Linked&include=Removed"

def scrape_ccni_record(regno):
    r = session.get(CCNI_CHARITY_URL.format(regno))

    headings = ("Public benefits", "What your organisation does", "Charitable purposes", "Governing document")
    record = {
        "regno": regno,
        "scraped": datetime.datetime.now(),
        "trustees": None,
        "employees": None,
        "volunteers": None,
        **{h.lower(): None for h in headings}
    }

    for block in r.html.find(".pcg-charity-details__block"):
        heading = block.find("h3", first=True).text

        if heading in headings:
            record[heading.lower()] = " ".join([p.text for p in block.find("p")])

    for fact in r.html.find(".pcg-charity-details__fact"):
        heading = fact.find(".pcg-charity-details__purpose", first=True).text.lower()
        value = fact.find(".pcg-charity-details__amount", first=True).text.replace(",", "")
        if value == 'N/A':
            record[heading] = None
            continue
        try:
            record[heading] = int(value)
        except ValueError:
            print("Could not convert: {}".format(
                fact.find(".pcg-charity-details__amount", first=True).text.replace(",", "")
            ))
            record[heading] = None

    return record

def get_ccni_records_generator(reader):
    for row in reader:
        for k in row:
            if row[k] == '':
                row[k] = None
        for f in ("Date registered",):
            if row.get(f):
                row[f] = datetime.datetime.strptime(row[f], "%d/%m/%Y")
        for f in ("Date for financial year ending",):
            if row.get(f):
                row[f] = datetime.datetime.strptime(row[f], "%d %B %Y")
        for f in ("Total income", "Total spending", "Charitable spending", "Income generation and governance", "Retained for future use"):
            if row.get(f):
                row[f] = int(row[f])
        for f in ("What the charity does", "Who the charity helps", "How the charity works"):
            if row.get(f):
                line_reader = csv.reader([row[f]])
                row[f] = list([l for l in line_reader][0])
        if "" in row:
            del row['']
        row['last_updated'] = datetime.datetime.now()
        yield row

def get_ccni_records(db):
    r = session.get(CCNI_EXPORT_URL)
    reader = csv.DictReader(io.StringIO(r.text))
    db["ccni_main"].insert_all(get_ccni_records_generator(tqdm.tqdm(reader)), pk="Reg charity number", replace=True)


# db["ccni_scrape"].upsert(record, pk="regno")

db = Database("results.db")
print("Fetching NI Charity Register")
get_ccni_records(db)
print("")
print("Scraping data from NI Charity Register")
for row in tqdm.tqdm(db["ccni_main"].rows_where(select='[Reg charity number]')):
    record = scrape_ccni_record(row["Reg charity number"])
    db["ccni_scrape"].upsert(record, pk="regno")