import urllib.request
import re
import time
import json
import psycopg2 # needed to load data into PostgreSQL DB

"""
------- ANSATZ -------
Erstellung einer Menge (keine Duplikate) von Link-Objekten, welche dann iterativ in eine PostgreSQL Datenbank eingebunden werden.
 
Link_Objekt: 
{
  URL: url
  Sprache: ['DE', 'EN']
  Internal: [True, False]
}
"""

def get_links_from_url(url):
  
  request_object = urllib.request.Request(url, headers={"User-Agent": "Barnabas' Crawler"})

  file = urllib.request.urlopen(request_object)

  dump = str(file.read()) # html of page
  dump = dump.replace('\\n', '').replace(' ', '') # remove newlines and whitespace

  # search for anchor tags and find all occurences
  anchor_regex = re.compile(r'(<a)(.*?)(</a>)')
  anchors = anchor_regex.findall(dump)
  # search for href tags
  href_regex = re.compile(r'href="(.*?)"')

  # <a href="google.com" >Google</a>

  match_objects = []

  for anchor in anchors:
    match_object = href_regex.search(anchor[1]) # anchor[1] because anchor[0] is the opening a tag and anchor[2] is the closing a tag
    if match_object:
      match_object_url = match_object.group(1) # group(1) refers to contents within quotation marks of href

      # if url is neither internal nor external, it was probably commented out (as can be seen in the source of the home page)
      if not (match_object_url.startswith('/') or match_object_url.startswith('http') or match_object_url.startswith('#')): continue 

      internal = True if (match_object_url.startswith('/') or match_object_url.startswith('#')) else False
      language = 'DE' if ((internal and match_object_url.startswith('/de')) or (not internal and match_object_url.endswith('de'))) else 'EN'

      url_dict = {
        "URL" : match_object_url,
        "Internal" : internal,
        "Sprache": language
      }
      url_dict = json.dumps(url_dict) # dict is not hashable and set can only contain hashable entries, so convert dict to string
      match_objects.append(url_dict)

  
  return set(match_objects) # no duplicates


def main():

  url = 'https://datatroniq.com'

  all_links = list(get_links_from_url(url))

  for link in all_links:
    link = json.loads(link) # to have access to object's properties
    print("current url: {}".format(url + link["URL"]))
    if link["Internal"]:
      time.sleep(0.1)
      child_links = get_links_from_url(url+link["URL"])
      for child_link in child_links:
        if not child_link in all_links:
          # first append, then handle for potential duplicates
          all_links.append(child_link)
          all_links = list(set(all_links))
  
  print()
  print("Total number of links found: {}".format(len(all_links)))


  conn = psycopg2.connect(host="localhost", database="postgres", user="postgres", port=5432) # establish database connection
  cur = conn.cursor()
  cur.execute("create type lang as enum('DE', 'EN')")
  cur.execute(
  """
  CREATE TABLE URLS (
    URL text,
    SPRACHE lang,
    Internal boolean
  )
  """
  )
  for link in all_links:
    link = json.loads(link) # to have access to object's properties
    cur.execute("INSERT INTO URLS VALUES (%s, %s, %s)", (link["URL"], link["Sprache"], link["Internal"]))

  conn.commit()
  print("Commited to database.")

if __name__ == "__main__":
  main()