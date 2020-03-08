from multiprocessing import Pool
import requests
from bs4 import BeautifulSoup
import csv

def generate_urls():
    all_urls = []
    playbill_url = "https://www.playbill.com"
    page = requests.get('https://www.playbill.com/grosses')
    soup = BeautifulSoup(page.text, 'html.parser')
    weeks = soup.find_all("option")
    week_writer = open('all_weeks.text', mode='w')
    for week in weeks:
        week_query_string = week.get("value")
        week_writer.write("{}\n".format(week_query_string[-10:]))
        all_urls.append(playbill_url + week_query_string)
    return all_urls

def scrape(url):
    try:
        page = requests.get(url)
        soup = BeautifulSoup(page.text, 'html.parser')
        tbody = soup.find_all("tbody")
        shows = tbody[0].find_all("tr")
        for show in shows:
            col = show.find_all("td")
            week = url[-10:]
            show_title = col[0].find("span", {"class": "data-value"}).text
            theatre_name = col[0].find("span", {"class": "subtext"}).text
            week_gross = col[1].find("span", {"class": "data-value"}).text
            potential_gross = col[1].find("span", {"class": "subtext"}).text
            diff = col[2].find("span", {"class": "data-value"}).text
            avg_ticket = col[3].find("span", {"class": "data-value"}).text
            top_ticket = col[3].find("span", {"class": "subtext"}).text
            seats_sold = col[4].find("span", {"class": "data-value"}).text
            seats_in_theatre = col[4].find("span", {"class": "subtext"}).text
            performances  = col[5].find("span", {"class": "data-value"}).text
            previews = col[5].find("span", {"class": "subtext"}).text
            cap = col[6].find("span", {"class": "data-value"}).text
            diff_cap = col[7].find("span", {"class": "data-value"}).text
            writer.writerow([week, show_title, theatre_name, week_gross, potential_gross, 
                             diff, avg_ticket, top_ticket, seats_sold, seats_in_theatre,
                             performances, previews, cap, diff_cap])
    except:
        print("Something's wrong at: " + url)
        pass

if __name__== "__main__":
    urls = generate_urls()
    writer = csv.writer(open('broadway_grosses_weekly_with_wait.csv', mode='w'))
    pool = Pool(20)
    pool.map(scrape, urls)
    pool.terminate()
    pool.join()
