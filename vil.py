import requests
from bs4 import BeautifulSoup
url = 'https://villageinfo.in/punjab.html'
headers =  {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
result = requests.get(url, headers=headers)
#print(result.content.decode())
dists=BeautifulSoup(result.content.decode(),"lxml")
taluk=''
zilla=''
row=''
#print(soup.find('table',attrs={'class':'vict vict-big'}).find_all('a'))
for dist in dists.find('table',attrs={'class':'vict vict-big'}).find_all('a'):
    zilla=dist.text
    tehsi_page=requests.get('https://villageinfo.in/'+dist['href'],headers=headers)
    tehsil=BeautifulSoup(tehsi_page.content.decode(),"lxml")
    for tehsil in tehsil.find('table',attrs={'class':'vict vict-big'}).find_all('a'):
        taluk=tehsil.text
        village_page=requests.get('https://villageinfo.in'+tehsil['href'],headers=headers)
        village=BeautifulSoup(village_page.content.decode(),"lxml")
        
        for vil in village.find('table',attrs={'class':'vict vict-big'}).find_all('a'):
            #print(vil.text)
            halli=''
            halli=vil.text
            village_data=requests.get('https://villageinfo.in'+vil['href'],headers=headers)
            villages=BeautifulSoup(village_data.content.decode(),"lxml")
            #print(zilla+","+taluk+","+villages.find('table',attrs={'class':'vi'}).findAll('tr')[1].text+","+villages.find('table',attrs={'class':'vi'}).findAll('tr')[5].text)
            if villages.find('table',attrs={'class':'vi'}) is not None:
                row+=zilla+","+taluk+","+halli+","+villages.find('table',attrs={'class':'vi'}).findAll('tr')[1].findAll('td')[1].text+","+villages.find('table',attrs={'class':'vi'}).findAll('tr')[5].findAll('td')[1].text+"\n"
                with open("D:/DNA/punjab.csv","w",encoding="utf8") as file1:
                    file1.write(row)
