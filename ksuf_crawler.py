import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient, ASCENDING
from datetime import datetime

e_code_list = {
    'basketball': 6,
    'baseball': 34,
    'soccer': 45
}

basketball_year_lcode = {
    2024: (197, ),
    2023: (165, ),
    2022: (149, 158),
    2021: (116, 120, 122, 129),
    2020: (49, 96, 97),
    2019: (15, 35),
    2018: (65, 80)
}

basketball_year_tcode = {

    2024: 1233,
    2023: 1056,
    2022: 958,
    2021: 821,
    2020: 458,
    2019: 200,
    2018: 8
}

baseball_year_lcode = {
    2024: (206, ),
    2023: (183, 193),
    2022: (151, 161),
    2021: (111, 125),
    2020: (51, 92),
    2019: (10, 33),
    2018: (83, 84, 86)
}

baseball_year_tcode = {

    2024: 1308,
    2023: 1187,
    2022: 987,
    2021: 775,
    2020: 481,
    2019: 172,
    2018: 28
}

soccer_year_lcode = {
    2024: (212, ),
    2023: (167, ),
    2022: (139, ),
    2021: (101, ),
    2020: (58, 98),
    2019: (22, 33),
    2018: (71, 79)
}

soccer_year_tcode = {

    2024: 1369,
    2023: 1078,
    2022: 866,
    2021: 699,
    2020: 602,
    2019: 227,
    2018: 76
}

# 크롤링 함수
def crawl_event(e_code_key):
    # e_code 값 가져오기
    e_code = e_code_list.get(e_code_key)
    if e_code is None:
        print(f"Invalid e_code_key: {e_code_key}")
        return
    elif e_code == 6:
        year_to_tcode = basketball_year_tcode
    elif e_code == 34:
        year_to_tcode = baseball_year_tcode
    elif e_code == 45:
        year_to_tcode = soccer_year_tcode    
    
    # URL 템플릿
    url_template = "https://www.kusf.or.kr/league/league_schedule.html?e_code={e_code}&ptype=&l_year={l_year}&l_mon=&l_code=&t_code={t_code}"
    
    # l_year 반복
    for l_year, t_code in year_to_tcode.items():
        # URL 생성
        url = url_template.format(e_code=e_code, l_year=l_year, t_code=t_code)
        print(f"Fetching URL: {url}")
        
        try:
            # 페이지 요청
            response = requests.get(url)
            response.raise_for_status()  # 응답 상태 코드가 200이 아니면 HTTPError 발생
            response.encoding = 'utf-8'
            # HTML 소스 파싱
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 모든 경기 일정을 포함하는 li 태그들을 찾기
            li_tags = soup.select('div.sls1 ul li')
            
            # 모든 event_data를 저장할 배열
            event_data = []
            for li in li_tags:
                col1_span = li.find('span', class_ = 'col1')
                if col1_span:
                    league = col1_span.text.strip()
                
                left_team = li.find('span', class_= 'left').find('em').text.strip()
                right_team = li.find('span', class_= 'right').find('em').text.strip()
                teams = [left_team, right_team] 

                # col2 클래스를 가진 div 태그에서 날짜와 시간을 포함하는 텍스트를 추출
                col2_div = li.find('div', class_='col2')
                if col2_div:
                    # 날짜와 시간 정보를 추출
                    date_text = col2_div.contents[0].strip()  # 첫 번째 텍스트 노드
                    time_location_text = col2_div.find('span').text.strip()  # span 태그 안의 텍스트
                    # start_time과 location 분리
                    start_time = f"{date_text} {time_location_text.split()[0]}"
                    location = ' '.join(time_location_text.split()[1:])    
                # socre 정보 추출
                score_text = li.find('strong').find('span').find('em').text.strip()
                if score_text == 'VS':
                    score = ['','']
                elif score_text == "우천취소":
                    score = "우천취소"
                else:
                    score = [s for s in score_text.split(':')]
                # m_code 정보 추출, 경기기록 주소를 구분, 중복 제거를 위한 정보
                m_code = ''
                col4_span = li.find('span', class_='col4')
                if col4_span:
                    a_tag = col4_span.find('a')
                    if a_tag and 'href' in a_tag.attrs:
                        m_code = a_tag['href'].split('=')[-1]
                        result_url = f"https://www.kusf.or.kr/league/{a_tag['href']}"
                
                        try:
                            if(e_code == 6): #basketball 기록일 때
                                team_record, player_record = crawl_basketball_result(result_url)
                            
                            elif(e_code == 34):
                                team_record, player_record = crawl_baseball_result(result_url)

                            elif(e_code == 45):
                                team_record, player_record = crawl_soccer_result(result_url)
                        
                        except requests.exceptions.HTTPError as http_err:
                            print(f"HTTP error occurred: {http_err} - result_url={result_url}")
                        except Exception as err:
                            print(f"Other error occurred: {err} - result_url={result_url}")
            
                
                event_data.append({'start_time': start_time, 'm_code': m_code, 'sports_type': e_code, 'location': location, 'teams': teams,
                                   'league': league, 'score': score, 'team_record': team_record, 'player_record': player_record})
            
            print(f"Successfully fetched data for e_code={e_code}, l_year={l_year}, t_code={t_code}")
            for data in event_data:
                print(f"date: {data['start_time']}, location: {data['location']}, teams: {data['teams']}, league: {data['league']}, score: {data['score']}, m_code: {data['m_code']}")
                print(f"team_record: {data['team_record']}")
                print(f"player_record: {data['player_record']}")

        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err} - e_code={e_code},l_year={l_year}, t_code={t_code}")
        except Exception as err:
            print(f"Other error occurred: {err} - e_code={e_code}, l_year={l_year}, t_code={t_code}, m_code={m_code}")


def crawl_basketball_result(url):
    response = requests.get(url)
    response.raise_for_status()
    response.encoding = 'utf-8'

    soup = BeautifulSoup(response.text, 'html.parser')

    # 각 tr 태그 찾기
    tr_tags = soup.select('div.rankCase_Scroll_sub.rec_team tbody tr')

    # 각 tr 태그 내의 td 태그의 텍스트를 배열로 변환
    team_record = []
    team_keys = ['q1', 'q2', 'q3', 'q4', 'rb', 'as', 'steal', 'bl', 'foul', 'two', 'tree', 'free']
    for tr in tr_tags:
        td_values = [td.text.strip() for td in tr.find_all('td')]
        td_dict = dict(zip(team_keys, td_values))
        team_record.append(td_dict)

    #선수 고유 p_code 추출
    home_pcodes = []
    href_tags = soup.select('div.tab-con.tabData2 tbody a')
    for tag in href_tags:
        href = tag['href']
        p_code = href.split('p_code=')[1].split('&')[0]
        home_pcodes.append(p_code)
    
    away_pcodes = []
    href_tags = soup.select('div.tab-con.tabData3 tbody a')
    for tag in href_tags:
        href = tag['href']
        p_code = href.split('p_code=')[1].split('&')[0]
        away_pcodes.append(p_code)

    
    player_keys = ['score', 'time', 'two_per', 'three_per', 'freethrow_per', 'rb', 'as', 'steal', 'bl', 'foul']
    home_values = []
    away_values = []
    
    home_tags = soup.select('div.rankCase_Scroll_sub.rec_player1 tbody tr')
    away_tags = soup.select('div.rankCase_Scroll_sub.rec_player2 tbody tr')

    for tr in home_tags:
        player_values = [td.text.strip() for td in tr.find_all('td')]
        home_dict = dict(zip(player_keys, player_values))
        home_values.append(home_dict)
    
    home_player = dict(zip(home_pcodes, home_values))

    for tr in away_tags:
        player_values = [td.text.strip() for td in tr.find_all('td')]
        away_dict = dict(zip(player_keys, player_values))
        away_values.append(away_dict)
    
    away_player = dict(zip(away_pcodes, away_values))

    player_record = [home_player, away_player]

    return team_record, player_record

def crawl_baseball_result(url):
    response = requests.get(url)
    response.raise_for_status()
    response.encoding = 'utf-8'

    soup = BeautifulSoup(response.text, 'html.parser')

    # 야구 경기기록 tr 태그 찾기
    game_tags = soup.select('div.rankCase_Scroll_sub.rec_boxscore tbody tr')
    game_record = []
    game_keys = ['i1', 'i2', 'i3', 'i4', 'i5', 'i6', 'i7', 'i8', 'i9', 'runs', 'hits', 'errors' , 'base_on_balls']
    
    for tr in game_tags:
        td_values = [td.text.strip() for td in tr.find_all('td')]
        td_dict = dict(zip(game_keys, td_values))
        game_record.append(td_dict)

    # 야구 팀기록 tr 태그 찾기
    team_tags = soup.select('div.rankCase_Scroll_sub.rec_team tbody tr')
    team_record = []
    team_keys = ['score', 'hit', 'hit2', 'hit3', 'hr', 'steal', 'k', 'error', 'obp', 'slg']

    for tr in team_tags:
        td_values = [td.text.strip() for td in tr.find_all('td')]
        td_dict = dict(zip(team_keys, td_values))
        team_record.append(td_dict)
    
    # 경기기록과 팀기록을 합친다.
    for i in range(len(team_record)):
        team_record[i]['score_detail'] = game_record[i]

    # 홈팀 선수 p_code 추출
    home_tbody = soup.select('div.tab-con.tabData2 tbody')
    home_pcodes = {}
    hitter_pcodes = []
    pitcher_pcodes = []

    hitter_href_tags = home_tbody[0].find_all("a")
    for tag in hitter_href_tags:
        href = tag['href']
        p_code = href.split('p_code=')[1].split('&')[0]
        hitter_pcodes.append(p_code)
    home_pcodes['hitter'] = hitter_pcodes

    pitcher_href_tags = home_tbody[2].find_all("a")
    for tag in pitcher_href_tags:
        href = tag['href']
        p_code = href.split('p_code=')[1].split('&')[0]
        pitcher_pcodes.append(p_code)
    home_pcodes['pitcher'] = pitcher_pcodes
    
    # 원정팀 선수 p_code 추출
    away_tbody = soup.select('div.tab-con.tabData3 tbody')
    away_pcodes = {}
    hitter_pcodes = []
    pitcher_pcodes = []

    hitter_href_tags = away_tbody[0].find_all("a")
    for tag in hitter_href_tags:
        href = tag['href']
        p_code = href.split('p_code=')[1].split('&')[0]
        hitter_pcodes.append(p_code)
    away_pcodes['hitter'] = hitter_pcodes

    pitcher_href_tags = away_tbody[2].find_all("a")
    for tag in pitcher_href_tags:
        href = tag['href']
        p_code = href.split('p_code=')[1].split('&')[0]
        pitcher_pcodes.append(p_code)
    away_pcodes['pitcher'] = pitcher_pcodes
    
    # 타자와 투수의 기록 키 설정
    hitter_keys = ['타석', '타수', '안타', '타점', '득점', '2타', '3타', '홈런', '도루', '희타', '희비', '4구', '사구', '삼진']
    pitcher_keys = ['선발', '승패', '이닝', '타자', '투구수', '타수', '안타', '홈런', '희타', '희비', '4구', '사구', '삼진', '폭투', '보크', '실점', '자책점']
    
    home_hitter_tags = soup.select('div.rankCase_Scroll_sub.rec_player1 tbody tr')
    home_pitcher_tags = soup.select('div.rankCase_Scroll_sub.rec_player2 tbody tr')
    away_hitter_tags = soup.select('div.rankCase_Scroll_sub.rec_player3 tbody tr')
    away_pitcher_tags = soup.select('div.rankCase_Scroll_sub.rec_player4 tbody tr')

    home_pitcher_values = []
    home_hitter_values = []

    for tr in home_hitter_tags:
        hitter_values = [td.text.strip() for td in tr.find_all('td')]
        home_dict = dict(zip(hitter_keys, hitter_values))
        home_hitter_values.append(home_dict)
    
    for tr in home_pitcher_tags:
        pitcher_values = [td.text.strip() for td in tr.find_all('td')]
        home_dict = dict(zip(pitcher_keys, pitcher_values))
        home_pitcher_values.append(home_dict)

    position = ['hitter', 'pitcher']
    home_hitter = dict(zip(home_pcodes['hitter'], home_hitter_values))
    home_pitcher = dict(zip(home_pcodes['pitcher'], home_pitcher_values))
    home_record = dict(zip(position, [home_hitter, home_pitcher]))

    away_pitcher_values = []
    away_hitter_values = []

    for tr in away_hitter_tags:
        hitter_values = [td.text.strip() for td in tr.find_all('td')]
        away_dict = dict(zip(hitter_keys, hitter_values))
        away_hitter_values.append(away_dict)
    
    for tr in away_pitcher_tags:
        pitcher_values = [td.text.strip() for td in tr.find_all('td')]
        away_dict = dict(zip(pitcher_keys, pitcher_values))
        away_pitcher_values.append(away_dict)

    away_hitter = dict(zip(away_pcodes['hitter'], away_hitter_values))
    away_pitcher = dict(zip(away_pcodes['pitcher'], away_pitcher_values))
    away_record = dict(zip(position, [away_hitter, away_pitcher]))
    player_record = [home_record, away_record]

    return team_record, player_record

def crawl_soccer_result(url):
    response = requests.get(url)
    response.raise_for_status()
    response.encoding = 'utf-8'

    soup = BeautifulSoup(response.text, 'html.parser')

    # 각 tr 태그 찾기
    tr_tags = soup.select('div.rankCase_Scroll_sub.rec_team tbody tr')

    # 각 tr 태그 내의 td 태그의 텍스트를 배열로 변환
    team_record = []
    team_keys = ['first', 'second', 'as', 'yellow', 'red']
    for tr in tr_tags:
        td_values = [td.text.strip() for td in tr.find_all('td')]
        td_dict = dict(zip(team_keys, td_values))
        team_record.append(td_dict)

    #선수 고유 p_code 추출
    home_pcodes = []
    href_tags = soup.select('div.tab-con.tabData2 tbody a')
    for tag in href_tags:
        href = tag['href']
        p_code = href.split('p_code=')[1].split('&')[0]
        home_pcodes.append(p_code)
    
    away_pcodes = []
    href_tags = soup.select('div.tab-con.tabData3 tbody a')
    for tag in href_tags:
        href = tag['href']
        p_code = href.split('p_code=')[1].split('&')[0]
        away_pcodes.append(p_code)

    
    player_keys = ['score', 'as', 'yellow', 'red']
    home_values = []
    away_values = []
    
    home_tags = soup.select('div.rankCase_Scroll_sub.rec_player1 tbody tr')
    away_tags = soup.select('div.rankCase_Scroll_sub.rec_player2 tbody tr')

    for tr in home_tags:
        player_values = [td.text.strip() for td in tr.find_all('td')]
        home_dict = dict(zip(player_keys, player_values))
        home_values.append(home_dict)
    
    home_player = dict(zip(home_pcodes, home_values))

    for tr in away_tags:
        player_values = [td.text.strip() for td in tr.find_all('td')]
        away_dict = dict(zip(player_keys, player_values))
        away_values.append(away_dict)
    
    away_player = dict(zip(away_pcodes, away_values))

    player_record = [home_player, away_player]

    return team_record, player_record

def crawl_rank(e_code_key):
    e_code = e_code_list.get(e_code_key)
    if e_code is None:
        print(f"Invalid e_code_key: {e_code_key}")
        return
    elif e_code == 6:
        year_to_lcode = basketball_year_lcode
        record_keys = [
                    'rank', 'nog', 'win_rate', 'wins', 'lose',
                    'score', 'as', 'rb', 'st', 'bl', 
                    'two', 'three', 'ft'
                    ]
    elif e_code == 34:
        year_to_lcode = baseball_year_lcode
        record_keys = [
                    'ranking', 'nog', 'win_point', 'wins', 'draw',
                    'lose', 'win_rate', 'oba', 'sa'
                    ]
    elif e_code == 45:
        year_to_lcode = soccer_year_lcode 
        record_keys = [
                    'ranking', 'nog', 'win_point', 'wins', 'draw', 
                    'lose', 'score', 'loss', 'margin', 'as', 'yellow', 'red'
                    ]
    # URL 템플릿
    url_template = "https://www.kusf.or.kr/league/league_ranking.html?e_code={e_code}&ptype=&l_year={l_year}&l_mon=&l_code={l_code}"

    for l_year, lcode_list in year_to_lcode.items():
        for l_code in lcode_list:
            url = url_template.format(e_code=e_code, l_year=l_year, l_code=l_code)
            print(f"Fetching URL: {url}")
            try:
                # 페이지 요청
                response = requests.get(url)
                response.raise_for_status()  # 응답 상태 코드가 200이 아니면 HTTPError 발생
                response.encoding = 'utf-8'
                # HTML 소스 파싱
                soup = BeautifulSoup(response.text, 'html.parser')
                rank_data = {}
                rank_data['league_name'] =  soup.select_one('div.kind2 option[selected]').text

                team_names = [th.text.strip() for th in soup.select('tbody#LeagueStatsTable_tit th.tleft')]
                team_records = []
                for tr in soup.select('tbody#LeagueStatsTable_table tr'):
                    record_values = [td.text.strip() for td in tr.find_all('td')]
                    record_dict = dict(zip(record_keys, record_values))
                    team_records.append(record_dict)
                # 학교 이름을 key로, 각 기록들을 담은 딕셔너리를 value로 하는 딕셔너리 생성
                team_data = dict(zip(team_names, team_records))
                rank_data['league_record'] = team_data
                print(f"Successfully fetched data for e_code={e_code}, l_year={l_year}, l_code={l_code}")
                print(f"league: {rank_data['league_name']}, league_record: {rank_data['league_record']}")

            except requests.exceptions.HTTPError as http_err:
                print(f"HTTP error occurred: {http_err} - e_code={e_code},l_year={l_year}, l_code={l_code}")
            
            except Exception as err:
                print(f"Other error occurred: {err} - e_code={e_code}, l_year={l_year}, l_code={l_code}")


def crawl_basketball_player():
    e_code = 6
    # URL 템플릿
    url_template = "https://www.kusf.or.kr/league/league_ranking_player.html?e_code={e_code}&ptype=&l_year={l_year}&l_code={l_code}&t_code={t_code}&srch_word="
    player_data = []
    id_list = []
    for l_year, t_code in basketball_year_tcode.items():
        for l_code in basketball_year_lcode[l_year]:
            url = url_template.format(e_code=e_code, l_year=l_year, l_code=l_code, t_code=t_code)                          
            print(f"Fetching URL: {url}")
            
            try:    
                # 페이지 요청
                response = requests.get(url)
                response.raise_for_status()  # 응답 상태 코드가 200이 아니면 HTTPError 발생
                response.encoding = 'utf-8'
                # HTML 소스 파싱
                soup = BeautifulSoup(response.text, 'html.parser')
                a_tags = soup.select('tbody#LeagueStatsTable_tit a')
                
                # p_code 추출
                for a in a_tags:
                    href = a.get('href')    
                    player_url = f"https://www.kusf.or.kr{href}"
                    try:
                        player_record = {}
                        response = requests.get(player_url)
                        response.raise_for_status()  # 응답 상태 코드가 200이 아니면 HTTPError 발생
                        response.encoding = 'utf-8'
                        # HTML 소스 파싱
                        soup = BeautifulSoup(response.text, 'html.parser')  
                        player_record['name'] = soup.select_one('div.playerBasic.mb60 td.person p').text.strip()
                        date_str = soup.select_one('div.playerBasic.mb60 div.right tr:nth-of-type(1) td:nth-of-type(1)').text.strip()
                        date_obj = datetime.strptime(date_str, '%Y년 %m월 %d일').date()
                        player_record['birthday'] = date_obj.strftime("%Y-%m-%d")
                        player_record['no'] =  int(soup.select_one('div.playerBasic.mb60 td.person strong').text.strip().replace('#', '').strip())
                        player_record['position'] = soup.select_one('div.playerBasic.mb60 div.right tr:nth-of-type(3) td:nth-of-type(1)').text.strip()
                        player_record['id'] =  f"{player_record['name']} {date_str}"
                        height = soup.select_one('div.playerBasic.mb60 div.right tr:nth-of-type(2) td:nth-of-type(1)').text.strip()
                        weight = soup.select_one('div.playerBasic.mb60 div.right tr:nth-of-type(2) td:nth-of-type(2)').text.strip()
                        player_record['physical_info'] = {'height': height, 'weight': weight}

                        years_divs = soup.select('div.playerCase_Scroll')
                        years_div = years_divs[2]
                        record_key = ['nog', 'score', 'min', 'two', 'three', 'ft', 'rb', 'as', 'st', 'bl', 'foul']
                        l_years = [td.text.strip() for td in years_div.select('.tbls1.rankCase.playerCase_Scroll_tit tbody td')]
                        # 리그 기록 추출
                        yearly_record = []
                        for tr in soup.select('div.playerCase_Scroll_sub.playerScroll3 tbody tr'):
                            record_dict = dict(zip(record_key, [td.text.strip() for td in tr.find_all('td')]))
                            yearly_record.append(record_dict)
                        league_record = dict(zip(l_years, yearly_record))
                        player_record['league_record'] = league_record

                        years_div = years_divs[3]
                        t_years = [td.text.strip() for td in years_div.select('.tbls1.rankCase.playerCase_Scroll_tit tbody td')]
                        # 토너먼트 기록 추출
                        yearly_record = []
                        for tr in soup.select('.playerCase_Scroll_sub.playerScroll4 tbody tr'):
                            record_dict = dict(zip(record_key, [td.text.strip() for td in tr.find_all('td')]))
                            yearly_record.append(record_dict)
                        tournament_record = dict(zip(t_years, yearly_record))
                        player_record['tournament_record'] = tournament_record
                        # 중복제거
                        if player_record['id'] not in id_list:
                            id_list.append(player_record['id'])
                            player_data.append(player_record)
                        
                    except requests.exceptions.HTTPError as http_err:
                        print(f"HTTP error occurred: {http_err} - player_url={player_url}")
                    except Exception as err:
                        print(f"Other error occurred: {err} - player_url={player_url}") 
                print(f"Successfully fetched data for e_code={e_code}, l_year={l_year}, l_code={l_code}, t_code={t_code}")

            except requests.exceptions.HTTPError as http_err:
                print(f"HTTP error occurred: {http_err} - e_code={e_code},l_year={l_year},l_code={l_code}, t_code={t_code}")
        
            except Exception as err:
                print(f"Other error occurred: {err} - e_code={e_code}, l_year={l_year}, l_code={l_code}, t_code={t_code}")
            
    for data in player_data:
        print(data)
    
    return player_data


def crawl_baseball_player():
    e_code = 34
    # URL 템플릿
    url_template = "https://www.kusf.or.kr/league/league_ranking_player.html?e_code={e_code}&ptype={ptype}&l_year={l_year}&l_code={l_code}&t_code={t_code}&srch_word="
    player_data = []
    id_list = []
    hitter_key = ['nog', 'ba', 'appearance', 'hit', 'hit2', 'hit3', 'hr', 'score,' 'steal', 'walks', 'k', 'obp', 'slg', 'ops']
    pitcher_key = ['nog', 'era', 'inning', 'wins', 'lose', 'k', 'hits', 'hr', 'er,' 'walks', 'dead', 'whip']
    for l_year, t_code in baseball_year_tcode.items():
        for l_code in baseball_year_lcode[l_year]:
            # tuta 확인을 위한 리스트
            hitter_list = []
            hitter_url = url_template.format(e_code=e_code, ptype = "", l_year=l_year, l_code=l_code, t_code=t_code)
            pitcher_url = url_template.format(e_code=e_code, ptype = "p", l_year=l_year, l_code=l_code, t_code=t_code)                          
            print(f"Fetching URL: {hitter_url}")
            
            try:    
                # 페이지 요청
                response = requests.get(hitter_url)
                response.raise_for_status()  # 응답 상태 코드가 200이 아니면 HTTPError 발생
                response.encoding = 'utf-8'
                # HTML 소스 파싱
                soup = BeautifulSoup(response.text, 'html.parser')
                a_tags = soup.select('tbody#LeagueStatsTable_tit a')
                
                # p_code 추출
                for a in a_tags:
                    href = a.get('href')    
                    player_url = f"https://www.kusf.or.kr{href}"
                    try:
                        player_record = {}
                        response = requests.get(player_url)
                        response.raise_for_status()  # 응답 상태 코드가 200이 아니면 HTTPError 발생
                        response.encoding = 'utf-8'
                        # HTML 소스 파싱
                        soup = BeautifulSoup(response.text, 'html.parser')  
                        player_record['name'] = soup.select_one('div.playerBasic.mb60 td.person p').text.strip()
                        date_str = soup.select_one('div.playerBasic.mb60 div.right tr:nth-of-type(1) td:nth-of-type(1)').text.strip()
                        date_obj = datetime.strptime(date_str, '%Y년 %m월 %d일').date()
                        player_record['birthday'] = date_obj.strftime("%Y-%m-%d")
                        player_record['no'] =  int(soup.select_one('div.playerBasic.mb60 td.person strong').text.strip().replace('#', '').strip())
                        player_record['position'] = soup.select_one('div.playerBasic.mb60 div.right tr:nth-of-type(3) td:nth-of-type(1)').text.strip()
                        player_record['id'] =  f"{player_record['name']} {date_str}"
                        height = soup.select_one('div.playerBasic.mb60 div.right tr:nth-of-type(2) td:nth-of-type(1)').text.strip()
                        weight = soup.select_one('div.playerBasic.mb60 div.right tr:nth-of-type(2) td:nth-of-type(2)').text.strip()
                        player_record['physical_info'] = {'height': height, 'weight': weight}
                        player_record['tuta'] = 0

                        years_divs = soup.select('div.playerCase_Scroll')
                        years_div = years_divs[2]
                        l_years = [td.text.strip() for td in years_div.select('.tbls1.rankCase.playerCase_Scroll_tit tbody td')]
                        # 리그 기록 추출
                        yearly_record = []
                        for tr in soup.select('div.playerCase_Scroll_sub.playerScroll3 tbody tr'):
                            record_dict = dict(zip(hitter_key, [td.text.strip() for td in tr.find_all('td')]))
                            yearly_record.append(record_dict)
                        league_record = dict(zip(l_years, yearly_record))
                        player_record['league__hitter_record'] = league_record

                        years_div = years_divs[3]
                        t_years = [td.text.strip() for td in years_div.select('.tbls1.rankCase.playerCase_Scroll_tit tbody td')]
                        # 토너먼트 기록 추출
                        yearly_record = []
                        for tr in soup.select('.playerCase_Scroll_sub.playerScroll4 tbody tr'):
                            record_dict = dict(zip(hitter_key, [td.text.strip() for td in tr.find_all('td')]))
                            yearly_record.append(record_dict)
                        tournament_record = dict(zip(t_years, yearly_record))
                        player_record['tournament_hitter_record'] = tournament_record

                        # 중복제거
                        if player_record['id'] not in id_list:
                            id_list.append(player_record['id'])
                            player_data.append(player_record)
                            hitter_list.append(player_record['id'])
                                    
                    except requests.exceptions.HTTPError as http_err:
                        print(f"HTTP error occurred: {http_err} - player_url={player_url}")
                    except Exception as err:
                        print(f"Other error occurred: {err} - player_url={player_url}") 
                print(f"Successfully fetched data for e_code={e_code}, l_year={l_year}, l_code={l_code}, t_code={t_code}")

            except requests.exceptions.HTTPError as http_err:
                print(f"HTTP error occurred: {http_err} - e_code={e_code},l_year={l_year},l_code={l_code}, t_code={t_code}")
        
            except Exception as err:
                print(f"Other error occurred: {err} - e_code={e_code}, l_year={l_year}, l_code={l_code}, t_code={t_code}")

            print(f"Fetching URL: {pitcher_url}")
            try:    
                # 페이지 요청
                response = requests.get(pitcher_url)
                response.raise_for_status()  # 응답 상태 코드가 200이 아니면 HTTPError 발생
                response.encoding = 'utf-8'
                # HTML 소스 파싱
                soup = BeautifulSoup(response.text, 'html.parser')
                a_tags = soup.select('tbody#LeagueStatsTable_tit a')
                
                # p_code 추출
                for a in a_tags:
                    href = a.get('href')    
                    player_url = f"https://www.kusf.or.kr{href}"
                    try:
                        player_record = {}
                        response = requests.get(player_url)
                        response.raise_for_status()  # 응답 상태 코드가 200이 아니면 HTTPError 발생
                        response.encoding = 'utf-8'
                        # HTML 소스 파싱
                        soup = BeautifulSoup(response.text, 'html.parser')  
                        player_record['name'] = soup.select_one('div.playerBasic.mb60 td.person p').text.strip()
                        date_str = soup.select_one('div.playerBasic.mb60 div.right tr:nth-of-type(1) td:nth-of-type(1)').text.strip()
                        date_obj = datetime.strptime(date_str, '%Y년 %m월 %d일').date()
                        player_record['birthday'] = date_obj.strftime("%Y-%m-%d")
                        player_record['no'] =  int(soup.select_one('div.playerBasic.mb60 td.person strong').text.strip().replace('#', '').strip())
                        player_record['position'] = soup.select_one('div.playerBasic.mb60 div.right tr:nth-of-type(3) td:nth-of-type(1)').text.strip()
                        player_record['id'] =  f"{player_record['name']} {date_str}"
                        height = soup.select_one('div.playerBasic.mb60 div.right tr:nth-of-type(2) td:nth-of-type(1)').text.strip()
                        weight = soup.select_one('div.playerBasic.mb60 div.right tr:nth-of-type(2) td:nth-of-type(2)').text.strip()
                        player_record['physical_info'] = {'height': height, 'weight': weight}
                        player_record['tuta'] = 0
                        
                        years_divs = soup.select('div.playerCase_Scroll')
                        years_div = years_divs[6]
                        l_years = [td.text.strip() for td in years_div.select('.tbls1.rankCase.playerCase_Scroll_tit tbody td')]
                        # 리그 기록 추출
                        yearly_record = []
                        for tr in soup.select('div.playerCase_Scroll_sub.playerScroll23 tbody tr'):
                            record_dict = dict(zip(pitcher_key, [td.text.strip() for td in tr.find_all('td')]))
                            yearly_record.append(record_dict)
                        league_record = dict(zip(l_years, yearly_record))
                        player_record['league_pitcher_record'] = league_record

                        years_div = years_divs[7]
                        t_years = [td.text.strip() for td in years_div.select('.tbls1.rankCase.playerCase_Scroll_tit tbody td')]
                        # 토너먼트 기록 추출
                        yearly_record = []
                        for tr in soup.select('.playerCase_Scroll_sub.playerScroll24 tbody tr'):
                            record_dict = dict(zip(pitcher_key, [td.text.strip() for td in tr.find_all('td')]))
                            yearly_record.append(record_dict)
                        tournament_record = dict(zip(t_years, yearly_record))
                        player_record['tournament_pitcher_record'] = tournament_record

                        # 중복제거
                        if player_record['id'] not in id_list:
                            id_list.append(player_record['id'])
                            player_data.append(player_record)
                        
                        if player_record['id'] in hitter_list:
                            for data in player_data:
                                if data['id'] == player_record['id']:
                                    data['tuta'] = 1
                                    data['league_pitcher_record'] = league_record
                                    data['tournament_pitcher_record'] = tournament_record
                                    
                    except requests.exceptions.HTTPError as http_err:
                        print(f"HTTP error occurred: {http_err} - player_url={player_url}")
                    except Exception as err:
                        print(f"Other error occurred: {err} - player_url={player_url}") 
                print(f"Successfully fetched data for e_code={e_code}, l_year={l_year}, l_code={l_code}, t_code={t_code}")

            except requests.exceptions.HTTPError as http_err:
                print(f"HTTP error occurred: {http_err} - e_code={e_code},l_year={l_year},l_code={l_code}, t_code={t_code}")
        
            except Exception as err:
                print(f"Other error occurred: {err} - e_code={e_code}, l_year={l_year}, l_code={l_code}, t_code={t_code}")
            
    for data in player_data:
        print(data)
    
    return player_data


def crawl_soccer_player():
    e_code = 45
    # URL 템플릿
    url_template = "https://www.kusf.or.kr/league/league_ranking_player.html?e_code={e_code}&ptype=&l_year={l_year}&l_code={l_code}&t_code={t_code}&srch_word="
    player_data = []
    id_list = []
    for l_year, t_code in soccer_year_tcode.items():
        for l_code in soccer_year_lcode[l_year]:
            url = url_template.format(e_code=e_code, l_year=l_year, l_code=l_code, t_code=t_code)                          
            print(f"Fetching URL: {url}")
            
            try:    
                # 페이지 요청
                response = requests.get(url)
                response.raise_for_status()  # 응답 상태 코드가 200이 아니면 HTTPError 발생
                response.encoding = 'utf-8'
                # HTML 소스 파싱
                soup = BeautifulSoup(response.text, 'html.parser')
                a_tags = soup.select('tbody#LeagueStatsTable_tit a')
                
                # p_code 추출
                for a in a_tags:
                    href = a.get('href')    
                    player_url = f"https://www.kusf.or.kr{href}"
                    try:
                        player_record = {}
                        response = requests.get(player_url)
                        response.raise_for_status()  # 응답 상태 코드가 200이 아니면 HTTPError 발생
                        response.encoding = 'utf-8'
                        # HTML 소스 파싱
                        soup = BeautifulSoup(response.text, 'html.parser')  
                        player_record['name'] = soup.select_one('div.playerBasic.mb60 td.person p').text.strip()
                        date_str = soup.select_one('div.playerBasic.mb60 div.right tr:nth-of-type(1) td:nth-of-type(1)').text.strip()
                        date_obj = datetime.strptime(date_str, '%Y년 %m월 %d일').date()
                        player_record['birthday'] = date_obj.strftime("%Y-%m-%d")
                        player_record['no'] =  int(soup.select_one('div.playerBasic.mb60 td.person strong').text.strip().replace('#', '').strip())
                        player_record['position'] = soup.select_one('div.playerBasic.mb60 div.right tr:nth-of-type(3) td:nth-of-type(1)').text.strip()
                        player_record['id'] =  f"{player_record['name']} {date_str}"
                        height = soup.select_one('div.playerBasic.mb60 div.right tr:nth-of-type(2) td:nth-of-type(1)').text.strip()
                        weight = soup.select_one('div.playerBasic.mb60 div.right tr:nth-of-type(2) td:nth-of-type(2)').text.strip()
                        player_record['physical_info'] = {'height': height, 'weight': weight}

                        years_divs = soup.select('div.playerCase_Scroll')
                        years_div = years_divs[2]
                        record_key = ['nog', 'score', 'as', 'yellow', 'red']
                        l_years = [td.text.strip() for td in years_div.select('.tbls1.rankCase.playerCase_Scroll_tit tbody td')]
                        # 리그 기록 추출
                        yearly_record = []
                        for tr in soup.select('div.playerCase_Scroll_sub.playerScroll3 tbody tr'):
                            record_dict = dict(zip(record_key, [td.text.strip() for td in tr.find_all('td')]))
                            yearly_record.append(record_dict)
                        league_record = dict(zip(l_years, yearly_record))
                        player_record['league_record'] = league_record

                        years_div = years_divs[3]
                        t_years = [td.text.strip() for td in years_div.select('.tbls1.rankCase.playerCase_Scroll_tit tbody td')]
                        # 토너먼트 기록 추출
                        yearly_record = []
                        for tr in soup.select('.playerCase_Scroll_sub.playerScroll4 tbody tr'):
                            record_dict = dict(zip(record_key, [td.text.strip() for td in tr.find_all('td')]))
                            yearly_record.append(record_dict)
                        tournament_record = dict(zip(t_years, yearly_record))
                        player_record['tournament_record'] = tournament_record
                        # 중복제거
                        if player_record['id'] not in id_list:
                            id_list.append(player_record['id'])
                            player_data.append(player_record)
                        
                        
                    except requests.exceptions.HTTPError as http_err:
                        print(f"HTTP error occurred: {http_err} - player_url={player_url}")
                    except Exception as err:
                        print(f"Other error occurred: {err} - player_url={player_url}") 
                print(f"Successfully fetched data for e_code={e_code}, l_year={l_year}, l_code={l_code}, t_code={t_code}")

            except requests.exceptions.HTTPError as http_err:
                print(f"HTTP error occurred: {http_err} - e_code={e_code},l_year={l_year},l_code={l_code}, t_code={t_code}")
        
            except Exception as err:
                print(f"Other error occurred: {err} - e_code={e_code}, l_year={l_year}, l_code={l_code}, t_code={t_code}")
            
    for data in player_data:
        print(data)
    
    return player_data















# Connect to MongoDB
client = MongoClient('mongodb://localhost:56789/')

# Select the database
db = client['pildong_database']

# Select the collection
event_collection = db['Event']

# Create a unique index on the hash field to prevent duplicate events
event_collection.create_index([('m_code', ASCENDING)], unique=True)

# Function to insert event data
def insert_event(eid, start_time, sports_type, location, teams, league, score, record, m_code):
    # Create the event document
    event = {
        'eid': eid,
        'start_time': start_time,
        'sports_type': sports_type,
        'location': location,
        'teams': teams,
        'league': league,
        'score': score,
        'record': record,  # Ensure record is a list of dictionaries
        'm_code': m_code
    }

    # Insert the event document into the collection
    try:
        event_collection.insert_one(event)
        print(f"Event '{eid}' inserted with hash '{event['hash']}'")
    except Exception as e:
        print(f"Error inserting event '{eid}': {e}")