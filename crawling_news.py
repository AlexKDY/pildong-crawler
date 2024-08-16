from selenium import webdriver
from browsermobproxy import Server
from browsermobproxy import Server
import psutil
import time
from selenium import webdriver
import json
from bs4 import BeautifulSoup
from selenium.webdriver.chrome.options import Options
import re

def process_kill(name):
    """중복 실행중인 프로세스 종료"""

    for proc in psutil.process_iter():
        if proc.name() == name:
            proc.kill()

def capture_network_traffic(url):
    """프록시 서버 시작 및 네트워크 트래픽 캡쳐"""

    server = Server(path="/home/alexkim/Downloads/browsermob-proxy-2.1.4/bin/browsermob-proxy")
    server.start()
    time.sleep(1)
    proxy = server.create_proxy()
    time.sleep(1)

    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--proxy-server={0}".format(proxy.proxy))
    chrome_options.add_argument('--ignore-certificate-errors')

    driver = webdriver.Chrome(options=chrome_options)

    proxy.new_har("naver_page", options={'captureContent': True}) # options={'captureContent': True} https://github.com/lightbody/browsermob-proxy

    driver.get(url)
    time.sleep(3)
    for i in range(10):
        try:
            more_button = driver.find_element("css selector", '.btn_lst_more')
            driver.execute_script("arguments[0].scrollIntoView(true);", more_button)
            time.sleep(1)  # 스크롤 후 약간의 대기 시간
            more_button.click()
            time.sleep(3)  # 데이터를 로드할 시간을 줍니다.
        except Exception as e:
            print(f"더 이상 로드할 데이터가 없습니다. (에러: {e})")
            break
    driver.quit()
    server.stop()
    # 트래픽 분석
    har_data = proxy.har

    # 브라우저 종료 및 proxy 서버 종료
    driver.quit()
    server.stop()

    # XHR 요청 중 JSON 응답을 포함하는 요청 찾기
    xhr_responses = []
    for entry in har_data['log']['entries']:
        # 만약 JSON 형식의 XHR 요청이라면 응답 데이터를 수집합니다.
        if 'json' in entry['response']['content'].get('mimeType', ''):
            xhr_responses.append(entry['response']['content']['text'])
    return xhr_responses

def crawl_news_data(xhr_responses):
    """응답 데이터 가공"""
    """XHR 요청의 JSON 응답 데이터 출력"""
    description_list = []
    for i, response in enumerate(xhr_responses):
        news_links = re.findall(r'a href=\\"(\/viewer\/postView\.naver\?volumeNo=[^"]+)\\"', response)
        for link in news_links:
            url = "https://m.post.naver.com" + link
            description = crawl_news(url)
            description.append(description)

    return description_list

def crawl_news(url):
    
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument('--ignore-certificate-errors')
    # Chrome WebDriver 설정 (경로를 자신의 환경에 맞게 변경)
    driver = webdriver.Chrome(options=chrome_options)

    # 웹 페이지 로드
    driver.get(url)

    # 페이지가 로드되고 동적 콘텐츠가 로드될 시간을 기다림
    time.sleep(3)

    description = []
    try:
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        elements = soup.select('div.se_textView span')
        # 각 span 태그의 텍스트 추출
        for element in elements:
            description.append(element.get)
            
            
    except Exception as e:
        print(f"본문을 추출할 수 없습니다: {e}")

    # 브라우저 종료
    driver.quit()
    
    return description



if __name__ == '__main__':
    url = 'https://m.post.naver.com/my/series/detail.naver?memberNo=51653576&seriesNo=623989'
    

    # 중복된 프로세스 제거
    process_kill("browsermob-proxy")


    xhr_responses = capture_network_traffic(url)
    description_list = crawl_news_data(xhr_responses)
    for data in description_list:
        print(data)