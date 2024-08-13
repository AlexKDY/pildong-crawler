from selenium import webdriver

# Chrome WebDriver 객체 생성
driver = webdriver.Chrome()

# 웹 페이지 열기
driver.get('https://www.google.com')

# 드라이버 종료
driver.quit()
