from helper_class import *


class INTERFACING():

    def __init__(self):
        self._MAX_TRIAL_REQUESTS = 10
        self._WAIT_TIME_BETWEEN_REQUESTS = 10

    def get_url_response_json(self,url):

        print('Getting JSON response of: ',url)
        scrapeowl_url = "https://api.scrapeowl.com/v1/scrape"
        object_of_data = {
                "api_key": "F5GCB7P3SAHMCFNKFzBofsIvzVhFAttf9JIDnCO4XGP8bpY5x02OtLxmtte9",
                "url": url
                }

        data = json.dumps(object_of_data)

        status_code = 999 
        count = 0
        
        while status_code != 200 and count < self._MAX_TRIAL_REQUESTS:
            try:
                headers = {
                "Content-Type": "application/json"
                }
                response = requests.post(scrapeowl_url, data, timeout=180, headers=headers)
                status_code = response.status_code
                print("Status Code: ",status_code)
                if status_code == 200:
                    html = response.json()['html']
                    try: 
                        html[20]
                        return html
                    except Exception as e: 
                        print(e)
                        status_code = 999
            except Exception as e: 
                print(e)
                status_code = 999
            count += 1
            time.sleep(self._WAIT_TIME_BETWEEN_REQUESTS)

    def check_page_validity(self, html_content):

        valid_page = False
        try:
            if "Sign in for the best experience" in html_content:
                print("++++++++++++Page Source is InValid-Option-1...")
                valid_page = False

            elif "The request could not be satisfied." in html_content:
                print("++++++++++++Page Source is InValid-Option-2...")
                valid_page = False

            elif "Robot Check" in html_content:
                print("++++++++++++Page Source is InValid-Option-3...")
                valid_page = False

            elif len(html_content) < 100:
                print("++++++++++++Page Source is InValid-Option-4...")
                valid_page = False

            else:
                # print()
                # print("------------------Page Source is Valid...")
                # print()
                valid_page = True
        except:
            pass

        return valid_page

    def get_url_response(self, url, check, premium_proxies, render_js):
        if check == 'product' or check == 'vendor':
            url = url.split('?')[0]
        print("Getting HTML Response of: ",url)
        scrapeowl_url = "https://api.scrapeowl.com/v1/scrape"
        object_of_data = {
                "api_key": "F5GCB7P3SAHMCFNKFzBofsIvzVhFAttf9JIDnCO4XGP8bpY5x02OtLxmtte9",
                "url": url,
                "render_js": render_js,
                "premium_proxies": premium_proxies,
                "country": "us"
                }

        data = json.dumps(object_of_data)

        status_code = 999 
        count = 0
        
        while status_code != 200 and count < self._MAX_TRIAL_REQUESTS:
            try:
                headers = {
                "Content-Type": "application/json"
                }
                response = requests.post(scrapeowl_url, data, timeout=180, headers=headers)
                status_code = response.status_code
                print('Status Code: ',status_code)
                if status_code == 200:
                    html = response.json()['html']
                    if check == 'search':
                        try:
                            soup = BeautifulSoup(html, 'lxml')
                            soup.find('ul',class_=re.compile('srp-results')).find_all('li',class_=re.compile('s-item'))
                            return html
                        except Exception as e: 
                            print(e)
                            status_code = 999
                    if check == 'product':
                        try:
                            soup = BeautifulSoup(html, 'lxml')
                            soup.find('div',text=re.compile('Item location:')).find_next_sibling('div').text.strip()
                            return html
                        except:
                            try:
                                soup = BeautifulSoup(html, 'lxml')
                                soup.find('div',text=re.compile('Located in:')).find_next_sibling('div').text.strip()
                                return html
                            except Exception as e: 
                                try:
                                    soup = BeautifulSoup(html, 'lxml')
                                    soup.find(lambda tag:tag.name=="span" and "Located in:" in tag.text).text.replace('Located in:', '').strip()
                                    return html
                                except:
                                    print(e)
                                    status_code = 999
                    elif check == 'vendor':
                        try:
                            soup = BeautifulSoup(html, 'lxml')
                            soup.find('span',text=re.compile('Member since:')).find_next_sibling('span').text.strip()
                            return html
                        except Exception as e: 
                            print(e)
                            status_code = 999
                    else:
                        try: 
                            html[20]
                            return html
                        except Exception as e: 
                            print(e)
                            status_code = 999
            except Exception as e: 
                print(e)
                status_code = 999
                
            count += 1
            time.sleep(self._WAIT_TIME_BETWEEN_REQUESTS)
        
        return False

    def get_page_html(self,search_url, check, premium_proxies, render_js):

        trials = 0
        res = None

        print("-"*50)
        print()
        while trials < 2:

            res = self.get_url_response(search_url, check, premium_proxies, render_js)
            if not res:
                return False

            valid_page = self.check_page_validity(res)
            trials += 1

            if valid_page:
                break

            time.sleep(self._WAIT_TIME_BETWEEN_REQUESTS)

        return res

    def make_soup_url(self,page_url,parser, check, premium_proxies=False, render_js=False):
        html_response = self.get_page_html(page_url, check, premium_proxies, render_js)
        if not html_response:
            return html_response

        return BeautifulSoup(html_response, parser)

if __name__ == "__main__":
    interface = INTERFACING()