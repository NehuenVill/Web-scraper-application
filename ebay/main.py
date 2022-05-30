from interface_class import *
from helper_class import *
import math

class EBAYCLASS():

    def __init__(self):
        self.interface = INTERFACING()
        self.helper = Helper()
        self.config = self.helper.read_json_file('./config.json')

        self.output_data_folder = self.helper.checking_folder_existence(self.config['output_data_folder'])
        self.output_data_folder = self.helper.checking_folder_existence(self.output_data_folder + self.helper.get_timestamp() + '/')
        self.log_folder = self.helper.checking_folder_existence(self.output_data_folder + 'log/')
        self.base_url = 'https://www.ebay.com'

    def get_search_results(self,current_url,page_number):
        
        soup = self.interface.make_soup_url(current_url, 'lxml', 'search', True, True)
        if not soup:
            print("No response?")
            return [], 0

        if "We're unable to show you more than 10,000 results." in soup.text.strip():
            return [], 0

        products = soup.find('ul',class_=re.compile('srp-results')).find_all('li',class_=re.compile('s-item'))

        try:
            pages = math.ceil(int(soup.find('h1',class_='srp-controls__count-heading').span.text.replace(',',''))/192)
        except:
            pages = 99
        products_list = []
        for product in products:
            try:
                product_link = product.a['href']
            except:
                continue

            try:
                product_name = product.h3.text.strip()
            except:
                continue

            try:
                listing_date = product.find('span',class_=re.compile('s-item__listingDate')).span.text.strip()
            except:
                listing_date = ''

            products_list.append([page_number,product_name,product_link,listing_date])

        print("Total Scraped Links: ",len(products_list))
        print("Total Pages:", pages)

        return products_list, pages

    def get_product_information(self,current_url,listing_date):

        soup = self.interface.make_soup_url(current_url,'lxml', 'product')
        if not soup:
            print("No response?")
            return []

        # self.helper.write_random_file(soup,'random.html')

        try:
            product_title = soup.find('h1',class_='x-item-title__mainTitle').span.text
        except:
            try:
                product_title = soup.find('h1',id='itemTitle').span.nextSibling.strip()
            except:
                product_title = ''

        print('Product Title: ',product_title)

        try:
            all_categories_html = soup.find('nav', class_='breadcrumbs').find_all('a',class_='seo-breadcrumb-text')
        except:
            all_categories_html = []
        all_categories = ' > '.join([_category.text.strip() for _category in all_categories_html])
        print('All Categories: ',all_categories)

        product_price = soup.find('span',id='prcIsum')
        if product_price is None:
            product_price = soup.find('span',id='mm-saleDscPrc')

        if product_price is not None:
            product_price = product_price.text.strip()
        else:
            product_price = ''

        print('Product Price: ',product_price)

        try:
            starting_bid = soup.find('span',id='prcIsum_bidPrice').text.strip()
        except:
            try:
                starting_bid = soup.find('div',text='Starting bid:').find_next_sibling('div').text.strip()
            except:
                starting_bid = ''
        print('Starting Bid: ',starting_bid)

        try:
            item_location = soup.find('div',text=re.compile('Item location:')).find_next_sibling('div').text.strip()
        except:
            try:
                item_location = soup.find('div',text=re.compile('Located in:')).find_next_sibling('div').text.strip()
            except:
                try:
                    item_location = soup.find(lambda tag:tag.name=="span" and "Located in:" in tag.text).text.replace('Located in:', '').strip()
                except:
                    item_location = ''
        print('Item Location: ',item_location)

        try:
            ships_to = soup.find('div',text=re.compile('Ships to:')).find_next_sibling('div').text.strip().split('|')[0].strip()
        except:
            ships_to = ''
        print('Ships to: ',ships_to)

        try:
            shipping_price = soup.find('span',id="fshippingCost").span.text.strip()
        except:
            shipping_price = ''

        try:
            shipping_type = soup.find('span',id='fShippingSvc').get_text('###').split('###')[0].strip()
        except:
            shipping_type = ''
        print('Shipping Type: ',shipping_type)

        # try:
        #     payment_methods_html = soup.find('div',id='payDet1').find_all('img')
        # except:
        #     payment_methods_html = []

        # payments_methods = []
        # for _payment in payment_methods_html:
        #     payments_methods.append(_payment['title'])

        # payments_methods = '|'.join(payments_methods)
        # print('Payment Methods: ',payments_methods)

        payments_methods = []
        try:
            for span in soup.find('div', class_='d-payments-minview').find('div', class_='ux-labels-values__values-content').find_all('span'):
                if 'aria-label' in span.attrs:
                    payments_methods.append(span.attrs['aria-label'])
        except:
            payments_methods = []
        payments_methods = '|'.join(payments_methods)
        print('Payment Methods: ',payments_methods)

        try:
            seller_name = soup.find('h2',class_='si-ttl').find_next_sibling('div').find('span',class_='mbg-nw').text.strip()
        except:
            try:
                seller_name = soup.find('h2',class_='si-bsl-ttl').find_next_sibling('div').find('span',class_='mbg-nw').text.strip()
            except:
                try:
                    seller_name = soup.find('div', string="Seller:").find_next_sibling('div').a.span.text
                except:
                    try:
                        seller_name = soup.find('div', attrs={'data-testid': 'ux-seller-section__item--seller'}).find_all('a')[0].span.text
                    except:
                        seller_name = ''
        print('Seller Name: ',seller_name)

        try:
            seller_url = soup.find('h2',class_='si-ttl').find_next_sibling('div').find('span',class_='mbg-nw').parent['href']
        except:
            try:
                seller_url = soup.find('h2',class_='si-bsl-ttl').find_next_sibling('div').find('span',class_='mbg-nw').parent['href']
            except:
                try:
                    seller_url = soup.find('div', string="Seller:").find_next_sibling('div').a.attrs['href']
                except:
                    try:
                        seller_url = soup.find('div', attrs={'data-testid': 'ux-seller-section__item--seller'}).find_all('a')[0].attrs['href']
                    except:
                        seller_url = ''
        print('Seller URL: ',seller_url)

        try:
            seller_rating_count = soup.find('h2',class_='si-ttl').find_next_sibling('div').find('span',class_='mbg-l').a.text.strip()
        except:
            try:
                seller_rating_count = soup.find('h2',class_='si-bsl-ttl').find_next_sibling('div').find('span',class_='mbg-l').a.text.strip()
            except:
                try:
                    seller_rating_count = soup.find('div', attrs={'data-testid': 'ux-seller-section__item--seller'}).find_all('a')[1].span.text
                except:
                    seller_rating_count = ''
        print('Seller Rating Count: ',seller_rating_count)

        try:
            seller_positive_feedback = soup.find('h2',class_='si-ttl').find_next_sibling('div').find('div',id='si-fb').text.strip()
        except:
            try:
                seller_positive_feedback = soup.find('h2',class_='si-bsl-ttl').find_next_sibling('div').find('div',id='si-fb').text.strip()
            except:
                try:
                    seller_positive_feedback = soup.find('div', attrs={'data-testid': 'ux-seller-section__item--seller'}).parent.find_next_sibling('div').span.text
                except:
                    seller_positive_feedback = ''
        print('Seller Positive Feedback: ',seller_positive_feedback)

        try:
            ebay_item_number = soup.find('div',id='descItemNumber').text.strip()
        except:
            ebay_item_number = ''
        print('Ebay Item Number: ',ebay_item_number)

        type_of_buy = []
        type_of_buy_bid = soup.find('a',{'data-cta':'placebid'})
        if type_of_buy_bid is not None:
            type_of_buy.append('Bidding')

        type_of_buy_offers = soup.find('a',text=re.compile('Buy It Now'))
        if type_of_buy_offers is None:
            type_of_buy_offers = soup.find('input',value=re.compile('Buy It Now'))

        if type_of_buy_offers is not None:
            type_of_buy.append('Buy It Now')


        type_of_buy_offers = soup.find('a',text=re.compile('Make Offer'))
        if type_of_buy_offers is not None:
            type_of_buy.append('Make Offer')

        type_of_buy = '|'.join(type_of_buy)
        print('Type of Buy: ',type_of_buy)

        # try:
        #     description_html = soup.find('h2',text='Item specifics').parent.find_all('table')[-1].find_all('tr')
        # except:
        #     description_html = []
        # description = {}

        # for _desc in description_html:
        #     desc = _desc.find_all('td')
        #     for td in range(0,len(desc),2):
        #         try:
        #             description[desc[td].text.strip()] = ' '.join(desc[td + 1].text.strip().split())
        #         except:
        #             continue

        # description = {}
        # try:
        #     for row in soup.find('div', {"data-testid":"ux-layout-section__item"}).find_all('div', class_="ux-layout-section__row"):
        #         items = row.find_all('div',recursive=False)
        #         for idx, item in enumerate(items):
        #             if item.attrs['class'][0] == 'ux-labels-values__labels':
        #                 description[items[idx].text] = items[idx+1].text
        # except:
        #     print('Could not get description')


        # TOP DESCRIPTION SMALL
        # description = {}
        # try:
        #     for form in soup.find('h2', id='itemInfoLabel').parent.find_all('form'):
        #         try:
        #             div = form.find('div', class_='nonActPanel')
        #             for info in div.find_all('div', recursive=False):
        #                 for keyvalue in info.find_all('div', recursive=False):
        #                     if keyvalue.has_attr('class') and 'lable' in keyvalue.attrs['class']:
                                
        #                         key = keyvalue.text.replace('\n', '').replace('\t','').replace('\xa0', '').strip()
        #                     else:
                                
        #                         value = keyvalue.text.replace('\n', '').replace('\t','').replace('\xa0', '').strip()
        #                 description[key] = value
        #         except:
        #             continue
        # except:
        #     print('Could not get description')
        description = {}
        try:
            for row in soup.find('div', class_={'ux-layout-section__item--table-view'}).find_all('div', class_='ux-layout-section__row'):
                keys = [x.text.strip() for x in row.find_all('div', class_='ux-labels-values__labels')]
                values =  [x.text.strip() for x in row.find_all('div', class_='ux-labels-values__values')]
                for k, v in zip(keys,values):
                    description[k] = v
        except:
            print('Could not get description')
        

        if 0: #no need of this part to get the desription.
            product_description_url = soup.find('iframe',id='desc_ifr')['src']
            print('Frame URL: ',product_description_url)

            soup = self.interface.make_soup_url(product_description_url,'lxml', 'description')
            product_description_html = soup.find('div',id='ds_div').contents
            product_description = []

            for _desc in product_description_html:
                if len(_desc.strip()) > 0:
                    product_description.append(_desc.strip())

            product_description = '\n'.join(product_description)

            print('Product Description: ',product_description)

        #description['product_description'] = product_description

        print(json.dumps(description,indent=4))

        return [product_title,current_url,product_price,starting_bid,item_location,ships_to,shipping_price,
                shipping_type,payments_methods,type_of_buy,seller_name,seller_url,seller_positive_feedback,seller_rating_count,
                ebay_item_number,json.dumps(description),all_categories,listing_date]

    def start_scraping_brands(self):
        
        processed_json_file = self.log_folder + 'brands_processed.json'
        processed_json_data = self.helper.json_exist_data(processed_json_file)

        headers = ['page_number','extracted_title','extracted_urls','listing_date']

        input_urls_file = self.config['brands_input_urls_file']

        if not self.helper.is_file_exist(input_urls_file):
            print("Brands file not exist..")
            return False

        input_urls_data = self.helper.reading_csv(input_urls_file)

        for _url in range(1,len(input_urls_data)):

            if len(input_urls_data[_url]) < 2:
                continue

            current_brand = input_urls_data[_url][0]
            current_url = input_urls_data[_url][1]

            current_filename = self.output_data_folder + current_brand.replace(' ','_') + '.csv'
            current_filename = current_filename.lower()

            print(_url," / ",len(input_urls_data)," : ",current_brand)

            if current_url not in processed_json_data:

                page_number = 1
                blank_page_count = 0
                pages = 54  
                
                while page_number < pages+1:
                    _current_url = f'{current_url}&_pgn={page_number}'
                    if _current_url  not in processed_json_data:
                        search_results, pages = self.get_search_results(_current_url,page_number)
                        if len(search_results) > 0:
                            self.helper.writing_output_file(search_results,headers,current_filename) 
                            
                            processed_json_data.append(_current_url)
                            self.helper.write_json_file(processed_json_data,processed_json_file)
                        else:
                            break

                    else:
                        print(_current_url, ' already processed...')
                    
                    page_number += 1
                        
                processed_json_data.append(current_url)
                self.helper.write_json_file(processed_json_data,processed_json_file)

            else:
                print(current_url," already processed...")

            print('*'*50)
            print()

        return True

    def start_scraping_each_product_details(self):

        processed_json_file = self.log_folder + 'products_processed.json'
        processed_json_data = self.helper.json_exist_data(processed_json_file)

        headers = ['Title','URL','Price: Buy it Now','Price: Bidding','Location','Ships To','Shipping Price',
                'Shipping Type','Payment Methods','Type of Buy','Seller: Name','Seller: URL','Seller: Rating','Seller: Amount of Ratings',
                'eBay Item Number','Description','All Categories','Listing Date'] 
    
        all_files = self.helper.list_all_files(self.output_data_folder,'.csv')

        for _file in range(len(all_files)):

            current_filename = all_files[_file]

            if 'product' in current_filename:
                continue

            current_file_data = self.helper.reading_csv(current_filename)

            output_file_name = current_filename.replace('.csv','_products.csv')
            vendors_output_file_name = current_filename.replace('.csv','_products_vendors.csv')

            for data in range(1,len(current_file_data)):
                current_url = current_file_data[data][2]

                print(data," / ",len(current_file_data))

                if current_url.split('?')[0] not in processed_json_data:

                    product_data = self.get_product_information(current_url,current_file_data[data][-1])
                    if len(product_data) > 0:
                        self.helper.writing_output_file([product_data],headers,output_file_name)

                    processed_json_data.append(current_url.split('?')[0])
                    self.helper.write_json_file(processed_json_data,processed_json_file)

                else:
                    print(current_url, " Already Processed....")

                print('-'*50)
                print()

    def get_vendor_information(self,vendor_url,seller_name):

        soup = self.interface.make_soup_url(vendor_url, 'lxml', 'vendor')

        try:
            seller_description = soup.find('h2',class_='bio inline_value').text.strip()
        except:
            seller_description = ''
        print('Seller Description: ',seller_description)

        try:
            rating_count = soup.find('a',title=re.compile(' feedback score is'))['title'].split()[-1]
        except:
            rating_count = ''
        print('Rating Count: ',rating_count)

        try:
            positive_feedback_percent = soup.find('div',class_='perctg').text.strip()
        except:
            positive_feedback_percent = ''
        print('Positive Feedback: ',positive_feedback_percent)

        try:
            products_url = soup.find('a',text=re.compile('Items for sale'))['href']
        except:
            products_url = ''
        print('Products URL: ',products_url)

        try:
            store_url = soup.find('a',text='Visit store')['href']
        except:
            store_url = ''
        print('Store URL: ',store_url)

        try:
            feedback_url = soup.find('a',title='See all feedback')['href']
        except:
            feedback_url = ''
        print('Feedback URL: ',feedback_url)

        try:
            member_since = soup.find('span',text=re.compile('Member since:')).find_next_sibling('span').text.strip()
        except:
            member_since = ''
        print('Member Since: ',member_since)

        try:
            seller_location = soup.find('span',class_='mem_loc').text.strip()
        except:
            seller_location = ''
        print('Seller Location: ',seller_location)

        try:
            detailed_rating_postive = soup.find('a',title='Positive').find('span',class_='num').text.strip()
        except:
            detailed_rating_postive = ''
        print('Detailed Rating Positive: ',detailed_rating_postive)

        try:
            detailed_rating_neutral = soup.find('a',title='Neutral').find('span',class_='num').text.strip()
        except:
            detailed_rating_neutral = ''
        print('Detailed Rating Neutral: ',detailed_rating_neutral)

        try:
            detailed_rating_negative = soup.find('a',title='Negative').find('span',class_='num').text.strip()
        except:
            detailed_rating_negative = ''
        print('Detailed Rating Negative: ',detailed_rating_negative)

        try:
            feedback_ratings_html = soup.find('div',id='dsr').find_all('div',class_=re.compile('fl each'))
        except:
            feedback_ratings_html = []
        feedback_ratings = []
        for _feedback in feedback_ratings_html:
            feedback_title = _feedback.find('span',class_='dsr_type').text.strip()
            feedback_count = _feedback.find('span',class_='dsr_type').find_previous_sibling('span').text.strip()
            feedback_ratings.append(f'{feedback_title} ({feedback_count})')

        feedback_ratings = '|'.join(feedback_ratings)
        print('Feedback Ratings: ',feedback_ratings)

        return [seller_name,vendor_url,seller_description,rating_count,positive_feedback_percent,products_url,
                store_url,feedback_url,member_since,seller_location,
                detailed_rating_postive,detailed_rating_neutral,detailed_rating_negative,feedback_ratings]

    def start_scraping_vendors_information(self):

        processed_json_file = self.log_folder + 'products_vendor_information_processed.json'
        processed_json_data = self.helper.json_exist_data(processed_json_file)

        headers = ['Name','URL','Description','Amount of Ratings','Rating','Products URL',
                'Store URL','Feedback URL','Member Since','Location',
                'Amount of Positive Ratings','Amount of Neutral Ratings','Amount of Negative Ratings','Rating Description']

        all_files = self.helper.list_all_files(self.output_data_folder,'.csv')

        for _file in range(len(all_files)):

            current_filename = all_files[_file]

            if '_products.csv' not in current_filename:
                continue

            current_file_data = self.helper.reading_csv(current_filename)

            output_file_name = current_filename.replace('.csv','_vendors_information.csv')

            for data in range(1,len(current_file_data)):
                try:
                    vendor_url = current_file_data[data][11]
                except:
                    continue
                vendor_name = current_file_data[data][10]
                
                if len(vendor_url) < 3:
                    continue

                print(data," / ",len(current_file_data))

                if vendor_url.split('?')[0] not in processed_json_data:

                    vendors_data = self.get_vendor_information(vendor_url,vendor_name)

                    if len(vendors_data) > 0:
                        self.helper.writing_output_file([vendors_data],headers,output_file_name)

                    processed_json_data.append(vendor_url.split('?')[0])
                    self.helper.write_json_file(processed_json_data,processed_json_file)

                else:
                    print(vendor_url, " Already Processed....")

                print('-'*50)
                print()

    def get_vendor_feedbacks(self,vendor_url,vendor_name):

        feedback_url = f'https://www.ebay.com/fdbk/update_feedback_profile?url=username%3D{vendor_name}%26filter%3Dfeedback_page%253AAll%252Cperiod%253ATWELVE_MONTHS%26page_id%3D1%26limit%3D100&module=%3Fmodules%3DFEEDBACK_SUMMARY'

        json_data = self.interface.get_url_response_json(feedback_url)
        json_data = json.loads(json_data)

        try:
            all_feedbacks = json_data['modules']['FEEDBACK_SUMMARY']['feedbackView']['feedbackCards']
        except:
            all_feedbacks = []

        feedbacks = []

        for _feedback in range(len(all_feedbacks)):
            feedback = all_feedbacks[_feedback]
            print(_feedback," : ",len(all_feedbacks))

            try:
                feedback_title = feedback['feedbackInfo']['comment']['textSpans'][0]['text']
            except:
                feedback_title = ''
            
            print('Feedback Title: ',feedback_title)

            try:
                feedback_product = feedback['feedbackInfo']['item']['itemTitle']['textSpans'][0]['text']
            except:
                feedback_product = ''

            print('Feedback Product: ',feedback_product)

            try:
                feedback_buyer = feedback['feedbackInfo']['context']['textSpans'][0]['text']
            except:
                feedback_buyer = ''

            print('Feedback Buyer: ',feedback_buyer)
            try:
                feedback_price = feedback['feedbackInfo']['item']['itemPrice']['textSpans'][0]['text']
            except:
                feedback_price = ''

            try:
                feedback_best_price_note = feedback['feedbackInfo']['item']['note']['textSpans'][0]['text']
            except:
                feedback_best_price_note = ''

            feedback_price += ('. ' + feedback_best_price_note)

            print('Feedback Price: ',feedback_price)

            try:
                feedback_date = feedback['feedbackInfo']['contextTime']['textSpans'][0]['text']
            except:
                feedback_date = ''

            print('Feedback Date: ',feedback_date)

            print('-'*50)
            print()

            feedbacks.append([vendor_name,vendor_url,feedback_title,feedback_product,feedback_price,feedback_buyer,feedback_date])
        
        return feedbacks

    def start_scraping_vendors_feedbacks(self):

        processed_json_file = self.log_folder + 'products_vendor_feedbacks_processed.json'
        processed_json_data = self.helper.json_exist_data(processed_json_file)

        headers = ['vendor_name','vendor_url','feedback_title','feedback_product','feedback_price','feedback_buyer','feedback_date']

        all_files = self.helper.list_all_files(self.output_data_folder,'.csv')

        for _file in range(len(all_files)):

            current_filename = all_files[_file]

            if '_vendors_information.csv' not in current_filename:
                continue

            current_file_data = self.helper.reading_csv(current_filename)

            output_file_name = current_filename.replace('_vendors_information','_vendors_feedbacks')

            for data in range(1,len(current_file_data)):
                vendor_url = current_file_data[data][1]
                vendor_name = current_file_data[data][0]
                
                if len(vendor_url) < 3:
                    continue

                print(data," / ",len(current_file_data))

                if vendor_url not in processed_json_data:

                    vendor_feedbacks = self.get_vendor_feedbacks(vendor_url,vendor_name)

                    if len(vendor_feedbacks) > 0:
                        self.helper.writing_output_file(vendor_feedbacks,headers,output_file_name)

                    processed_json_data.append(vendor_url)
                    self.helper.write_json_file(processed_json_data,processed_json_file)

                else:
                    print(vendor_url, " Already Processed....")

                print('-'*50)
                print()

    def start_scraping_vendor_products(self):

        processed_json_file = self.log_folder + 'products_vendor_products_processed.json'
        processed_json_data = self.helper.json_exist_data(processed_json_file)

        headers = ['page_number','extracted_title','extracted_urls']

        all_files = self.helper.list_all_files(self.output_data_folder,'.csv')

        for _file in range(len(all_files)):

            current_filename = all_files[_file]

            if '_vendors_information.csv' not in current_filename:
                continue

            current_file_data = self.helper.reading_csv(current_filename)

            output_file_name = current_filename.replace('_vendors_information','_vendors_products')

            for data in range(1,len(current_file_data)):

                current_url = current_file_data[data][5]
                current_url = f'{current_url}&_ipg=200&'

                print(data," / ",len(current_file_data))

                if current_url not in processed_json_data:
                    page_number = 1
                    blank_page_count = 0

                    while 1:
                        _current_url = f'{current_url}_pgn={page_number}'

                        if _current_url  not in processed_json_data:
                            search_results, pages = self.get_search_results(_current_url,page_number)

                            if len(search_results) > 0:
                                self.helper.writing_output_file(search_results,headers,output_file_name) 
                                
                                processed_json_data.append(_current_url)
                                self.helper.write_json_file(processed_json_data,processed_json_file)
                            else:
                                break

                        else:
                            print(_current_url, ' already processed...')
                        
                        page_number += 1
                            
                    processed_json_data.append(current_url)
                    self.helper.write_json_file(processed_json_data,processed_json_file)

                else:
                    print(current_url," already processed...")

                print('*'*50)
                print()

if __name__ == "__main__":

    handle = EBAYCLASS()

    # need input file in input_data folder with name brands_url_to_scrap.csv

    print("Scraping Brands....")
    handle.start_scraping_brands()

    print()
    print("Scraping Each Product's Details...")
    handle.start_scraping_each_product_details()

    print()
    print("Scraping Vendors Information...")
    handle.start_scraping_vendors_information()

    # print()
    # print('Scraping Vendors Feedbacks...')
    # handle.start_scraping_vendors_feedbacks()

    # print()
    # print("Scraping Vendor Product Listing...")
    # output_data_folder = handle.start_scraping_vendor_products()