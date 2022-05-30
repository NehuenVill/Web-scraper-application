import pandas as pd
import numpy as np
import re
import os
import requests
import calendar
import json
from ast import literal_eval
from datetime import datetime


def get_file(platform, brand, date, filetype):
    if filetype == '':
        return pd.read_csv('D:/Pellervo/'+ brand +'/raw/'+ platform +'/'+ date +'/'+ platform + '-' + date + '-' + brand +'.csv')
    return pd.read_csv('D:/Pellervo/'+ brand +'/raw/'+ platform +'/'+ date +'/'+ platform + '-' + date + '-' + brand +'_'+ filetype +'.csv')

def save_file(df, platform, brand, date, filetype):
    if not os.path.exists('D:/Pellervo/'+ brand +'/clean/'+ platform +'/'+ date ):
        os.makedirs('D:/Pellervo/'+ brand +'/clean/'+ platform +'/'+ date )
    df.to_csv('D:/Pellervo/'+ brand +'/clean/'+ platform +'/'+ date +'/'+ platform + '-' + date + '-' + brand +'_'+ filetype +'.csv', index=False)

class AmazonFrCleaner():
    def __init__(self, brand, date):
        self.brand = brand
        self.date = date
        self.df_prve = get_file('amazon_fr', brand, date, 'products_vendors')
        self.df_pr = get_file('amazon_fr', brand, date, 'products')
        self.df_ve = get_file('amazon_fr', brand, date, 'vendors')
        self.df_prve_new = pd.DataFrame()
        self.df_pr_new = pd.DataFrame()
        self.df_ve_new = pd.DataFrame()

    def get_vendor_url(self, x):
        if x == None or x == '' or type(x) == float:
            return 'https://www.amazon.fr'
        else:
            return 'https://www.amazon.fr/sp?seller=' + x

    def clean_price(self, x):
        if '€' in x:
            currency = '€'
        else:
            currency = ''
        return x.replace('€','').replace(',','.').replace(' ','').replace('\xa0','').replace('\u202f','').strip(), currency

    def clean_rating_amount(self, x):
        amount = ''
        match = re.search(r'\((.*?) évaluations', x)
        if match!= None:
            amount = match.group(1)
        return amount

    def clean_delivery(self, x):
        country = ''
        match = re.search(r'depuis (.*?)\.', x)
        if match!= None:
            country = match.group(1)
        return country

    def clean_sales_rank(self, x):
        rank = x.split('en')[0].replace(',', '').strip()
        return x, rank

    def clean_rating(self, x):
        try:
            return float(x.replace('sur 5 étoiles','').replace(',','.').strip()) / 5
        except:
            return ''

    def clean_rating_amount(self, x):
        return x.replace('évaluations','').replace('évaluation','').replace('\xa0','').strip()

    def clean_list(self, x):
        x = literal_eval(x)
        gender = ''
        for g in ['Homme', 'Femme', 'Bébé', 'Garçon', 'Fille']:
            if g in x:
                gender = g
                x.remove(g)
        if 'Chaussures femme' in x: 
            x.remove('Chaussures femme')
            gender = 'Femme'
        if 'Chaussures homme' in x: 
            x.remove('Chaussures homme')
            gender = 'Homme'
        if len(x) == 0:
            return gender, '', '', '', '', ''
        if len(x) == 1:
            return gender, ' | '.join(x), x[0], '', '', ''
        if len(x) == 2:
            return gender, ' | '.join(x), x[0], x[1], '', ''
        if len(x) == 3:
            return gender, ' | '.join(x), x[0], x[1], x[2], ''
        if len(x) > 3:
            return gender, ' | '.join(x), x[0], x[1], x[2], x[3]

    def translate_date(self, x):
        months = {
            "janvier": "January",
            "février": "February",
            "mars": "March",
            "avril": "April",
            "mai": "May",
            "juin": "June",
            "juillet": "July",
            "août": "August",
            "septembre": "September",
            "octobre": "October",
            "novembre": "November",
            "décembre": "December"
        }
        for m in months.keys():
            x = x.replace(m, months[m])
        return x

    def clean_details(self, details, specs):
        if type(details) == float:
            details = '{}'
        if type(specs) ==  float:
            specs = '{}'
        x_old = {**literal_eval(details), **literal_eval(specs)}
        result = []
        x = {}
        for old_key in x_old:
            if 'matériau' in old_key.lower() or 'materiau' in old_key.lower():
                new_key = 'Matériau'
            else:
                new_key = old_key.replace('\u200f','')
            x[new_key] = x_old[old_key]
        for tag in ['ASIN', 'Date de mise en ligne sur Amazon.fr', 'Service', 'Fabricant',  "Numéro du modèle de l'article", 
                    "Classement des meilleures ventes d'Amazon", 'Moyenne des commentaires client',
                    "Dimensions du produit (L x l x h)", "Dimensions du colis",  'Is Discontinued By Manufacturer',
                    "Pays d'origine", 'Référence constructeur', 'Marque', "Poids de l'article", 'Package Weight', 
                    'Couleur du modèle', 'Volume indicatif', 'Format', 'Couleur', 'Caractéristiques spéciales', 'Forme', 
                    'Public cible', 'Type de peau', 'Spécialité', 'Numéro de pièce', 'Garantie', "Type d'affichage", 
                    'Numéro du modèle', 'Matériau']:
            if tag in x:
                result.append(x[tag].replace('\u200e',''))
            else:
                result.append('')
        return result

    def clean_double_column(self, x, y):
        if type(x) != float and x != '':
            return x
        elif type(y) != float and y != '':
            return y
        else:
            return ''

    def clean_date_posted(self, x):
        if x != None and x != '' and pd.notnull(x):
            return datetime.strptime(x, "%d %B %Y")
        else:
            return ''

    def get_lifetime_rating(self, x):
        x_list = x.split('/')
        try:
            rating = x_list[-1].replace('%','')
            rating = float(rating)/100
        except:
            rating = ''
        return rating

    def get_lifetime_count(self, x):
        x_list = x.split('/')
        try:
            count = x_list[-1]
        except:
            count = ''
        return count.replace('\xa0','')

    def clean_products_vendors(self):
        self.df_prve = self.df_prve.drop_duplicates(subset=['ID','Vendor: ID'])
        self.df_prve_new[['ID', 'Title', 'Condition']] = self.df_prve[['ID', 'Title', 'Condition']]
        self.df_prve_new['Vendor URL'] = self.df_prve.apply(lambda x: self.get_vendor_url(x['Vendor: ID']), axis=1)
        self.df_prve_new[['Price', 'Currency']] = self.df_prve.apply(lambda x: self.clean_price(x['Price']) if pd.notnull(x['Price']) else ('',''), axis = 1, result_type="expand")
        self.df_prve_new = self.df_prve_new[['ID', 'Price', 'Currency', 'Condition', 'Vendor URL']]    

    def clean_products(self):
        self.df_pr = self.df_pr.drop_duplicates(subset='ID')
        self.df_pr[['ASIN', 'Date Posted', 'Service', 'Manufacturer', "Item Model Number",
                "Best Sellers Rank", 'Average Customer Reviews',
                 "Product Dimensions", "Package Dimensions", 'Discontinued By Manufacturer',
                "Country", "Manufacturer Reference", "Brand", "Item Weight", "Package Weight",
                'Model Color', 'Indicative Volume', 'Format', 'Color', 'Special Characteristics', 'Shape',
                'Target audience', 'Skin Type', 'Specialty', 'Part Number', 'Guarantee', "Display Type",
                'Model Number', 'Material']] = self.df_pr.apply(lambda x: self.clean_details(x['Details'], x['Specs']) if pd.notnull(x['Details']) or pd.notnull(x['Specs']) else ('',)*29, axis=1, result_type="expand")
        self.df_pr_new[['ID', 'URL', 'Title', 'Description', 'Brand', 'Country']] = self.df_pr[['ID', 'URL', 'Title', 'Description', 'Brand', 'Country']]
        self.df_pr_new['Image URL'] = self.df_pr['Image']
        self.df_pr_new['Date Posted'] = self.df_pr.apply(lambda x: self.clean_date_posted(self.translate_date(x['Date Posted'])) if pd.notnull(x['Date Posted']) else x['Date Posted'], axis = 1)
        self.df_pr_new['Rating'] = self.df_pr.apply(lambda x: self.clean_rating(x['Rating']) if pd.notnull(x['Rating']) else x['Rating'], axis = 1)
        self.df_pr_new['Rating Count'] = self.df_pr.apply(lambda x: self.clean_rating_amount(x['Amount of Ratings']) if pd.notnull(x['Amount of Ratings']) else x['Amount of Ratings'], axis = 1)
        self.df_pr_new[['Gender', 'Category', 'Category 1', 'Category 2', 'Category 3', 'Category 4']] = self.df_pr.apply(lambda x: self.clean_list(x['Category Tree']) if pd.notnull(x['Category Tree']) else ('', '', '', '', '', ''), axis=1, result_type="expand")
        self.df_pr_new[['Sales Rank Description', 'Sales Rank']] = self.df_pr.apply(lambda x: self.clean_sales_rank(x['Best Sellers Rank']) if pd.notnull(x['Best Sellers Rank']) else ('',''), axis = 1, result_type="expand")
        self.df_pr_new['Model Number'] = self.df_pr.apply(lambda x: self.clean_double_column(x['Item Model Number'], x['Model Number']), axis=1)
        self.df_pr_new['Color'] = self.df_pr.apply(lambda x: self.clean_double_column(x['Model Color'], x['Color']), axis=1)
        self.df_pr_new['Special Features'] = self.df_pr.apply(lambda x: self.clean_double_column(x['Special Characteristics'], x['Specialty']), axis=1)
        self.df_pr_new[['ASIN', 'Manufacturer', "Product Dimensions", "Package Dimensions", 'Discontinued By Manufacturer', 
       "Manufacturer Reference", "Brand", "Item Weight", "Package Weight", 'Indicative Volume', 'Format', 'Shape','Target audience', 'Skin Type', 'Part Number', 'Guarantee', 
       "Display Type", "Material"]] = self.df_pr[['ASIN', 'Manufacturer', "Product Dimensions", "Package Dimensions",
                                 'Discontinued By Manufacturer', 
       "Manufacturer Reference", "Brand", "Item Weight", "Package Weight", 'Indicative Volume', 'Format', 'Shape','Target audience', 'Skin Type', 'Part Number', 'Guarantee', 
       "Display Type", "Material"]]

    def combine_products_vendors(self):
        self.df_prve_merged = self.df_prve_new.merge(self.df_pr_new, how='left')
        self.df_prve_merged['URL URL'] = self.df_prve_merged['Vendor URL'].str.lower()+' '+self.df_prve_merged['URL'].str.lower()
        self.df_prve_merged['Location'] = self.df_prve_merged['Country']
        self.df_prve_merged = self.df_prve_merged[['URL URL', 'URL', 'Title', 'Description', 'Category', 'Gender', 'Price', 'Currency', 'Condition', 'Location', 
                 'Date Posted', 'Vendor URL', 'Rating', 'Rating Count', 'Brand', 'Image URL', 
               'Sales Rank', 'Sales Rank Description', 'ASIN', 'Manufacturer', 'Model Number', 'Special Features',
                "Product Dimensions", "Package Dimensions",  "Item Weight", "Package Weight", 'Discontinued By Manufacturer', 
        'Material', 'Color', 'Category 1', 'Category 2', 'Category 3', 'Category 4']]

    def clean_vendors(self):
        self.df_ve_new[['ID', 'Name', 'Description', 'Legal Name',
       'Type', 'Number', 'Address Client', 'Address Commercial', 'Positive Rating Timeline', 'Neutral Rating Timeline', 'Negative Rating Timeline', 'Rating Count Timeline', 'VAT']] = self.df_ve[['ID', 'Name', 'Description', 'Legal Name',
       'Type', 'Number', 'Address Client', 'Address Commercial', 'Positive (30/90/12/lifetime)', 'Neutral (30/90/12/lifetime)',
       'Negative (30/90/12/lifetime)', 'Count (30/90/12/lifetime)', 'TVA']]
        self.df_ve_new['URL'] = self.df_ve.apply(lambda x: self.get_vendor_url(x['ID']) if pd.notnull(x['ID']) else x['ID'], axis = 1)
        self.df_ve_new['Positive Rating'] = self.df_ve_new.apply(lambda x: self.get_lifetime_rating(x['Positive Rating Timeline']) if pd.notnull(x['Positive Rating Timeline']) else x['Positive Rating Timeline'], axis = 1)
        self.df_ve_new['Neutral Rating'] = self.df_ve_new.apply(lambda x: self.get_lifetime_rating(x['Neutral Rating Timeline']) if pd.notnull(x['Neutral Rating Timeline']) else x['Neutral Rating Timeline'], axis = 1)
        self.df_ve_new['Negative Rating'] = self.df_ve_new.apply(lambda x: self.get_lifetime_rating(x['Negative Rating Timeline']) if pd.notnull(x['Negative Rating Timeline']) else x['Negative Rating Timeline'], axis = 1)
        self.df_ve_new['Rating Count'] = self.df_ve_new.apply(lambda x: self.get_lifetime_count(x['Rating Count Timeline']) if pd.notnull(x['Rating Count Timeline']) else x['Rating Count Timeline'], axis = 1)
        self.df_ve_new['Rating'] = self.df_ve_new['Positive Rating']
        self.df_ve_new['Member Since'] = ''
        self.df_ve_new['Location'] = self.df_ve_new.apply(lambda x: x['Address Commercial'].strip(',').strip() if pd.notnull(x['Address Commercial']) else '', axis=1)
        self.df_ve_new = self.df_ve_new[['URL', 'Name', 'Description', 'Rating', 'Rating Count', 'Location', 'Member Since', 'VAT',
           'Legal Name',  'Type', 'Number',
       'Rating Count Timeline', 'Positive Rating Timeline', 'Neutral Rating Timeline', 'Negative Rating Timeline', 
       'Positive Rating', 'Neutral Rating', 'Negative Rating', 'Address Client', 'Address Commercial']]
        self.df_ve_new = self.df_ve_new.append({'URL':'https://www.amazon.fr', 'Name':'Amazon.fr'}, ignore_index=True)

    def save_products_vendors(self):
        save_file(self.df_prve_merged, 'amazon_fr', self.brand, self.date, 'products')
        save_file(self.df_ve_new, 'amazon_fr', self.brand, self.date, 'vendors')


class CdiscountComCleaner():
    def __init__(self, brand, date):
        self.brand = brand
        self.date = date
        self.df_prve = get_file('cdiscount_com', brand, date, 'products_vendors')
        self.df_pr = get_file('cdiscount_com', brand, date, 'products')
        self.df_ve = get_file('cdiscount_com', brand, date, 'vendors')
        self.df_prve_new = pd.DataFrame()
        self.df_pr_new = pd.DataFrame()
        self.df_ve_new = pd.DataFrame()

    def clean_vendor_url(self, x):
        if 'cdiscount' not in x:
            x = 'https://www.cdiscount.com/' + x
        return x.replace('/mpvv', '/mpv').replace('//mpv', '/mpv').replace('#_his_','')

    def clean_price(self, x):
        if '€' in x:
            currency = '€'
        else:
            currency = ''
        return x.replace('€','.') , currency

    def get_info(self, jInfo, key):
        if jInfo is None:
            return ''
        if key in jInfo:
            if jInfo[key] == None:
                return ''
            return jInfo[key]
        else:
            return ''

    def get_values(self, d):
        r = []
        for c in ['Nom du produit', 'Catégorie', 'Marque', 'Référence', 'Couleur(s)', 'Dessus / Tige', 'Genre', 'Forme de monture']:
            r.append(self.get_info(d, c))
        return r

    def clean_rating_count(self, x):
        return x.replace('avis', '').strip()

    def clean_capital(self, x):
        return x.replace('EUR', '').strip()

    def clean_products_vendors(self):
        self.df_prve_new[['URL', 'Title', 'Condition', 'Country']] = self.df_prve[['Product URL', 'Product Title', 'Etat', "Pays d'expédition"]]
        self.df_prve_new['Vendor URL'] = self.df_prve.apply(lambda x: self.clean_vendor_url(x['Vendor Link']) if pd.notnull(x['Vendor Link']) else '', axis=1)
        self.df_prve_new[['Price', 'Currency']] = self.df_prve.apply(lambda x: self.clean_price(x['Vendor Price']) if pd.notnull(x['Vendor Price']) else ('',''), axis = 1, result_type="expand")
        self.df_prve_new = self.df_prve_new[['URL', 'Price', 'Currency', 'Condition', 'Country', 'Vendor URL']]

    def clean_products(self):
        self.df_pr_new[['URL', 'Title', 'Rating', 'Rating Count', 'Description']] = self.df_pr[['URL', 'Title', 'Rating', 'Reviews Count', 'product_description']]
        self.df_pr_new[['Product Name', 'Category', 'Brand', 'Reference', 'Color', 'Material', 'Gender', 'Frame Shape' ]] = self.df_pr.apply(lambda x: self.get_values(dict(zip(x[['identifier_name_'+str(i) for i in range(1,11)]], x[['identifier_value_'+str(i) for i in range(1,11)]]))), axis=1, result_type="expand")

    def combine_products_vendors(self):
        self.df_prve_merged = self.df_prve_new.merge(self.df_pr_new, how='left', left_on='URL', right_on='URL')
        self.df_prve_merged['URL URL'] = self.df_prve_merged['Vendor URL'].str.lower()+' '+self.df_prve_merged['URL'].str.lower()
        self.df_prve_merged['Location'] = self.df_prve_merged['Country']
        self.df_prve_merged['Date Posted'] = ''
        self.df_prve_merged = self.df_prve_merged[['URL URL', 'URL', 'Title', 'Description', 'Category', 'Gender', 'Price', 'Currency', 'Condition', 'Location',
                        'Date Posted', 'Vendor URL', 'Rating', 'Rating Count', 'Product Name', 'Brand', 'Material', 'Color']]

    def clean_vendors(self):
        self.df_ve_new['URL'] = self.df_ve.apply(lambda x: self.clean_vendor_url(x['vendor_url']) if pd.notnull(x['vendor_url']) else '', axis=1)
        self.df_ve_new[['Name', 'Legal Name', 'Legal Status', 'Registration Number', 'VAT', 'Address HQ', 'Address Customer Service', 
           'Return Address', 'Origin Country', 'Rating Count', 'Rating']] = self.df_ve[['vendor_name', 'Raison Sociale',
       'Statut et forme juridique de l’entreprise', "N° d'Immatriculation", 'Numéro individuel d’identification TVA',
       'Adresse du siège sociale', 'Adresse du service clientèle',
       'Adresse pour effectuer les retours', "Pays d'expédition",
       'reviews_count', 'rating_count']]
        self.df_ve_new['Capital'] = self.df_ve.apply(lambda x: self.clean_capital(x['Capital social']) if pd.notnull(x['Capital social']) else x['Capital social'], axis=1)
        for i in [1,2,3,4,5]:
            c = str(i)+ ' star rating count'
            self.df_ve_new[c.title()] = self.df_ve.apply(lambda x: self.clean_rating_count(x[c]) if pd.notnull(x[c]) else x[c], axis=1)

        self.df_ve_new['Description'] = '' # change this later!
        self.df_ve_new['Member Since'] = ''
        self.df_ve_new['Location'] = self.df_ve_new['Address HQ']

        self.df_ve_new = self.df_ve_new[['URL', 'Name', 'Description', 'Rating', 'Rating Count', 'Location', 'Member Since', 'VAT', 'Legal Name', 
                'Legal Status', 'Registration Number', 'Address HQ', 'Address Customer Service', 'Return Address',
                'Origin Country', 'Capital', '1 Star Rating Count', '2 Star Rating Count', '3 Star Rating Count',
                '4 Star Rating Count', '5 Star Rating Count' ]]

    def save_products_vendors(self):
        save_file(self.df_prve_merged, 'cdiscount_com', self.brand, self.date, 'products')
        save_file(self.df_ve_new, 'cdiscount_com', self.brand, self.date, 'vendors')

class EbayComCleaner():
    def __init__(self, brand, date):
        self.brand = brand
        self.date = date
        self.translations = {}
        self.df_pr = get_file('ebay_com', brand, date, 'products')
        self.df_ve = get_file('ebay_com', brand, date, 'vendors')
        self.df_pr_new = pd.DataFrame()
        self.df_ve_new = pd.DataFrame()

    def clean_date(self):
        self.df_pr['Date Posted'] = ''
        initial_year = year = int(self.date[-4:])
        month_nr = int(self.date[3:5])
        # months = {month: index for index, month in enumerate(["janv.", "févr.", "mars", "avr.", "mai", "juin", "juil.", "août", "sept.", "oct.", "nov.", "déc."]) if month}
        for index, row in self.df_pr.iterrows():
            try:
                page = row['page_number']
                if page == 1:
                    year = initial_year
                if datetime.strptime(row['Listing Date']+' '+str(year), "%b-%d %H:%M %Y") > datetime.today():
                    year = initial_year - 1
                month_nr_new = row['Listing Date'][:2]
                if month_nr_new == 12 and month_nr == 1:
                    year = year-1
                month_nr = month_nr_new
                if 'Feb-29' in row['Listing Date']:
                    year = 2020
                self.df_pr.loc[index, 'Date Posted'] = datetime.strptime(row['Listing Date']+' '+str(year), "%b-%d %H:%M %Y")
            except Exception as e:
                print(e, year, row['Listing Date'])

    def clean_price(self, x):
        x = x.replace(',','.')
        if x == 'FREE':
            return 0, ''
        amount = re.findall(r'[-+]?\d*\.\d+|\d+', x)
        if len(amount) == 1:
            amount = amount[0]
            currency = x.replace(amount, '').replace(' ','')
            return amount, currency
        else:
            return '', ''

    def combine_price(self, x):
        if pd.notnull(x['Price: Buy it Now']) and x['Price: Buy it Now'] != '':
            return x['Price: Buy it Now'], x['Price: Buy it Now Currency']
        elif pd.notnull(x['Price: Bidding']) and x['Price: Bidding'] != '':
            return x['Price: Bidding'], x['Price: Bidding Currency']
        else:
            return '', ''

    def clean_location(self, x):
        x_list = x.split(',')
        if len(x_list) == 1:
            return x_list[-1].strip(), '', ''
        if len(x_list) == 2:
            return x_list[-1].strip(), '', x_list[-2].strip()
        if len(x_list) >= 3:
            return x_list[-1].strip(), x_list[-2].strip(), x_list[-3].strip()

    def clean_payment_method(self, x):
        amex = False
        discover = False
        google_pay = False
        master_card = False
        paypal = False
        visa = False
        if 'Amex' in x or 'American Express' in x:
            amex = True
        if 'Discover' in x:
            discover = True
        if 'Google Pay' in x:
            google_pay = True
        if 'Master Card' in x or 'MasterCard' in x or 'Mastercard' in x:
            master_card = True
        if 'PayPal' in x:
            paypal = True
        if 'Visa' in x:
            visa = True
        return amex, discover, google_pay, master_card, paypal, visa

    def clean_rating(self, x):
        amount = re.findall(r'[-+]?\d*\.\d+|\d+', x)
        try:
            return float(amount[0])/100
        except:
            return ''

    def clean_description(self, x):
        result = ['']*25
        for i, tag in enumerate(['Brand', 'Color', 'Style', 'Department', 'Condition', 'Type', 'Character', 'UPC', 'Material',
    'Upper Material', 'Pattern', 'Size', 'Model', 'Closure', 'Features', 'Theme', 'Accents', 'Size Type', 'Occasion',
    'US Shoe Size', 'Heel Height', 'Vintage', 'Shoe Width', "US Shoe Size (Women's)", 'Heel Style']):
            if tag+':' in x:
                result[i] = x[tag+':']
        return result

    def clean_categories(self, x):
        gender = ''
        try:
            x.split('>')
        except:
            return '', '', '', '', ''
        categories = [c.strip() for c in x.split('>')]
        if '' in categories:
            categories.remove('')
        for c in categories:
            if c in ['women', 'female', 'Women', 'Female']:
                categories.remove(c)
                gender = 'Women'
            if c in ['men', 'male', 'Men', 'Male']:
                categories.remove(c)
                gender = 'Men'
            if c in ['children', 'kids', 'boys', 'girls', 'Children', 'Kids', 'Boys', 'Girls']:
                categories.remove(c)
                gender = 'Kids'
            if c in ['babies', 'baby', 'Babies', 'Baby']:
                categories.remove(c)
                gender = 'Baby'
        if len(categories) == 0:
            return gender, '', '', '', ''
        if len(categories) == 1:
            return gender, ' | '.join(categories), categories[0], '', ''
        if len(categories) == 2:
            return gender, ' | '.join(categories), categories[0], categories[1], ''
        if len(categories) >= 3:
            return gender, ' | '.join(categories), categories[0], categories[1], categories[2]    
    
    def clean_rating_description(self, x):
        return x.replace('|', ' | ')

    def clean_member_since(self, x):
        try:
            return datetime.strptime(x, "%d %m %Y").date()
        except:
            try:
                return datetime.strptime(x, "%Y年%m月%d日").date()
            except Exception as e:
                return np.NaN

    def clean_products(self):
        self.clean_date()
        self.df_pr_new[['Title', 'Ships To', 'Shipping Type', 'Type of Buy', 'eBay Item Number', 'Date Posted']] = self.df_pr[['Title', 'Ships To', 'Shipping Type', 'Type of Buy', 'eBay Item Number', 'Date Posted']] 
        self.df_pr_new['URL'] = self.df_pr.apply(lambda x: x['URL'].split('?')[0], axis=1)
        self.df_pr_new['Vendor URL'] = self.df_pr.apply(lambda x: x['Seller: URL'].split('?')[0] if pd.notnull(x['Seller: URL']) else '', axis=1)
        self.df_pr_new[['Price: Buy it Now', 'Price: Buy it Now Currency']] = self.df_pr.apply(lambda x: self.clean_price(x['Price: Buy it Now']) if pd.notnull(x['Price: Buy it Now']) else ('', ''), axis=1, result_type="expand")
        self.df_pr_new[['Price: Bidding', 'Price: Bidding Currency']] = self.df_pr.apply(lambda x: self.clean_price(x['Price: Bidding']) if pd.notnull(x['Price: Bidding']) else ('', ''), axis=1, result_type="expand")
        self.df_pr_new[['Shipping Price', 'Shipping Price: Currency']] = self.df_pr.apply(lambda x: self.clean_price(x['Shipping Price']) if pd.notnull(x['Shipping Price']) else ('', ''), axis=1, result_type="expand")
        self.df_pr_new[['Price', 'Currency']] = self.df_pr_new.apply(lambda x: self.combine_price(x), axis=1, result_type="expand")
        self.df_pr_new['Location'] = self.df_pr['Location'].str.title()
        self.df_pr_new[['Country', 'State', 'City']] = self.df_pr.apply(lambda x: self.clean_location(x['Location']) if pd.notnull(x['Location']) else ('', '', ''), axis=1, result_type="expand")
        self.df_pr_new[['Amex', 'Discover', 'Google Pay', 'Master Card', 'PayPal', 'Visa']] =  self.df_pr.apply(lambda x: self.clean_payment_method(x['Payment Methods']) if pd.notnull(x['Payment Methods']) else ('','','','','',''), axis=1, result_type="expand")
        self.df_pr_new[['Brand', 'Color', 'Style', 'Department', 'Condition', 'Type', 'Character', 'UPC', 'Material',
 'Upper Material', 'Pattern', 'Size', 'Model', 'Closure', 'Features', 'Theme', 'Accents', 'Size Type', 'Occasion',
 'US Shoe Size', 'Heel Height', 'Vintage', 'Shoe Width', "US Shoe Size Women", 'Heel Style']] = self.df_pr.apply(lambda x: self.clean_description(literal_eval(x['Description'])) if pd.notnull(x['Description']) else ('',)*25, axis=1,  result_type="expand")
        self.df_pr_new['Condition'] = self.df_pr_new.apply(lambda x: x['Condition'].split(':')[0], axis=1)
        self.df_pr_new[['Gender', 'Category', 'Category 1', 'Category 2', 'Category 3']] = self.df_pr.apply(lambda x: self.clean_categories(x['All Categories']), axis=1, result_type="expand")
        self.df_pr_new['Description'] = ''
        self.df_pr_new['URL URL'] = self.df_pr_new['Vendor URL'].str.lower() + ' ' + self.df_pr_new['URL'].str.lower()
        self.df_pr_new = self.df_pr_new[['URL URL', 'URL', 'Title', 'Description', 'Category', 'Gender', 'Price', 'Currency', 'Condition', 
                       'Location' , 'Date Posted', 'Vendor URL',
           'Ships To', 'Shipping Type', 'Type of Buy', 'eBay Item Number', 'Price: Buy it Now',
           'Price: Buy it Now Currency', 'Price: Bidding','Price: Bidding Currency', 'Shipping Price', 'Shipping Price: Currency', 'Country', 'State', 'City',
           'Amex', 'Discover','Google Pay', 'Master Card', 'PayPal', 'Visa', 
           'Brand', 'Color', 'Style', 'Department', 'Type', 'Character', 'UPC',
           'Material', 'Upper Material', 'Pattern', 'Size', 'Model', 'Closure',
           'Features', 'Theme', 'Accents', 'Size Type', 'Occasion', 'US Shoe Size',
           'Heel Height', 'Vintage', 'Shoe Width', "US Shoe Size Women",
           'Heel Style', 'Category 1', 'Category 2','Category 3']]

    def clean_vendors(self):
        self.df_ve_new[['Name', 'Description']] = self.df_ve[['Name', 'Description']]
        self.df_ve_new['URL'] = self.df_ve.apply(lambda x: x['URL'].split('?')[0], axis=1)
        self.df_ve_new['Products URL'] = self.df_ve.apply(lambda x: x['Products URL'].split('?')[0] if pd.notnull(x['Products URL']) else '', axis=1)
        self.df_ve_new['Store URL'] = self.df_ve.apply(lambda x: x['Store URL'].split('?')[0] if pd.notnull(x['Store URL']) else '', axis=1)
        self.df_ve_new['Feedback URL'] = self.df_ve.apply(lambda x: x['Feedback URL'].split('?')[0] if pd.notnull(x['Feedback URL']) else '', axis=1)
        self.df_ve_new['Member Since'] = self.df_ve.apply(lambda x: self.clean_member_since(x['Member Since']) if pd.notnull(x['Member Since']) else x['Member Since'], axis=1)
        self.df_ve_new[['Rating Count','Positive Rating Count','Neutral Rating Count','Negative Rating Count', 'Location']] = self.df_ve[['Amount of Ratings', 'Amount of Positive Ratings', 'Amount of Neutral Ratings',
                       'Amount of Negative Ratings', 'Location']]
        self.df_ve_new['Rating'] = self.df_ve.apply(lambda x: self.clean_rating(x['Rating']) if pd.notnull(x['Rating']) else x['Rating'], axis=1)
        self.df_ve_new['Rating Description'] = self.df_ve.apply(lambda x: self.clean_rating_description(x['Rating Description']) if pd.notnull(x['Rating Description']) else x['Rating Description'], axis=1)
        self.df_ve_new['Positive Rating Count'] = self.df_ve_new.apply(lambda x: int(str(x['Positive Rating Count']).replace(',','').replace('\xa0', '')) if pd.notnull(x['Positive Rating Count']) and type(x['Positive Rating Count'])!= float  else x['Positive Rating Count'], axis=1)
        self.df_ve_new['Neutral Rating Count'] = self.df_ve_new.apply(lambda x: int(str(x['Neutral Rating Count']).replace(',','').replace('\xa0', '')) if pd.notnull(x['Neutral Rating Count']) and type(x['Neutral Rating Count'])!= float else x['Neutral Rating Count'], axis=1)
        self.df_ve_new['Negative Rating Count'] = self.df_ve_new.apply(lambda x: int(str(x['Negative Rating Count']).replace(',','').replace('\xa0', '')) if pd.notnull(x['Negative Rating Count']) and type(x['Negative Rating Count'])!= float else x['Negative Rating Count'], axis=1)
        self.df_ve_new = self.df_ve_new[['URL', 'Name', 'Description', 'Rating', 'Rating Count', 'Location', 'Member Since',
           'Positive Rating Count', 'Neutral Rating Count', 'Negative Rating Count', 'Rating Description',
           'Products URL', 'Store URL','Feedback URL']]

    def save_products_vendors(self):
        save_file(self.df_pr_new, 'ebay_com', self.brand, self.date, 'products')
        save_file(self.df_ve_new, 'ebay_com', self.brand, self.date, 'vendors')


class EbayFrCleaner():
    def __init__(self, brand, date):
        self.brand = brand
        self.date = date
        self.translations = {}
        self.df_pr = get_file('ebay_fr', brand, date, 'products')
        self.df_ve = get_file('ebay_fr', brand, date, 'vendors')
        self.df_pr_new = pd.DataFrame()
        self.df_ve_new = pd.DataFrame()

    def clean_date(self):
        self.df_pr['Date Posted'] = ''
        initial_year = year = int(self.date[-4:])
        month_nr = int(self.date[3:5])
        # months = {month: index for index, month in enumerate(["janv.", "févr.", "mars", "avr.", "mai", "juin", "juil.", "août", "sept.", "oct.", "nov.", "déc."]) if month}
        for index, row in self.df_pr.iterrows():

            for idx, month in enumerate(["janv.", "févr.", "mars", "avr.", "mai", "juin", "juil.", "août", "sept.", "oct.", "nov.", "déc."]):
                row['Listing Date'] = row['Listing Date'].replace(month, str(idx+1))
            try:
                page = row['page_number']
                if page == 1:
                    year = initial_year
                if datetime.strptime(row['Listing Date']+' '+str(year), "%m-%d %H:%M %Y") > datetime.today():
                    year = initial_year - 1
                month_nr_new = row['Listing Date'][:2]
                if month_nr_new == 12 and month_nr == 1:
                    year = year-1
                month_nr = month_nr_new
                if '2-29' in row['Listing Date']:
                    year = 2020
                self.df_pr.loc[index, 'Date Posted'] = datetime.strptime(row['Listing Date']+' '+str(year), "%m-%d %H:%M %Y")
            except Exception as e:
                print(e, year, row['Listing Date'])

    def clean_price(self, x):
        x = x.replace(',','.')
        if x == 'FREE':
            return 0, ''
        amount = re.findall(r'[-+]?\d*\.\d+|\d+', x)
        if len(amount) == 1:
            amount = amount[0]
            currency = x.replace(amount, '').replace(' ','')
            return amount, currency
        else:
            return '', ''

    def combine_price(self, x):
        if pd.notnull(x['Price: Buy it Now']) and x['Price: Buy it Now'] != '':
            return x['Price: Buy it Now'], x['Price: Buy it Now Currency']
        elif pd.notnull(x['Price: Bidding']) and x['Price: Bidding'] != '':
            return x['Price: Bidding'], x['Price: Bidding Currency']
        else:
            return '', ''

    def clean_location(self, x):
        x_list = x.split(',')
        if len(x_list) == 1:
            return x_list[-1].strip(), '', ''
        if len(x_list) == 2:
            return x_list[-1].strip(), '', x_list[-2].strip()
        if len(x_list) >= 3:
            return x_list[-1].strip(), x_list[-2].strip(), x_list[-3].strip()

    def clean_payment_method(self, x):
        amex = False
        discover = False
        google_pay = False
        master_card = False
        paypal = False
        visa = False
        if 'Amex' in x or 'American Express' in x:
            amex = True
        if 'Discover' in x:
            discover = True
        if 'Google Pay' in x:
            google_pay = True
        if 'Master Card' in x or 'MasterCard' in x or 'Mastercard' in x:
            master_card = True
        if 'PayPal' in x:
            paypal = True
        if 'Visa' in x:
            visa = True
        return amex, discover, google_pay, master_card, paypal, visa

    def clean_rating(self, x):
        amount = re.findall(r'[-+]?\d*\.\d+|\d+', x)
        try:
            return float(amount[0])/100
        except:
            return ''

    def clean_description(self, x):
        result = ['']*25
        for i, tag in enumerate(['Brand', 'Color', 'Style', 'Department', '\u00c9tat', 'Type', 'Character', 'UPC', 'Material',
    'Upper Material', 'Pattern', 'Size', 'Model', 'Closure', 'Features', 'Theme', 'Accents', 'Size Type', 'Occasion',
    'US Shoe Size', 'Heel Height', 'Vintage', 'Shoe Width', "US Shoe Size (Women's)", 'Heel Style']):
            if tag+':' in x:
                result[i] = x[tag+':']
        return result

    def clean_categories(self, x):
        gender = ''
        try:
            x.split('>')
        except:
            return '', '', '', '', ''
        categories = [c.strip() for c in x.split('>')]
        if '' in categories:
            categories.remove('')
        if len(categories) == 0:
            return gender, '', '', '', ''
        if len(categories) == 1:
            return gender, ' | '.join(categories), categories[0], '', ''
        if len(categories) == 2:
            return gender, ' | '.join(categories), categories[0], categories[1], ''
        if len(categories) >= 3:
            return gender, ' | '.join(categories), categories[0], categories[1], categories[2]    
    
    def clean_rating_description(self, x):
        return x.replace('|', ' | ')

    def clean_member_since(self, x):
        for idx, month in enumerate(["janv.", "févr.", "mars", "avr.", "mai", "juin", "juil.", "août", "sept.", "oct.", "nov.", "déc."]):
            x = x.replace(month, str(idx+1))
        try:
            return datetime.strptime(x, "%d %m %Y").date()
        except:
            try:
                return datetime.strptime(x, "%Y年%m月%d日").date()
            except Exception as e:
                return np.NaN

    def clean_products(self):
        self.clean_date()
        self.df_pr_new[['Title', 'Ships To', 'Shipping Type', 'Type of Buy', 'eBay Item Number', 'Date Posted']] = self.df_pr[['Title', 'Ships To', 'Shipping Type', 'Type of Buy', 'eBay Item Number', 'Date Posted']] 
        self.df_pr_new['URL'] = self.df_pr.apply(lambda x: x['URL'].split('?')[0], axis=1)
        self.df_pr_new['Vendor URL'] = self.df_pr.apply(lambda x: x['Seller: URL'].split('?')[0] if pd.notnull(x['Seller: URL']) else '', axis=1)
        self.df_pr_new[['Price: Buy it Now', 'Price: Buy it Now Currency']] = self.df_pr.apply(lambda x: self.clean_price(x['Price: Buy it Now']) if pd.notnull(x['Price: Buy it Now']) else ('', ''), axis=1, result_type="expand")
        self.df_pr_new[['Price: Bidding', 'Price: Bidding Currency']] = self.df_pr.apply(lambda x: self.clean_price(x['Price: Bidding']) if pd.notnull(x['Price: Bidding']) else ('', ''), axis=1, result_type="expand")
        self.df_pr_new[['Shipping Price', 'Shipping Price: Currency']] = self.df_pr.apply(lambda x: self.clean_price(x['Shipping Price']) if pd.notnull(x['Shipping Price']) else ('', ''), axis=1, result_type="expand")
        self.df_pr_new[['Price', 'Currency']] = self.df_pr_new.apply(lambda x: self.combine_price(x), axis=1, result_type="expand")
        self.df_pr_new['Location'] = self.df_pr['Location'].str.title()
        self.df_pr_new[['Country', 'State', 'City']] = self.df_pr.apply(lambda x: self.clean_location(x['Location']) if pd.notnull(x['Location']) else ('', '', ''), axis=1, result_type="expand")
        self.df_pr_new[['Amex', 'Discover', 'Google Pay', 'Master Card', 'PayPal', 'Visa']] =  self.df_pr.apply(lambda x: self.clean_payment_method(x['Payment Methods']) if pd.notnull(x['Payment Methods']) else ('','','','','',''), axis=1, result_type="expand")
        self.df_pr_new[['Brand', 'Color', 'Style', 'Department', 'Condition', 'Type', 'Character', 'UPC', 'Material',
 'Upper Material', 'Pattern', 'Size', 'Model', 'Closure', 'Features', 'Theme', 'Accents', 'Size Type', 'Occasion',
 'US Shoe Size', 'Heel Height', 'Vintage', 'Shoe Width', "US Shoe Size Women", 'Heel Style']] = self.df_pr.apply(lambda x: self.clean_description(literal_eval(x['Description'])) if pd.notnull(x['Description']) else ('',)*25, axis=1,  result_type="expand")
        self.df_pr_new['Condition'] = self.df_pr_new.apply(lambda x: x['Condition'].split(':')[0], axis=1)
        self.df_pr_new[['Gender', 'Category', 'Category 1', 'Category 2', 'Category 3']] = self.df_pr.apply(lambda x: self.clean_categories(x['All Categories']), axis=1, result_type="expand")
        self.df_pr_new['Description'] = ''
        self.df_pr_new['URL URL'] = self.df_pr_new['Vendor URL'].str.lower() + ' ' + self.df_pr_new['URL'].str.lower()
        self.df_pr_new = self.df_pr_new[['URL URL', 'URL', 'Title', 'Description', 'Category', 'Gender', 'Price', 'Currency', 'Condition', 
                       'Location' , 'Date Posted', 'Vendor URL',
           'Ships To', 'Shipping Type', 'Type of Buy', 'eBay Item Number', 'Price: Buy it Now',
           'Price: Buy it Now Currency', 'Price: Bidding','Price: Bidding Currency', 'Shipping Price', 'Shipping Price: Currency', 'Country', 'State', 'City',
           'Amex', 'Discover','Google Pay', 'Master Card', 'PayPal', 'Visa', 
           'Brand', 'Color', 'Style', 'Department', 'Type', 'Character', 'UPC',
           'Material', 'Upper Material', 'Pattern', 'Size', 'Model', 'Closure',
           'Features', 'Theme', 'Accents', 'Size Type', 'Occasion', 'US Shoe Size',
           'Heel Height', 'Vintage', 'Shoe Width', "US Shoe Size Women",
           'Heel Style', 'Category 1', 'Category 2','Category 3']]

    def clean_vendors(self):
        self.df_ve_new[['Name', 'Description']] = self.df_ve[['Name', 'Description']]
        self.df_ve_new['URL'] = self.df_ve.apply(lambda x: x['URL'].split('?')[0], axis=1)
        self.df_ve_new['Products URL'] = self.df_ve.apply(lambda x: x['Products URL'].split('?')[0] if pd.notnull(x['Products URL']) else '', axis=1)
        self.df_ve_new['Store URL'] = self.df_ve.apply(lambda x: x['Store URL'].split('?')[0] if pd.notnull(x['Store URL']) else '', axis=1)
        self.df_ve_new['Feedback URL'] = self.df_ve.apply(lambda x: x['Feedback URL'].split('?')[0] if pd.notnull(x['Feedback URL']) else '', axis=1)
        self.df_ve_new['Member Since'] = self.df_ve.apply(lambda x: self.clean_member_since(x['Member Since']) if pd.notnull(x['Member Since']) else x['Member Since'], axis=1)
        self.df_ve_new[['Rating Count','Positive Rating Count','Neutral Rating Count','Negative Rating Count', 'Location']] = self.df_ve[['Amount of Ratings', 'Amount of Positive Ratings', 'Amount of Neutral Ratings',
                       'Amount of Negative Ratings', 'Location']]
        self.df_ve_new['Rating'] = self.df_ve.apply(lambda x: self.clean_rating(x['Rating']) if pd.notnull(x['Rating']) else x['Rating'], axis=1)
        self.df_ve_new['Rating Description'] = self.df_ve.apply(lambda x: self.clean_rating_description(x['Rating Description']) if pd.notnull(x['Rating Description']) else x['Rating Description'], axis=1)
        self.df_ve_new['Positive Rating Count'] = self.df_ve_new.apply(lambda x: int(str(x['Positive Rating Count']).replace(',','').replace('\xa0', '')) if pd.notnull(x['Positive Rating Count']) and type(x['Positive Rating Count'])!= float  else x['Positive Rating Count'], axis=1)
        self.df_ve_new['Neutral Rating Count'] = self.df_ve_new.apply(lambda x: int(str(x['Neutral Rating Count']).replace(',','').replace('\xa0', '')) if pd.notnull(x['Neutral Rating Count']) and type(x['Neutral Rating Count'])!= float else x['Neutral Rating Count'], axis=1)
        self.df_ve_new['Negative Rating Count'] = self.df_ve_new.apply(lambda x: int(str(x['Negative Rating Count']).replace(',','').replace('\xa0', '')) if pd.notnull(x['Negative Rating Count']) and type(x['Negative Rating Count'])!= float else x['Negative Rating Count'], axis=1)
        self.df_ve_new = self.df_ve_new[['URL', 'Name', 'Description', 'Rating', 'Rating Count', 'Location', 'Member Since',
           'Positive Rating Count', 'Neutral Rating Count', 'Negative Rating Count', 'Rating Description',
           'Products URL', 'Store URL','Feedback URL']]

    def save_products_vendors(self):
        save_file(self.df_pr_new, 'ebay_fr', self.brand, self.date, 'products')
        save_file(self.df_ve_new, 'ebay_fr', self.brand, self.date, 'vendors')

class LeboncoinFrCleaner():
    def __init__(self, brand, date):
        self.brand = brand
        self.date = date
        self.df_pr = get_file('leboncoin_fr', brand, date, 'products')
        self.df_ve = get_file('leboncoin_fr', brand, date, 'vendors')
        self.df_pr_new = pd.DataFrame()
        self.df_ve_new = pd.DataFrame()

    def get_location(self, places):
        location = ''
        for p in places:
            if not pd.isnull(p):
                location += ', '
                location += str(p)
        return location.strip(',').strip()

    def get_url(self, x):
        return 'https://www.leboncoin.fr/profil/' + x

    def clean_products(self):
        self.df_pr_new[['URL', 'Title', 'Description', 'Price', 'Condition', 'Gender', 'Category',
           'Brand', 'Color', 'Shoe Size', 'Material','Size',
           'Country ID', 'Region', 'Department', 'City', 'Zipcode', 'Latitude', 'Longitude', 'Index Date', 'Date Posted']] = self.df_pr[['URL', 'Title', 'Description', 'Price', 'Condition', 'Gender', 'Category',
           'Brand', 'Color', 'Shoe Size', 'Material','Size',
           'Country ID', 'Region', 'Department', 'City', 'Zipcode', 'Latitude', 'Longitude', 'Index Date', 'Publication Date']]
        self.df_pr_new['Currency'] = 'EUR'
        self.df_pr_new['Vendor URL'] = self.df_pr.apply(lambda x: self.get_url(x['Vendor: ID']) if pd.notnull(x['Vendor: ID']) else '', axis=1)
        self.df_pr_new['Location'] = self.df_pr_new.apply(lambda x: self.get_location([x['Zipcode'], x['City'], x['Department'], x['Region'], x['Country ID']]), axis=1)
        self.df_pr_new['URL URL'] = self.df_pr_new['Vendor URL'].str.lower()+' '+self.df_pr_new['URL'].str.lower()
        self.df_pr_new = self.df_pr_new[['URL URL', 'URL', 'Title', 'Description', 'Category', 'Gender', 'Price', 'Currency', 'Condition', 
                       'Location', 'Date Posted', 'Vendor URL', 'Brand', 'Material', 'Color', 'Size', 'Shoe Size', 'Index Date']]
        self.df_pr_new = self.df_pr_new.drop_duplicates(subset='URL URL')

    def clean_vendors(self):
        df_ve_2 = self.df_pr[['Vendor: Store ID', 'Vendor: ID', 'Vendor: Type', 'Vendor: Name', 'Vendor: SIREN']].drop_duplicates()
        self.df_ve = self.df_ve.merge(df_ve_2, how='left', left_on = 'ID', right_on='Vendor: ID')
        self.df_ve_new[['Name', 'Rating', 'Rating Count', 'Reply Time', 'Type', 'SIREN']] = self.df_ve[['Vendor: Name', 'Rating', 'Amount of Ratings', 'Reply Time', 'Vendor: Type', 'Vendor: SIREN']]
        self.df_ve_new['URL'] = self.df_ve.apply(lambda x: self.get_url(x['Vendor: ID']) if pd.notnull(x['Vendor: ID']) else '', axis=1)
        self.df_ve_new['Location'] = ''
        self.df_ve_new['Member Since'] = '' #could scrape this field with expensive requests
        self.df_ve_new['Description'] = ''
        self.df_ve_new = self.df_ve_new[['URL', 'Name', 'Description', 'Rating', 'Rating Count', 'Location', 'Member Since', 'Reply Time', 'Type', 'SIREN']]

    def save_products_vendors(self):
        save_file(self.df_pr_new, 'leboncoin_fr', self.brand, self.date, 'products')
        save_file(self.df_ve_new, 'leboncoin_fr', self.brand, self.date, 'vendors')

class MercariComCleaner():
    def __init__(self, brand, date):
        self.brand = brand
        self.date = date
        self.df_pr = get_file('mercari_com', brand, date, 'products')
        self.df_ve = get_file('mercari_com', brand, date, 'vendors')
        self.df_pr_new = pd.DataFrame()
        self.df_ve_new = pd.DataFrame()

    def clean_price(self, x):
        x = x.replace(',','.')
        if x.lower() == 'free':
            return 0, ''
        amount = re.findall(r'[-+]?\d*\.\d+|\d+', x)
        if len(amount) == 1:
            amount = amount[0]
            currency = x.replace(amount, '').replace(' ','')
            return amount, currency
        else:
            return '', ''

    def clean_paypal(self, x):
        if 'Paypal' in x:
            return True
        else:
            return False

    def clean_categories(self, x, brand):
        brand = brand.replace('_', ' ').capitalize()
        x = x.split('/')
        x = [item.strip() for item in x]
        x = [item.replace("Women's",'').strip().capitalize() for item in x]
        x = [item.replace("Men's",'').strip().capitalize() for item in x]
        gender = ''
        if 'Men' in x:
            gender = 'Men'
            x.remove('Men')
        elif 'Women' in x:
            gender = 'Women'
            x.remove('Women')
        if 'Mercari' in x: 
            x.remove('Mercari')
        if brand in x: 
            x.remove(brand)
        if len(x) == 0:
            return gender, '', '', '', ''
        if len(x) == 1:
            return gender, ' | '.join(x), x[0], '', ''
        if len(x) == 2:
            return gender, ' | '.join(x), x[0], x[1], ''
        if len(x) >= 3:
            return gender, ' | '.join(x), x[0], x[1], x[2]

    def clean_details(self, x):
        result = ['']*20
        for i, tag in enumerate(['Shipping', 'Condition', 'Posted', 'Description', 'Brand', 'Size', 'Tags', 'Material', 'End Use', 
                                'Style', 'Heel Height', 'Dress Style', 'Dress Occasion', 'Heel Type', 'Model', 
                                'Color', 'Shaft Height', 'Product Line', 'Product Type', 'Metal Type']):
            if tag in x:
                result[i] = x[tag]
        return result

    def clean_shipping(self, x):
        x = x.split('|')
        x = [i.strip() for i in x]
        shipping_price = ''
        shipping_currency = ''
        shipping_time = ''
        shipping_from = ''
        if len(x) == 3:
            shipping_price, shipping_currency = self.clean_price(x[0])
            shipping_time = x[1].strip()
            shipping_from = x[2].replace('from','').strip()
        else:
            for item in x:
                if 'from' in item:
                    shipping_from = item.replace('from','').strip()
                if '$' in item or 'Free' in item:
                    shipping_price, shipping_currency = self.clean_price(item)
                if 'days' in item:
                    shipping_time = item.strip()
        return shipping_price, shipping_currency, shipping_time, shipping_from

    def clean_date(self, x):
        try:
            return datetime.strptime(x, "%m/%d/%y").date()
        except:
            return ''

    def clean_url(self, x):
        x = x.split('?')[0]
        if x[-1] != '/':
            x = x + '/'
        return x    

    def clean_listings(self, x):
        return str(x).replace('items listed', '').strip()

    def clean_sales(self, x):
        return str(x).replace('sales', '').replace('Description','').strip()

    def clean_ratings(self, x):
        return str(x).replace('reviews', '').strip()

    def clean_badges(self, x):
        return re.sub('Member Since \d{4} \|', '', x).strip()

    def clean_rating(self, x):
        return int(''.join(filter(str.isdigit, x)))

    def clean_products(self):
        self.df_pr_new[['URL', 'Title', 'Brand']] = self.df_pr[['URL', 'Title', 'Brand']]
        self.df_pr_new['Vendor URL'] = self.df_pr.apply(lambda x: self.clean_url(x['Seller: URL']),axis=1)
        self.df_pr_new[['Price', 'Currency']] = self.df_pr.apply(lambda x: self.clean_price(x['Price']) if pd.notnull(x['Price']) else ('',''), axis=1, result_type="expand")
        self.df_pr_new[['Gender', 'Category', 'Category 1', 'Category 2', 'Category 3']] = self.df_pr.apply(lambda x: self.clean_categories(x['Categories'], self.brand) if pd.notnull(x['Categories']) else ('','','','',''), axis=1, result_type="expand")
        self.df_pr_new[['Shipping', 'Condition', 'Posted', 'Description', 'Brand', 'Size', 'Tags', 'Material', 'End Use', 
                             'Style', 'Heel Height', 'Dress Style', 'Dress Occasion', 'Heel Type', 'Model', 
                             'Color', 'Shaft Height', 'Product Line', 'Product Type', 'Metal Type']] = self.df_pr.apply(lambda x: self.clean_details(literal_eval(x['Details'])) if pd.notnull(x['Details']) else ('','','','','','','','','','','','','','','','','','','',''), axis=1, result_type="expand")
        self.df_pr_new[['Shipping Price', 'Shipping Currency', 'Shipping Time', 'Shipping From']] = self.df_pr_new.apply(lambda x: self.clean_shipping(x['Shipping']) if pd.notnull(x['Shipping']) else ('','','',''), axis=1, result_type="expand")
        self.df_pr_new['Date Posted'] = self.df_pr_new.apply(lambda x: self.clean_date(x['Posted']), axis=1)
        self.df_pr_new['Location'] = ''
        self.df_pr_new['URL URL'] = self.df_pr_new['Vendor URL'].str.lower()+' '+self.df_pr_new['URL'].str.lower()
        self.df_pr_new = self.df_pr_new[['URL URL', 'URL', 'Title', 'Description', 'Category', 'Gender', 'Price', 'Currency', 'Condition', 'Location', 
        'Date Posted', 'Vendor URL', 'Shipping Price', 'Shipping Currency',
        'Shipping Time', 'Shipping From', 'Brand',  
        'Size', 'Tags', 'Material', 'End Use', 'Style', 'Heel Height', 'Dress Style', 'Dress Occasion',
        'Heel Type', 'Model', 'Color', 'Shaft Height', 'Product Line',
        'Product Type', 'Metal Type',        'Category 1', 'Category 2' ]]
        self.df_pr_new = self.df_pr_new.drop_duplicates(subset='URL URL')

    def clean_vendors(self):
        self.df_ve_new['URL'] = self.df_ve.apply(lambda x: self.clean_url(x['Seller: URL']), axis=1)
        self.df_ve_new[['Name', 'Reviews URL', 'Rating Count', 'Rating']] = self.df_ve[['Seller: Name', 'Seller: Reviews URL', 'Rating Count', 'Rating']]
        self.df_ve_new['Member Since'] = self.df_ve.apply(lambda x: datetime(int(x['Seller: Member Since']),1,1).date() if pd.notnull(x['Seller: Member Since']) else x['Seller: Member Since'], axis=1)
        self.df_ve_new['Listings'] = self.df_ve.apply(lambda x: self.clean_listings(x['Seller: Amount of Listings']) if pd.notnull(x['Seller: Amount of Listings']) else x['Seller: Amount of Listings'], axis=1)
        self.df_ve_new['Sales'] = self.df_ve.apply(lambda x: self.clean_sales(x['Seller: Amount of Sales']) if pd.notnull(x['Seller: Amount of Sales']) else x['Seller: Amount of Sales'], axis=1)
        self.df_ve_new['Badges'] = self.df_ve.apply(lambda x: self.clean_badges(x['Seller: Badges']) if pd.notnull(x['Seller: Badges']) else x['Seller: Badges'], axis=1)
        self.df_ve_new['Description'] = ''
        self.df_ve_new['Location'] = ''
        self.df_ve_new = self.df_ve_new[['URL', 'Name', 'Description', 'Rating',  'Rating Count',  'Location', 'Member Since',
        'Badges', 'Listings', 'Sales']]
        self.df_ve_new = self.df_ve_new.drop_duplicates(subset='URL')

    def save_products_vendors(self):
        save_file(self.df_pr_new, 'mercari_com', self.brand, self.date, 'products')
        save_file(self.df_ve_new, 'mercari_com', self.brand, self.date, 'vendors')


class PoshmarkComCleaner():
    def __init__(self, brand, date):
        self.brand = brand
        self.date = date
        self.df_pr = get_file('poshmark_com', brand, date, 'products')
        self.df_ve = get_file('poshmark_com', brand, date, 'vendors')
        self.df_pr_new = pd.DataFrame()
        self.df_ve_new = pd.DataFrame()

    def get_url(self, x):
        return 'https://poshmark.com/closet/'+x

    def get_location(self, places):
        location = ''
        for p in places:
            if not pd.isnull(p):
                location += ', '
                location += str(p)
        return location.strip(',').strip()

    def combine_categories(self, x):
        categories = []
        for c in x:
            if not pd.isnull(c):
                categories.append(c)
        if len(categories) == 0:
            return ''
        else:
            return ' | '.join(categories)

    def clean_products(self):
        self.df_pr_new[['URL',  'Title', 'Description', 'Brand', 'Condition',
        'Original Price', 'Price', 'Currency', 'Status', 'Status: Date',
        'Publish Count', 'Inventory Status', 'Inventory Updated',
        'Quantity Available', 'Quantity Reserved', 'Quantity Sold', 'Sizes', 'Shares', 'Comments',
        'Likes', 'Creator: Username', 'Product Canonical Name' ]] = self.df_pr[['URL',  'Title', 'Description', 'Brand', 'Condition',
        'Original Price', 'Price', 'Currency', 'Status', 'Status: Date',
        'Publish Count', 'Inventory Status', 'Inventory Updated',
        'Quantity Available', 'Quantity Reserved', 'Quantity Sold', 'Sizes', 'Shares', 'Comments',
        'Likes', 'Creator: Username', 'Product Canonical Name' ]]

        self.df_pr_new['Vendor URL'] = self.df_pr_new.apply(lambda x: self.get_url(x['Creator: Username']) if pd.notnull(x['Creator: Username']) else x['Creator: Username'], axis=1)
        self.df_pr_new['Location'] = 'USA'
        self.df_pr_new['URL URL'] = self.df_pr_new['Vendor URL'].str.lower()+' '+self.df_pr_new['URL'].str.lower()
        self.df_pr_new[['Category 1', 'Category 2', 'Image Name', 'Image URL', 'Gender']] = self.df_pr[['Category', 'Category Feature','Image: Name', 'Image: URL', 'Department']]
        self.df_pr_new['Category'] = self.df_pr.apply(lambda x: self.combine_categories([x['Category'], x['Category Feature']]), axis=1)
        self.df_pr_new['Date Posted'] = self.df_pr_new.apply(lambda x: datetime.fromisoformat(x['Status: Date']).replace(tzinfo=None), axis=1)
        self.df_pr_new = self.df_pr_new[['URL URL', 'URL', 'Title', 'Description', 'Category', 'Gender', 'Price', 'Currency', 'Condition', 'Location', 'Date Posted', 'Vendor URL', 
        'Brand',  'Original Price', 'Status', 'Product Canonical Name',  'Sizes', 
        'Publish Count', 'Inventory Status', 'Inventory Updated', 'Quantity Available',
        'Quantity Reserved', 'Quantity Sold', 'Shares', 'Comments', 'Likes', 'Image Name', 'Image URL', 'Category 1', 'Category 2']]
        self.df_pr_new = self.df_pr_new.drop_duplicates(subset='URL URL')

    def clean_vendors(self):
        self.df_ve_new[[ 'Display Handle', 'Full Name', 'First Name', 'Last Name',
        'Description', 'Email', 'Website', 'Home Domain', 'Gender', 'State',
        'Zip', 'City', 'College Name',
        'Retailer State', 'Shoe Size', 'Dress Size', 'Status',
        'Status Changed At', 'Created At', 'Last Active At', 'Last Active Date',
        'User Score', 'FB User ID', 'GP User ID', 'AP User ID', 'FB Info',
        'TM Info', 'PN Info', 'PN V2 Info', 'GP Info', 'AP Info', 'IG Info',
        'YT Info', 'Orders Shipped', 'Order Shipping Time',
        'Brands Following Count', 'Comments', 'Total Comments',
        'Total Hidden Comments', 'Notifications', 'Following',
        'Following Actions', 'Followers', 'Shares', 'Own Shares', 'Posts',
        'Retail Posts', 'Resale Posts', 'Wholesale Posts', 'Total Posts',
        'Size Set Count', 'Discovered Users', 'Referrals',
        'Seller 5 Star Rating Comment Count']] = self.df_ve[[ 'Display Handle', 'Full Name', 'First Name', 'Last Name',
        'Description', 'Email', 'Website', 'Home Domain', 'Gender', 'State',
        'Zip', 'City', 'College Name',
        'Retailer State', 'Shoe Size', 'Dress Size', 'Status',
        'Status Changed At', 'Created At', 'Last Active At', 'Last Active Date',
        'User Score', 'FB User ID', 'GP User ID', 'AP User ID', 'FB Info',
        'TM Info', 'PN Info', 'PN V2 Info', 'GP Info', 'AP Info', 'IG Info',
        'YT Info', 'Orders Shipped', 'Order Shipping Time',
        'Brands Following Count', 'Comments', 'Total Comments',
        'Total Hidden Comments', 'Notifications', 'Following',
        'Following Actions', 'Followers', 'Shares', 'Own Shares', 'Posts',
        'Retail Posts', 'Resale Posts', 'Wholesale Posts', 'Total Posts',
        'Size Set Count', 'Discovered Users', 'Referrals',
        'Seller 5 Star Rating Comment Count']]
        self.df_ve_new['Name'] = self.df_ve['Username']
        self.df_ve_new['URL'] = self.df_ve_new.apply(lambda x: self.get_url(x['Name']) if pd.notnull(x['Name']) else x['Name'], axis=1)
        self.df_ve_new['Member Since'] = self.df_ve_new.apply(lambda x: datetime.fromisoformat(x['Created At']).replace(tzinfo=None).date() if pd.notnull(x['Created At']) else x['Created At'], axis=1)
        self.df_ve_new['Rating'] = ''
        self.df_ve_new['Rating Count'] = ''
        self.df_ve_new['Location'] = self.df_ve_new.apply(lambda x: self.get_location([x['Zip'], x['City'], x['State'], 'USA']), axis=1)
        self.df_ve_new = self.df_ve_new[['URL', 'Name', 'Description', 'Rating', 'Rating Count', 'Location', 'Member Since', 
        'Display Handle', 'Full Name', 'First Name', 'Last Name',
        'Email', 'Website', 'Home Domain', 'Gender', 'State',
        'Zip',  'City', 'College Name',
        'Retailer State', 'Shoe Size', 'Dress Size', 'Status',
        'Status Changed At', 'Last Active At', 'Last Active Date',
        'User Score', 'FB User ID', 'GP User ID', 'AP User ID', 'FB Info',
        'TM Info', 'PN Info', 'PN V2 Info', 'GP Info', 'AP Info', 'IG Info',
        'YT Info', 'Orders Shipped', 'Order Shipping Time',
        'Brands Following Count', 'Comments', 'Total Comments',
        'Total Hidden Comments', 'Notifications', 'Following',
        'Following Actions', 'Followers', 'Shares', 'Own Shares', 'Posts',
        'Retail Posts', 'Resale Posts', 'Wholesale Posts', 'Total Posts',
        'Size Set Count', 'Discovered Users', 'Referrals',
        'Seller 5 Star Rating Comment Count']]

    def save_products_vendors(self):
        save_file(self.df_pr_new, 'poshmark_com', self.brand, self.date, 'products')
        save_file(self.df_ve_new, 'poshmark_com', self.brand, self.date, 'vendors')
    

class RakutenFrCleaner():
    def __init__(self, brand, date):
        self.brand = brand
        self.date = date
        self.df_prve = get_file('rakuten_fr', brand, date, 'products_vendors')
        self.df_pr = get_file('rakuten_fr', brand, date, 'products')
        self.df_ve = get_file('rakuten_fr', brand, date, 'vendors')
        self.df_prve_new = pd.DataFrame()
        self.df_pr_new = pd.DataFrame()
        self.df_ve_new = pd.DataFrame()

    def clean_url(self, x):
        return '/'.join(x.split('/')[:-1])

    def get_vendor_url(self, x):
        return 'https://fr.shopping.rakuten.com/boutique/' + x

    def clean_condition(self, x):
        return x.replace('Produit','').strip()

    def clean_category(self, x):
        for b in self.brand.split('_'):
            pattern = re.compile(b, re.IGNORECASE)
            x = pattern.sub('', x)
        x_list = x.split(' > ')
        categories = []
        gender = ''
        for c in x_list:
            if 'homme' in c:
                gender = 'Homme'
                pattern = re.compile('homme', re.IGNORECASE)
                c = pattern.sub('', c)
            if 'femme' in c:
                gender = 'Femme'
                pattern = re.compile('femme', re.IGNORECASE)
                c = pattern.sub('', c)
            c = c.title()
            if c != '' and c not in categories:
                categories.append(c)
        if len(categories) == 0:
            return '', '', '', '', gender
        if len(categories) == 1:
            return ' | '.join(categories), categories[0], '', '', gender
        if len(categories) == 2:
            return ' | '.join(categories), categories[0], categories[1], '', gender
        if len(categories) >=3:
            return ' | '.join(categories), categories[0], categories[1], categories[2], gender

    def clean_rating(self, x):
        try:
            x = x.replace('/5','').replace(',','.').replace('Pas de note','').strip()
            return float(x)/5
        except:
            return ''

    def clean_sales(self, x):
        return int(''.join(filter(str.isdigit, x)))

    def clean_phonenumber(self, x):
        return x.replace('Téléphone :','').strip()

    def clean_capital(self, x):
        return x.replace('€','').strip()

    def clean_acceptance_rate(self, x):
        try:
            rate = x.replace('%','').strip()
            rate = float(rate)/100
        except:
            rate = ''
        return rate

    def get_location(self, places):
        location = ''
        for p in places:
            if not pd.isnull(p):
                location += ', '
                location += str(p)
        return location.strip(',').strip()

    def clean_date(self, x):
        try:
            return datetime.strptime(x, "%d/%m/%Y")
        except:
            return ''

    def clean_products_vendors(self):
        self.df_prve_new[['Price']] = self.df_prve[['Price']]
        self.df_prve_new['URL'] = self.df_prve.apply(lambda x: self.clean_url(x['URL']), axis=1)
        self.df_prve_new['Vendor URL'] = self.df_prve.apply(lambda x: self.get_vendor_url(x['Vendor Name']) if pd.notnull(x['Vendor Name']) else x['Vendor Name'], axis = 1)
        self.df_prve_new['Condition'] = self.df_prve.apply(lambda x: self.clean_condition(x['Condition']) if pd.notnull(x['Condition']) else x['Condition'], axis = 1)
    
    def clean_products(self):
        self.df_pr_new[['Title', 'Brand', 'Rating', 'Image URL']] = self.df_pr[['Title', 'Brand', 'Rating', 'Image URL']]
        self.df_pr_new['URL'] = self.df_pr.apply(lambda x: self.clean_url(x['URL']), axis=1)
        self.df_pr_new['Rating Count'] = self.df_pr['Amount of Ratings']
        self.df_pr_new['Rating'] = self.df_pr_new.apply(lambda x: self.clean_rating(x['Rating']) if x['Rating Count'] > 0 else '', axis=1)
        self.df_pr_new[['Category', 'Category 1', 'Category 2', 'Category 3', 'Gender']] = self.df_pr.apply(lambda x: self.clean_category(x['Category Raw']) if pd.notnull(x['Category Raw']) else ('','','','',''), axis = 1, result_type="expand")

    def combine_products_vendors(self):
        self.df_prve_merged = self.df_prve_new.merge(self.df_pr_new, how='left', left_on='URL', right_on='URL')
        self.df_prve_merged['URL URL'] = self.df_prve_merged['Vendor URL'].str.lower()+' '+self.df_prve_merged['URL'].str.lower()
        self.df_prve_merged['Currency'] = 'EUR'
        self.df_prve_merged['Description'] = ''
        self.df_prve_merged['Location'] = ''
        self.df_prve_merged['Date Posted'] = ''
        self.df_prve_merged = self.df_prve_merged[['URL URL', 'URL', 'Title', 'Description', 'Category', 'Gender', 'Price', 'Currency', 'Condition', 'Location',
                'Date Posted', 'Vendor URL', 'Rating', 'Rating Count', 'Category 1', 'Category 2', 'Category 3']]

    def clean_vendors(self):
        self.df_ve_new[['Origin Country', 'Legal Name','Address', 'Zipcode', 'Country', 'SIREN']] = self.df_ve[['Origin Country', 'Legal Name','Address', 'Zipcode', 'Country', 'SIREN']]
        self.df_ve_new['Member Since'] = self.df_ve.apply(lambda x: self.clean_date(x['Registration Date']), axis=1)
        self.df_ve_new['Latest Online Date'] = self.df_ve.apply(lambda x: self.clean_date(x['Last Time Online']), axis=1)
        self.df_ve_new[['Name', 'VAT', 'Rating Count', 'Reply Time', 'Shipping Type', 'Type']] = self.df_ve[['Vendor Name', 'TVA', 'Total Reviews', 'Response Time', 'Shipment', 'Form']]
        self.df_ve_new['Location'] = self.df_ve.apply(lambda x: self.get_location([x['Address'], x['Zipcode'], x['Country']]), axis=1)
        self.df_ve_new['URL'] = self.df_ve.apply(lambda x: self.get_vendor_url(x['Vendor Name']) if pd.notnull(x['Vendor Name']) else x['Vendor Name'], axis = 1)
        self.df_ve_new['Acceptance Rate'] = self.df_ve.apply(lambda x: self.clean_acceptance_rate(x['Acceptance Rate']) if pd.notnull(x['Acceptance Rate']) else x['Acceptance Rate'], axis = 1)
        self.df_ve_new['Rating'] = self.df_ve.apply(lambda x: self.clean_rating(x['Rating']) if pd.notnull(x['Rating']) else x['Rating'], axis = 1)
        self.df_ve_new['Sales'] = self.df_ve.apply(lambda x: self.clean_sales(x['Amount of Sales']) if pd.notnull(x['Amount of Sales']) else x['Amount of Sales'], axis = 1)
        self.df_ve_new['Phonenumber'] = self.df_ve.apply(lambda x: self.clean_phonenumber(x['Phonenumber']) if pd.notnull(x['Phonenumber']) else x['Phonenumber'], axis = 1)
        self.df_ve_new['Capital'] = self.df_ve.apply(lambda x: self.clean_capital(x['Capital Social']) if pd.notnull(x['Capital Social']) else x['Capital Social'], axis = 1)
        self.df_ve_new['Description'] = ''
        self.df_ve_new = self.df_ve_new[self.df_ve_new['Name'].notna()]
        self.df_ve_new = self.df_ve_new[['URL', 'Description', 'Name', 'Rating', 'Rating Count', 'Location', 'Member Since', 'VAT', 'Legal Name', 
           'Reply Time', 'Type', 'SIREN', 'Capital', 'Phonenumber',  'Sales',  'Shipping Type',  'Origin Country',
           'Latest Online Date', 'Acceptance Rate']]

    def save_products_vendors(self):
        save_file(self.df_prve_merged, 'rakuten_fr', self.brand, self.date, 'products')
        save_file(self.df_ve_new, 'rakuten_fr', self.brand, self.date, 'vendors')


class VintedV1FrCleaner():
    def __init__(self, brand, date):
        self.brand = brand
        self.date = date
        self.df_pr = get_file('vinted_v1_fr', brand, date, 'products')
        self.df_ve = get_file('vinted_v1_fr', brand, date, 'vendors')
        self.df_pr_new = pd.DataFrame()
        self.df_ve_new = pd.DataFrame()

    def clean_payment_methods(self, x):
        carte_bancaire = False
        paypal = False
        sofort = False
        apple_pay = False
        ideal = False  
        credit_card = False
        if 'Carte bancaire' in x:
            carte_bancaire = True
        if 'PayPal' in x:
            paypal = True
        if 'Sofort' in x:
            sofort = True
        if 'Apple Pay' in x:
            apple_pay = True
        if 'iDeal' in x:
            ideal = True
        if 'Credit card' in x:
            credit_card = True
        return carte_bancaire, sofort, apple_pay, paypal, ideal, credit_card

    def clean_images(self, x):
        x_list = x.split('|')
        if len(x_list) > 0:
            return x_list[0]
        else:
            return ''

    def get_id(self, url):
        return (url.split('/'))[-1].split('-')[0]

    def clean_categories(self, x):
        gender = ''
        categories = x.split('/')
        for cat in categories:
            if cat == '':
                categories.remove(cat)
            for gen in ['women', 'men', 'kids', 'boys', 'girls', 'femmes', 'hommes', 'enfants', 'garcons', 'filles']:
                if gen in categories:
                    gender = gen.title()
                    categories.remove(gen)
        for id_, cat in enumerate(categories):
            categories[id_] = cat.replace('-', ' ').title()
        categories = categories[:-1]
        if len(categories) == 0:
            return gender, '', '', '', ''
        if len(categories) == 1:
            return gender, ' | '.join(categories), categories[0], '', ''
        if len(categories) == 2:
            return gender, ' | '.join(categories), categories[0], categories[1], ''
        if len(categories) >= 3:
            return gender, ' | '.join(categories), categories[0], categories[1], categories[2]      

    def get_location(self, places):
        location = ''
        for p in places:
            if not pd.isnull(p):
                location += ', '
                location += str(p)
        return location.strip(',').strip()

    def get_rating_count(self, x):
        total = 0
        for amount in x:
            try:
                total = total + int(amount)
            except:
                continue
        return total

    def clean_products(self):
        self.df_pr_new[['URL', 'Title', 'Description', 'Price', 'Currency', 'Condition', 'Brand', 'Size', 'Color', 
                   'City', 'Country', 'Active Bid Count', 'Favourite Count', 'View Count',
                   'Is for Sell','Is for Swap', 'Is for Give Away', 'Is Handicraft', 'Is Unisex',
                   'Updated At', 'Last Push Up At', 'User Updated At', 'Promoted Until', 'Vendor: Username', 'Date Posted', 'Vendor URL']] = self.df_pr[['URL', 'Title', 'Description', 'Price', 'Currency', 'Condition', 'Brand', 'Size', 'Color', 
                   'City', 'Country', 'Active Bid Count', 'Favourite Count', 'View Count',
                   'Is for Sell','Is for Swap', 'Is for Give Away', 'Is Handicraft', 'Is Unisex',
                   'Updated At', 'Last Push Up At', 'User Updated At', 'Promoted Until', 'Vendor: Username', 'Created At', 'Vendor URL']]
        self.df_pr_new[['Bank Card', 'Sofort', 'Apple Pay', 'PayPal', 'iDeal', 'Credit Card']] = self.df_pr.apply(lambda x: self.clean_payment_methods(x['Payment Methods']) if pd.notnull(x['Payment Methods']) else (False, False, False, False, False, False), axis=1, result_type = 'expand')
        self.df_pr_new['Image'] = self.df_pr.apply(lambda x: self.clean_images(x['Images']) if pd.notnull(x['Images']) else x['Images'], axis=1)
        self.df_pr_new[['Gender', 'Category', 'Category 1', 'Category 2', 'Category 3']] = self.df_pr.apply(lambda x: self.clean_categories(x['Categories']), axis=1, result_type="expand")
        self.df_pr_new = self.df_pr_new.drop_duplicates(subset='URL', keep="last")
        # df_ve_url = self.df_ve[['Username', 'URL']].rename({'URL': 'Vendor URL', 'Username': 'Vendor: Username'}, axis=1).drop_duplicates(subset='Vendor: Username')
        # self.df_pr_new = self.df_pr_new.merge(df_ve_url, how='left')
        self.df_pr_new['ID'] = self.df_pr_new.apply(lambda x: self.get_id(x['URL']), axis=1)
        self.df_pr_new['URL URL'] = self.df_pr_new['Vendor URL'].str.lower()+' '+self.df_pr_new['ID'].str.lower()
        self.df_pr_new['Location'] = self.df_pr_new.apply(lambda x: self.get_location([x['City'], x['Country']]), axis=1)
        self.df_pr_new = self.df_pr_new[['URL URL', 'URL', 'Title', 'Description', 'Category', 'Gender', 'Price', 'Currency', 'Condition', 
                       'Location', 'Date Posted', 'Vendor URL', 'Brand', 'Color', 'Size', 'Active Bid Count', 
                       'Favourite Count', 'View Count',  'Is for Swap', 'Bank Card', 'PayPal', 'Image', 'Category 1', 'Category 2', 'Category 3']]

    def clean_vendors(self):
        self.df_ve_new[['URL', 'Gender',  'Followers', 'Following', 'Rating', 
        'Last Login At', 'Postal Code', 'City', 'Country', 'Country ISO', 
       'Has Promoted Closet', 'Promoted Until',
       'Verified by Email', 'Verified by Facebook', 'Verified by Facebook At', 
       'Verified by Google', 'Verified by Google At', 'Verified by Phone', 'Verified by Phone At', 'Name', 'Description', 'Listings', 'Sales', 'Total Listings', 'Items Bought',  'Positive Rating Count', 'Neutral Rating Count',
           'Negative Rating Count', 'Meeting Transactions','Facebook Friends']] = self.df_ve[['URL', 'Gender',  'Followers', 'Following', 'Rating', 
        'Last Login At', 'Postal Code', 'City', 'Country', 'Country ISO', 
       'Has Promoted Closet', 'Promoted Until',
       'Verified by Email', 'Verified by Facebook', 'Verified by Facebook At', 
       'Verified by Google', 'Verified by Google At', 'Verified by Phone', 'Verified by Phone At', 'Username', 'About', 
                                                                                         'Amount of Listings',
           'Amount of Items Sold', 'Total Amount of Listings', 'Amount of Items Bought', 'Amount of Positive Ratings', 
           'Amount of Neutral Ratings','Amount of Negative Ratings', 'Amount of Meeting Transactions',
                                                                                         'Amount of Facebook Friends']]
        self.df_ve_new['Member Since'] = self.df_ve.apply(lambda x: pd.to_datetime(x['Created At']).date(), axis=1)
        self.df_ve_new['Location'] = self.df_ve.apply(lambda x: self.get_location([x['City'], x['Country']]), axis=1)
        self.df_ve_new['Rating Count'] = self.df_ve_new.apply(lambda x: self.get_rating_count([x['Positive Rating Count'], x['Neutral Rating Count'],x['Negative Rating Count']]), axis=1)
        self.df_ve_new[['Bank Card', 'Sofort', 'Apple Pay', 'PayPal', 'iDeal', 'Credit Card']] = self.df_ve.apply(lambda x: self.clean_payment_methods(x['Payment Methods']) if pd.notnull(x['Payment Methods']) else (False, False, False, False, False, False), axis=1, result_type = 'expand')
        self.df_ve_new = self.df_ve_new.drop_duplicates(subset='URL', keep="last")
        self.df_ve_new = self.df_ve_new[['URL', 'Name', 'Description', 'Rating', 'Rating Count', 'Location', 'Member Since', 'Gender', 'Listings', 'Sales', 
           'Total Listings', 'Items Bought', 'Meeting Transactions', 'Followers', 'Following', 'Facebook Friends',
           'Positive Rating Count', 'Neutral Rating Count', 'Negative Rating Count', 
           'Last Login At', 'Has Promoted Closet', 'Promoted Until', 
           'Verified by Email', 'Verified by Facebook', 'Verified by Facebook At', 'Verified by Google', 
           'Verified by Google At', 'Verified by Phone', 'Verified by Phone At',
           'Bank Card', 'Sofort', 'Apple Pay', 'PayPal', 'iDeal', 'Credit Card']]
        
    def save_products_vendors(self):
        save_file(self.df_pr_new, 'vinted_v1_fr', self.brand, self.date, 'products')
        save_file(self.df_ve_new, 'vinted_v1_fr', self.brand, self.date, 'vendors')

class VintedV2FrCleaner():
    def __init__(self, brand, date):
        self.brand = brand
        self.date = date
        self.df_pr = get_file('vinted_v2_fr', brand, date, 'products')
        self.df_ve = get_file('vinted_v2_fr', brand, date, 'vendors')
        self.df_pr_new = pd.DataFrame()
        self.df_ve_new = pd.DataFrame()

    def clean_image(self, x):
        return x.split('?')[0]

    def get_id(self, url):
        return (url.split('/'))[-1].split('-')[0]

    def clean_categories(self, x):
        x = x.replace('https://www.vinted.fr', '')
        gender = ''
        categories = x.split('/')
        for cat in categories:
            if cat == '':
                categories.remove(cat)
            for gen in ['women', 'men', 'kids', 'boys', 'girls', 'femmes', 'hommes', 'enfants', 'garcons', 'filles']:
                if gen in categories:
                    gender = gen.title()
                    categories.remove(gen)
        for id_, cat in enumerate(categories):
            categories[id_] = cat.replace('-', ' ').title()
        categories = categories[:-1]
        if len(categories) == 0:
            return gender, '', '', '', ''
        if len(categories) == 1:
            return gender, ' | '.join(categories), categories[0], '', ''
        if len(categories) == 2:
            return gender, ' | '.join(categories), categories[0], categories[1], ''
        if len(categories) >= 3:
            return gender, ' | '.join(categories), categories[0], categories[1], categories[2]      

    def clean_products(self):
        self.df_pr_new[['URL', 'Title', 'Price', 'Currency','Brand', 'Favourite Count', 'Is for Swap',  
                    'Vendor URL']] = self.df_pr[['URL', 'Title', 'Price', 'Currency', 'Brand', 'Favourite Count', 'For Swap',
                    'Vendor URL']]
        self.df_pr_new['Image'] = self.df_pr.apply(lambda x: self.clean_image(x['Image URL']) if pd.notnull(x['Image URL']) else x['Image URL'], axis=1)
        self.df_pr_new[['Gender', 'Category', 'Category 1', 'Category 2', 'Category 3']] = self.df_pr.apply(lambda x: self.clean_categories(x['URL']), axis=1, result_type="expand")
        self.df_pr_new = self.df_pr_new.drop_duplicates(subset='URL', keep="last")
        # df_ve_url = self.df_ve[['Username', 'URL']].rename({'URL': 'Vendor URL', 'Username': 'Vendor: Username'}, axis=1).drop_duplicates(subset='Vendor: Username')
        # self.df_pr_new = self.df_pr_new.merge(df_ve_url, how='left')
        self.df_pr_new['ID'] = self.df_pr_new.apply(lambda x: self.get_id(x['URL']), axis=1)
        self.df_pr_new['URL URL'] = self.df_pr_new['Vendor URL'].str.lower()+' '+self.df_pr_new['ID'].str.lower()
        self.df_pr_new = self.df_pr_new[['URL URL', 'URL', 'Title',  'Category', 'Gender', 'Price', 'Currency', 
                      'Vendor URL', 'Brand', 'Favourite Count', 'Is for Swap', 'Image', 'Category 1', 'Category 2', 'Category 3']]

    def clean_vendors(self):
        self.df_ve_new[['URL', 'Name']] = self.df_ve[['URL', 'Username']]
        self.df_ve_new = self.df_ve_new.drop_duplicates(subset='URL', keep="last")
        self.df_ve_new = self.df_ve_new[['URL', 'Name']]
        
    def save_products_vendors(self):
        save_file(self.df_pr_new, 'vinted_v2_fr', self.brand, self.date, 'products')
        save_file(self.df_ve_new, 'vinted_v2_fr', self.brand, self.date, 'vendors')


class VintedV3FrCleaner():
    def __init__(self, brand, date):
        self.brand = brand
        self.date = date
        self.df_se = get_file('vinted_v3_fr', brand, date, '')
        self.df_pr = get_file('vinted_v3_fr', brand, date, 'products')
        self.df_ve = get_file('vinted_v3_fr', brand, date, 'vendors')
        self.df_se_new = pd.DataFrame()
        self.df_pr_new = pd.DataFrame()
        self.df_ve_new = pd.DataFrame()

    def clean_payment_methods(self, x):
        carte_bancaire = False
        paypal = False
        sofort = False
        apple_pay = False
        ideal = False  
        credit_card = False
        if 'Carte bancaire' in x:
            carte_bancaire = True
        if 'PayPal' in x:
            paypal = True
        if 'Sofort' in x:
            sofort = True
        if 'Apple Pay' in x:
            apple_pay = True
        if 'iDeal' in x:
            ideal = True
        if 'Credit card' in x:
            credit_card = True
        return carte_bancaire, sofort, apple_pay, paypal, ideal, credit_card

    def clean_images(self, x):
        x_list = x.split('|')
        if len(x_list) > 0:
            return x_list[0]
        else:
            return ''

    def clean_image(self, x):
        return x.split('?')[0]

    def get_id(self, url):
        return (url.split('/'))[-1].split('-')[0]

    def clean_categories(self, x):
        x = x.replace('https://www.vinted.fr/', '')
        gender = ''
        x_list = x.split('/')
        if '' in x_list:
            x_list.remove('')
       
        if 'beaute' in x_list:
            x_list.remove('beaute')
        for f in ['femmes', 'women', 'dames', 'damen', 'mujer', 'donna', 'mulher']:
            if f in x_list:
                gender = 'Women'
                x_list.remove(f)
        for h in ['hommes', 'men', 'heren', 'herren', 'hombre', 'uomo', 'homem']:
            if h in x_list:
                gender = 'Men'
                x_list.remove(h)
        for e in ['enfants', 'kids', 'kinderen', 'kinder', 'ninos', 'ninas', 'filles', 'garcons', 'bambini', 
                'jongenskleding', 'meisjeskleding', 'bebe-garcon', 'bebe-filles', 'boys-clothing', 'girls-clothing', 'jungs', 
                'abbigliamento-bambino', 'abbigliamento-bambina', 'Ninos', 'babykleding']:
            if e in x_list:
                gender = 'Kids'
                x_list.remove(e)
        x_list = [w.replace('vestiti', 'vetements').replace('kleding', 'vetements').replace('ropa', 'vetements').replace('cappotti-e-giacche', 'manteaux-et-vestes') for w in x_list]
                
        try:
            category = x_list[0].replace('-', ' ').title().replace(' En ', ' en ').replace(' And ', ' and ').replace('T Shirts','T-Shirts')
            categories = [category]
        except:
            categories = []
        if len(categories) == 0:
            return gender, '', '', '', ''
        if len(categories) == 1:
            return gender, ' | '.join(categories), categories[0], '', ''
        if len(categories) == 2:
            return gender, ' | '.join(categories), categories[0], categories[1], ''
        if len(categories) >= 3:
            return gender, ' | '.join(categories), categories[0], categories[1], categories[2]   

    def clean_details(self, x):
        result = ['']*9
        for i, tag in enumerate(['Marque', 'Taille', 'État', 'Couleur', 'Emplacement', 'Modes de paiement', 'Nombre de vues', 
                                'Intéressés·ées', 'Ajouté']):
            if tag in x:
                result[i] = x[tag]
        return result

    def clean_location(self, x):
        location = x.split(',')
        location = [l.strip() for l in location]
        if len(location) == 1:
            return '', location[0]
        elif len(location) == 2:
            return location[0], location[1]
        else:
            return '', ''

    def get_location(self, places):
        location = ''
        for p in places:
            if not pd.isnull(p):
                location += ', '
                location += str(p)
        return location.strip(',').strip()

    def clean_bid_count(self, x):
        return x.replace('utilisateurs','').replace('utilisateur','').strip()

    def get_rating_count(self, x):
        total = 0
        for amount in x:
            try:
                total = total + int(amount)
            except:
                continue
        return total

    def clean_search_results(self):
        self.df_se_new[['URL', 'Vendor URL']] = self.df_se[['URL', 'Vendor URL']]
        self.df_se_new['ID'] = self.df_se_new.apply(lambda x: self.get_id(x['URL']), axis=1)
        self.df_se_new['URL URL'] = self.df_se_new['Vendor URL'].str.lower()+' '+self.df_se_new['ID'].str.lower()
        self.df_se_new = self.df_se_new[['URL URL']]

    def clean_products(self):
        self.df_pr[['Brand', 'Size', 'Condition', 'Color', 'Location', 'Payment Methods', 'View Count', 'Active Bid Count', 'Created At']] = self.df_pr.apply(lambda x: self.clean_details(json.loads(x['Details'])),axis=1, result_type="expand" )
        self.df_pr['Active Bid Count'] = self.df_pr.apply(lambda x: self.clean_bid_count(x['Active Bid Count']), axis=1)
        self.df_pr[['City', 'Country']] = self.df_pr.apply(lambda x: self.clean_location(x['Location']),axis=1, result_type="expand")
        self.df_pr_new[['URL', 'Title', 'Description', 'Price', 'Currency', 'Condition', 'Brand', 'Size', 'Color', 
                   'City', 'Country', 'Active Bid Count', 'Favourite Count', 'View Count',
                   'Is for Swap', 'Date Posted', 'Vendor URL', 'Location']] = self.df_pr[['URL', 'Title', 'Description', 'Price', 'Currency', 'Condition', 'Brand', 'Size', 'Color', 
                   'City', 'Country', 'Active Bid Count', 'Favourite Count', 'View Count', 'For Swap', 'Created At', 'Vendor URL', 'Location']]
        self.df_pr_new[['Bank Card', 'Sofort', 'Apple Pay', 'PayPal', 'iDeal', 'Credit Card']] = self.df_pr.apply(lambda x: self.clean_payment_methods(x['Payment Methods']) if pd.notnull(x['Payment Methods']) else (False, False, False, False, False, False), axis=1, result_type = 'expand')
        self.df_pr_new['Image'] = self.df_pr.apply(lambda x: self.clean_image(x['Image URL']) if pd.notnull(x['Image URL']) else x['Image URL'], axis=1)
        self.df_pr_new[['Gender', 'Category', 'Category 1', 'Category 2', 'Category 3']] = self.df_pr.apply(lambda x: self.clean_categories(x['URL']), axis=1, result_type="expand")
        self.df_pr_new = self.df_pr_new.drop_duplicates(subset='URL', keep="last")
        self.df_pr_new['ID'] = self.df_pr_new.apply(lambda x: self.get_id(x['URL']), axis=1)
        self.df_pr_new['URL URL'] = self.df_pr_new['Vendor URL'].str.lower()+' '+self.df_pr_new['ID'].str.lower()
        self.df_pr_new = self.df_pr_new[['URL URL', 'URL', 'Title', 'Description', 'Category', 'Gender', 'Price', 'Currency', 'Condition', 
                       'Location', 'Date Posted', 'Vendor URL', 'Brand', 'Color', 'Size', 'Active Bid Count', 
                       'Favourite Count', 'View Count',  'Is for Swap', 'Bank Card', 'PayPal', 'Image', 'Category 1', 'Category 2', 'Category 3']]

    def clean_vendors(self):
        self.df_ve_new[['URL', 'Gender',  'Followers', 'Following', 'Rating', 
        'Last Login At', 'Postal Code', 'City', 'Country', 'Country ISO', 
       'Has Promoted Closet', 'Promoted Until',
       'Verified by Email', 'Verified by Facebook', 'Verified by Facebook At', 
       'Verified by Google', 'Verified by Google At', 'Verified by Phone', 'Verified by Phone At', 'Name', 'Description', 'Listings', 'Sales', 'Total Listings', 'Items Bought',  'Positive Rating Count', 'Neutral Rating Count',
           'Negative Rating Count', 'Meeting Transactions','Facebook Friends']] = self.df_ve[['URL', 'Gender',  'Followers', 'Following', 'Rating', 
        'Last Login At', 'Postal Code', 'City', 'Country', 'Country ISO', 
       'Has Promoted Closet', 'Promoted Until',
       'Verified by Email', 'Verified by Facebook', 'Verified by Facebook At', 
       'Verified by Google', 'Verified by Google At', 'Verified by Phone', 'Verified by Phone At', 'Username', 'About', 
                                                                                         'Amount of Listings',
           'Amount of Items Sold', 'Total Amount of Listings', 'Amount of Items Bought', 'Amount of Positive Ratings', 
           'Amount of Neutral Ratings','Amount of Negative Ratings', 'Amount of Meeting Transactions',
                                                                                         'Amount of Facebook Friends']]
        self.df_ve_new['Member Since'] = self.df_ve.apply(lambda x: pd.to_datetime(x['Created At']).date(), axis=1)
        self.df_ve_new['Location'] = self.df_ve.apply(lambda x: self.get_location([x['City'], x['Country']]), axis=1)
        self.df_ve_new['Rating Count'] = self.df_ve_new.apply(lambda x: self.get_rating_count([x['Positive Rating Count'], x['Neutral Rating Count'],x['Negative Rating Count']]), axis=1)
        self.df_ve_new[['Bank Card', 'Sofort', 'Apple Pay', 'PayPal', 'iDeal', 'Credit Card']] = self.df_ve.apply(lambda x: self.clean_payment_methods(x['Payment Methods']) if pd.notnull(x['Payment Methods']) else (False, False, False, False, False, False), axis=1, result_type = 'expand')
        self.df_ve_new = self.df_ve_new.drop_duplicates(subset='URL', keep="last")
        self.df_ve_new = self.df_ve_new[['URL', 'Name', 'Description', 'Rating', 'Rating Count', 'Location', 'Member Since', 'Gender', 'Listings', 'Sales', 
           'Total Listings', 'Items Bought', 'Meeting Transactions', 'Followers', 'Following', 'Facebook Friends',
           'Positive Rating Count', 'Neutral Rating Count', 'Negative Rating Count', 
           'Last Login At', 'Has Promoted Closet', 'Promoted Until', 
           'Verified by Email', 'Verified by Facebook', 'Verified by Facebook At', 'Verified by Google', 
           'Verified by Google At', 'Verified by Phone', 'Verified by Phone At',
           'Bank Card', 'Sofort', 'Apple Pay', 'PayPal', 'iDeal', 'Credit Card']]
    

    def save_products_vendors(self):
        save_file(self.df_se_new, 'vinted_v3_fr', self.brand, self.date, 'search')
        save_file(self.df_pr_new, 'vinted_v3_fr', self.brand, self.date, 'products')
        save_file(self.df_ve_new, 'vinted_v3_fr', self.brand, self.date, 'vendors')


    