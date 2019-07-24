import os
import re
import csv
import pickle
import collections
import datetime, time
from collections import defaultdict

import requests
import lxml.html, cssselect




YEARS = [2015, 2016, 2017, 2018, 2019]


VENDORS = [
    'ABARTH', 'ALFA ROMEO', 'ALPINE', 'ASTON MARTIN', 'AUDI', 'BENTLEY', 'BMW', 'ALPINA',
    'CATERHAM', 'CITROEN', 'DACIA', 'DS', 'FERRARI', 'FIAT', 'FORD', 'HONDA', 'HYUNDAI',
    'INFINITI', 'JAGUAR', 'JEEP', 'KIA', 'LAMBORGHINI', 'LAND ROVER', 'LEVC', 'LEXUS',
    'LOTUS', 'MASERATI', 'MAZDA', 'MCLAREN', 'MERCEDES-BENZ', 'MG', 'MICROCAR',
    'MINI', 'MITSUBISHI', 'MORGAN', 'NISSAN', 'PEUGEOT', 'PORSCHE', 'RENAULT', 'ROLLS-ROYCE',
    'SEAT', 'SKODA', 'SMART', 'SSANGYONG', 'SUBARU', 'SUZUKI', 'TESLA', 'TOYOTA',
    'VAUXHALL', 'VOLKSWAGEN', 'VOLVO'
    ]


# Up to 99 shall work 
PICS_AMOUNT = 5 


class Engine:
    """ Persistent requests wrapper.
    Handles all errors except system ones,
    until counter reaches MAX_RETRY value.

    All methods:
    -> requests.Response
    """
    MAX_RETRY = 5
    DELAY = 4

    def get(self, *args, **kwargs):
        errors = 0
        while True:
            try:
                time.sleep(self.DELAY)
                response = requests.get(*args, **kwargs, timeout=10)
                response.raise_for_status()
                return response
            
            except Exception as e:
                if errors == self.MAX_RETRY:
                    raise ConnectionError(e)
                
                print(e.__class__.__name__, e)
                errors += 1
                

class FileTools:
    def save_data_csv(self, data_rows, filename, headers, encoding='utf-8-sig'):
        with open(filename, 'w', encoding=encoding) as OUT:
            OUT = csv.writer(OUT, delimiter=',', lineterminator='\n', escapechar='\\')
            OUT.writerow(headers)

            for row in data_rows:
                OUT.writerow(
                    [row.get(h, '-') if str(row.get(h, '')).strip() else '-'
                         for h in headers]
                )
                
    def load_data_csv(self, filename, encoding='utf-8-sig'):
        data = collections.defaultdict(list)
        if os.path.exists(filename):
            with open(filename, encoding=encoding) as IN_CSV:
                csv_data = csv.reader(IN_CSV, delimiter=',')
                headers = next(csv_data)
                data['rows'] = [dict(zip(headers, r)) for r in csv_data]
        return data

    def save_pickle(self, filename, obj):
        with open(filename, 'wb') as OUT_PKL:
            pickle.dump(obj, OUT_PKL)

    def load_pickle(self, filename):
        with open(filename, 'rb') as IN_PKL:
            return pickle.load(IN_PKL)
    

class NCSImgExt(FileTools, Engine):
    HEADERS = {
        'template': {
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-US;q=0.8,en;q=0.7',
            'cache-control': 'no-cache',
            'pragma': 'no-cache',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/75.0.3770.100 Safari/537.36'
            },
        
        'html': {
            'upgrade-insecure-requests': '1',
            'accept': 'text/html,application/xhtml+xml,application/xml'
                      ';q=0.9,image/webp,image/apng,*/*;q=0.8,application/'
                      'signed-exchange;v=b3'
            },
        
        'pics': {
            'accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
            'referer': '',
            }
        }

    USER_INPUT_MSG = {
            'year': 'Please input year/years or empty: '
        }

    URL = {
            'root': 'https://www.netcarshow.com/'
        }

    FILE = {
            'cache': 'cache.pkl',
            'summary': 'summary.csv'
        }

    CSV_HEADERS = ['VENDOR', 'YEAR', 'CAR', 'CAR_LINK']
    
    def __init__(self, vendors, years, pics_amount, verbose=True):
        self.vendors_choice = vendors
        self.years_choice = years
        self.pics_amount_limit = pics_amount
        self.verbose = verbose
        self.timestamp = self.timestamp_today
        self.pics_output_folder = self.folder_always_exists('pics')
        self.caches = self.load_url_data(self.FILE['cache'])
        self.summary_data = self.get_summary_data(self.FILE['summary'])

        [self.HEADERS[h].update(self.HEADERS['template']) for h in ['html', 'pics']]
        self.CSV_HEADERS.extend(
            ['IMAGE_{:0>2}'.format(n) for n in range(1, self.pics_amount_limit + 1)]
        )
        
    @property
    def timestamp_today(self):
        return datetime.datetime.today().strftime('%Y%m%d')

    def folder_always_exists(self, foldername):
        if not os.path.exists(foldername):
            os.mkdir(foldername)
        return foldername

    def input_parse_int(self, inp):
        inp = inp.strip()
        for sep in ', ':
            if sep in inp:
                return [int(i.strip()) for i in inp.split(sep) if i.strip().isdigit()]
        return [int(inp)]

    def print_dict(self, _dict):
        for k, v in _dict.items():
            print('{:>5}: {}'.format(k, v))
        print('-'*10)

    def input_years(self):
        years = range(self.min_year, self.current_year + 1)
        years_dict = dict(enumerate(years))
        self.print_dict(years_dict)

        _input = input(self.USER_INPUT_MSG['year'])
        if _input.strip():
            years_choice = self.input_parse_int(_input)
            return [years_dict.get(y, y) for y in years_choice]
        else:
            return list(years_dict.values())

    def load_url_data(self, filename):
        if os.path.exists(filename):
            data = self.load_pickle(filename)
        else:
            data = collections.defaultdict(dict)
            
        if self.timestamp in data: pass
        else:
            data = collections.defaultdict(dict)
            self.save_pickle(filename, data)
            
        return data

    def rebuild_summary_with_pics(self, pics_folder):
       
        def r(text, chrs=[], replace=''):
            for c in chrs: text = text.replace(c, replace)
            return text.strip()

        FILES = [i.lower().rstrip('.jpg') for i in os.listdir(pics_folder)]
        SORT_IMAGES = defaultdict(dict)
        EXCEPTIONS = 'rolls-royce', 'mercedes-benz'
        for img in FILES:
            if any([v in img.lower() for v in EXCEPTIONS]):
                for e in EXCEPTIONS: img = img.replace(e, e.replace('-', '|'))
            
            splitted_img = img.split('-')
            image_group_id = '-'.join(splitted_img[:-1])
            ROW = SORT_IMAGES[image_group_id]
            vendor, *car_name, year, _, img_num = splitted_img
            ROW['VENDOR'] = r(vendor, '_', ' ').replace('|', '-')
            ROW['YEAR'] = r(year)
            ROW['CAR'] = r(' '.join([ROW['VENDOR']] + car_name), '_', ' ').title()
            ROW['CAR_LINK'] = 'https://www.netcarshow.com/{}/{}-{}'.format(
                ROW['VENDOR'].replace(' ', '_'), year, '-'.join(car_name)
            ).lower()
            ROW['IMAGE_{}'.format(img_num)] = img.replace('|', '-') + '.jpg'

        return list(SORT_IMAGES.values())

    def get_summary_data(self, csv_filename):
        summary = self.load_data_csv(csv_filename)
        if not summary['rows']:
            summary['rows'] = self.rebuild_summary_with_pics(self.pics_output_folder)
        return summary

    def get_with_cache(self, url, headers):
        if not url in self.cache:
            response = self.get(url, headers=headers)
            self.cache[url] = response.text
        return self.cache[url]

    def root_url(self, a):
        return self.URL['root'] + a.get('href').strip('/')

    def verbose_print(self, msg):
        if self.verbose:
            print(msg)

    def lower_seq_items(self, seq):
        return [i.lower().strip() for i in seq]

    def compare_vendors_got_and_choice(self, vendors_got):
        normalized_vendors_got = self.lower_seq_items(vendors_got)
        failed_to_match = []
        for vendor_choice in self.vendors_choice:
            if not vendor_choice.lower() in normalized_vendors_got:
                failed_to_match.append(vendor_choice)

        if failed_to_match:
            self.verbose_print('FAILED TO MATCH VENDORS:')
            [print('{} - {}'.format(' '*5, f)) for f in failed_to_match]

    def run(s):
        car_links_stored = {r['CAR_LINK'] for r in s.summary_data['rows']}
        s.verbose_print('{} {}'.format('CARS AMOUNT:', len(car_links_stored)))
        
        s.cache = s.caches[s.timestamp]
        
        html = s.get_with_cache(s.URL['root'], headers=s.HEADERS['html'])
        tree = lxml.html.fromstring(html)
        vendors = {s.root_url(a): a.text.strip() for a in tree.cssselect('div.Ll li a')}
        s.compare_vendors_got_and_choice(vendors.values())
        
        normalized_vendors_choice = s.lower_seq_items(s.vendors_choice)
        for vendor_link, vendor_name in vendors.items():
            if vendor_name.lower() not in normalized_vendors_choice: continue
            s.verbose_print('{} {}'.format('-'*60, vendor_name))
            
            html = s.get_with_cache(vendor_link, headers=s.HEADERS['html'])
            tree = lxml.html.fromstring(html)
            cars = {
                li.cssselect('a')[0]: li.text.strip(' "') for li in tree.cssselect('ul.lst li')
                    if li.cssselect('a')
            }

            for a_car, car_year in cars.items():
                car_year = int(car_year) if car_year.isdigit() else None
                
                if car_year in s.years_choice:
                    car_link = s.root_url(a_car)

                    if car_link in car_links_stored: continue
                    else: car_links_stored.add(car_link)

                    car_name = a_car.text_content().strip()
                    s.verbose_print(car_name.rjust(60))

                    html = s.get_with_cache(car_link, headers=s.HEADERS['html'])
                    tree = lxml.html.fromstring(html)

                    resolutions = set(re.findall(r'\d+x\d+', html))
                    if resolutions:
                        highest_resolution = sorted(
                            resolutions, key=lambda x: int(x.split('x')[0]))[-1]
                    
                    pics_available = re.search(r'thz=\[(.*?)\]', html)
                    if pics_available:
                        pics_available = pics_available.group(1).replace("'", "").split(',')
                        
                    pics_amount = len(pics_available)
                    if pics_available and pics_amount > s.pics_amount_limit:
                        pics_amount = s.pics_amount_limit

                    ROW = {}
                    for pic_page in range(1, pics_amount + 1):
                        pic_link ='{}/{}/wallpaper_0{}.htm'.format(car_link, highest_resolution, pic_page)
                        html = s.get_with_cache(pic_link, headers=s.HEADERS['html'])
                        tree = lxml.html.fromstring(html)
                        image = tree.cssselect('meta[itemprop="contentUrl"]')

                        if image:
                            pic_filelink = image[0].get('content')
                            pic_filename = pic_filelink.split('/')[-1]
                            pic_filepath = os.path.join(s.pics_output_folder, pic_filename)

                            if not os.path.exists(pic_filepath):
                                s.HEADERS['pics']['referer'] = car_link
                                response = s.get(pic_filelink, headers=s.HEADERS['pics'])

                                with open(pic_filepath, 'wb') as OUT_IMG:
                                    OUT_IMG.write(response.content)

                            ROW['VENDOR'] = vendor_name
                            ROW['YEAR'] = car_year
                            ROW['CAR'] = car_name
                            ROW['CAR_LINK'] = car_link
                            ROW['IMAGE_{:0>2}'.format(pic_page)] = pic_filename
                            ROW['IMAGE_LINK'] = pic_filelink

                    s.summary_data['rows'].append(ROW)

                    
def main(*NCSImgExt_init_args):
    scr = NCSImgExt(*NCSImgExt_init_args)
    try:
        scr.run()
    finally:
        scr.save_pickle(scr.FILE['cache'], scr.caches)
        scr.save_data_csv(scr.summary_data['rows'], scr.FILE['summary'], scr.CSV_HEADERS)




if __name__ == '__main__':
    main(VENDORS, YEARS, PICS_AMOUNT)









