# -*- coding: utf-8 -*-

import requests
import os
import re
import sys
import time
import random
from urllib3 import encode_multipart_formdata

DS_STORE = '.DS_Store'

URL = 'http://ocr.shouji.sogou.com/v2/ocr/json'
reload(sys)


class OcrProxy(object):
    def __init__(self):
        self.headers = {'Host': 'ocr.shouji.sogou.com',
                   'Connection': 'KeepAlive',
                   'Expect': '100-continue'}

    def read_ocr_result(self, pic_file):
        parent_dir = os.path.basename(os.path.dirname(pic_file))
        data= {'pic':('pic.jpg',open(pic_file,'rb').read())}
        encode_data = encode_multipart_formdata(data, boundary='----WebKitFormBoundary8orYTmcj8BHvQpVU')
        formdata = encode_data[0]
        headers = self.headers
        headers['Content-Type'] = encode_data[1]
        req = requests.post(URL, headers=headers, data=formdata)
        # print(req.content)
        pattern = re.compile(r',EVClub\(\d+\)')
        if req.status_code == 200:
            result = req.json().get("result")
            if not result:
                print("Can't get any data in picture!")
                return
            with open('{0}.csv'.format(parent_dir), 'a+') as csv_file:
                for _data in result:
                    tmp_str = _data.get('content')
                    if not tmp_str.endswith('\n'):
                        tmp_str += '\n'
                    tmp_str = tmp_str.replace('o', '0')
                    tmp_str = tmp_str.replace('1,0', '10')
                    tmp_str = tmp_str.replace('n/a', '0%,')
                    tmp_str = tmp_str.replace('000000000', '000.000.000')
                    tmp_str = tmp_str.replace('000000', '000.000')
                    tmp_str = tmp_str.replace('000.000.000', '000.000.000,')
                    tmp_str = tmp_str.replace('i8', 'iB')
                    tmp_str = tmp_str.replace('Byes', 'Bytes')
                    tmp_str = tmp_str.replace(' Bytes', 'Bytes')
                    tmp_str = tmp_str.replace(' KiB', 'KiB')
                    tmp_str = tmp_str.replace(' MiB', 'MiB')
                    tmp_str = tmp_str.replace('Bytes', 'Bytes,')
                    tmp_str = tmp_str.replace('iB', 'iB,')
                    tmp_str = tmp_str.replace('EV', ',EV')
                    tmp_str = tmp_str.replace(u'：', ':')
                    tmp_str = tmp_str.replace('::', ':')
                    tmp_str = tmp_str.replace('%', '%,')
                    tmp_str = tmp_str.replace(') ', ')')
                    tmp_str = tmp_str.replace('）', ')')
                    tmp_str = tmp_str.replace('（', '(')
                    tmp_str = tmp_str.replace(' (', '(')
                    tmp_str = tmp_str.replace(',\n', '\n')
                    tmp_str = re.sub(pattern, ',', tmp_str)
                    tmp_str = tmp_str.replace(', ', ',')
                    tmp_str = tmp_str.replace(' ,', ',')
                    tmp_str = tmp_str.replace(',,', ',')
                    csv_file.write(tmp_str)

    def read_ocr_results(self, pics_path):
        file_list = os.listdir(pics_path)
        if DS_STORE in file_list:
            file_list.remove(DS_STORE)
        file_list.sort()
        for pic_file in file_list:
            print('Start: identifying "{0}".'.format(pic_file))
            self.read_ocr_result(os.path.join(pics_path, pic_file))
            print('End: "{0}" identified.'.format(pic_file))
            time.sleep(8+random.randint(1, 5))
        print("Done!")


if __name__ == "__main__":
    if len(sys.argv) >= 2:
        pic_folder = sys.argv[1]
        proxy_0 = OcrProxy()
        proxy_0.read_ocr_results(pic_folder)
    else:
        print('Please provide the folder path of picture files which are ready for OCR.')
        sys.exit(1)

