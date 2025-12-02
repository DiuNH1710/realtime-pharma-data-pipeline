import logging
import requests
import json
import time
from datetime import datetime, timezone, timedelta
from kafka import KafkaProducer
from dateutil.parser import parse

from manager_crawl_time.manager_time import get_last_crawl_time, update_last_crawl_time

cookies = {
    '_ga': 'GA1.4.1012141175.1739762337',
    '_gid': 'GA1.4.1395369825.1742048555',
    'SRV': '2ddae073-c2f3-4e19-9e59-6156b85527d9',
    'Abp.Localization.CultureName': 'en',
    'ASP.NET_SessionId': 'sjjpcdng3cfzx4gzelqh3kgp',
    '__RequestVerificationToken': '6O1Fcb2SCkv8dGcUt6TsPleFUIjn6SQ3D6Q1Z4PLRgNJseo55gYVtWxY2_SNhW0YrVb_HgiIudUH1oeAk0DU5Q1AhuA1',
    'XSRF-TOKEN': 'BrQYfUW1elt0jx4BSzYqkZqYTudRHAxFTLZ43247np44Wq_PetPKSGzrEBrMffJi1vZn3Nmp25hxs_4mU4LJEukechA1',
    '_ga_7KDDLF8TZN': 'GS1.4.1742098262.10.0.1742098263.0.0.0',
}

headers = {
    'accept': 'application/json, text/javascript, */*; q=0.01',
    'accept-language': 'vi,en-US;q=0.9,en;q=0.8',
    'cache-control': 'no-cache',
    'content-type': 'application/json',
    'expires': 'Sat, 01 Jan 2000 00:00:00 GMT',
    'origin': 'https://dichvucong.dav.gov.vn',
    'pragma': 'no-cache',
    'priority': 'u=1, i',
    'referer': 'https://dichvucong.dav.gov.vn/congbothuoc/index',
    'sec-ch-ua': '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
    'x-requested-with': 'XMLHttpRequest',
    'x-xsrf-token': 'BrQYfUW1elt0jx4BSzYqkZqYTudRHAxFTLZ43247np44Wq_PetPKSGzrEBrMffJi1vZn3Nmp25hxs_4mU4LJEukechA1',
    # 'cookie': '_ga=GA1.4.1012141175.1739762337; _gid=GA1.4.1395369825.1742048555; SRV=2ddae073-c2f3-4e19-9e59-6156b85527d9; Abp.Localization.CultureName=en; ASP.NET_SessionId=sjjpcdng3cfzx4gzelqh3kgp; __RequestVerificationToken=6O1Fcb2SCkv8dGcUt6TsPleFUIjn6SQ3D6Q1Z4PLRgNJseo55gYVtWxY2_SNhW0YrVb_HgiIudUH1oeAk0DU5Q1AhuA1; XSRF-TOKEN=BrQYfUW1elt0jx4BSzYqkZqYTudRHAxFTLZ43247np44Wq_PetPKSGzrEBrMffJi1vZn3Nmp25hxs_4mU4LJEukechA1; _ga_7KDDLF8TZN=GS1.4.1742098262.10.0.1742098263.0.0.0',
}

def get_total_count():
    print("Getting data from DVC page ...")
    try:
        res = requests.post(
            'https://dichvucong.dav.gov.vn/api/services/app/soDangKy/GetAllPublicServerPaging',
            cookies=cookies,
            headers=headers,
            json={
                'filterText': '',
                'SoDangKyThuoc': {},
                'KichHoat': True,
                'skipCount': 0,
                'maxResultCount': 10,
                'sorting': None,
            }, )
        res = res.json() #data bây giờ là dict hoặc list
        totalCount = (res["result"])["totalCount"]

        return (totalCount)
    except requests.RequestException as e:
        print(f"An error accured: {e}")


def get_data_per_page(skip_count):
    try:
        res = requests.post(
            'https://dichvucong.dav.gov.vn/api/services/app/soDangKy/GetAllPublicServerPaging',
            cookies=cookies,
            headers=headers,
            json={
                'SoDangKyThuoc': {},
                'KichHoat': True,
                'skipCount': skip_count,  # 100 =>totalCount
                'maxResultCount': 100,
                'sorting': None,
            }, )
        res = res.json()
        res = (res["result"])["items"]

        return res
    except requests.RequestException as e:
        print(f"An error accured: {e}")


def format_data(res, last_crawl_time=None):
    list_items = []
    for item in res:
        data = {}

        data['idThuoc'] = item['id']
        data['phanLoaiThuocEnum'] = item['phanLoaiThuocEnum']
        data['tenCongTyDangKy'] = (item['congTyDangKy'])["tenCongTyDangKy"]
        data['diaChiDangKy'] = (item['congTyDangKy'])["diaChiDangKy"]
        data['nuocDangKy'] = (item['congTyDangKy'])["nuocDangKy"]
        data['congTyDangKyId']= item["congTyDangKyId"]

        data['tenCongTySanXuat'] = (item['congTySanXuat'])["tenCongTySanXuat"]
        data['diaChiSanXuat'] = (item['congTySanXuat'])["diaChiSanXuat"]
        data['nuocSanXuat'] = (item['congTySanXuat'])["nuocSanXuat"]
        data['congTySanXuatId']= item['congTySanXuatId']

        dia_chi_san_xuat = data['diaChiSanXuat']
        nuoc_san_xuat = data['nuocSanXuat']

        if nuoc_san_xuat not in dia_chi_san_xuat:
            data['fullAddress'] = f"{dia_chi_san_xuat}, {nuoc_san_xuat}"
        else:
            data['fullAddress'] = dia_chi_san_xuat

        data['soDangKy'] = item['soDangKy']
        data['soDangKyCu'] = item['soDangKyCu']
        data['tenThuoc'] = item['tenThuoc']

        data['dotCap'] = (item['thongTinDangKyThuoc'])['dotCap']
        data['ngayCapSoDangKy'] = (item['thongTinDangKyThuoc'])['ngayCapSoDangKy']
        data['ngayHetHanSoDangKy'] = (item['thongTinDangKyThuoc'])['ngayHetHanSoDangKy']
        data['ngayGiaHanSoDangKy'] = (item['thongTinDangKyThuoc'])['ngayGiaHanSoDangKy']
        data['soQuyetDinh'] = (item['thongTinDangKyThuoc'])['soQuyetDinh']

        data['thongTinRutSoDangKy'] = (item['thongTinRutSoDangKy'])['urlCongVanRutSoDangKy']

        data['dangBaoChe'] = (item['thongTinThuocCoBan'])['dangBaoChe']
        data['dongGoi'] = (item['thongTinThuocCoBan'])['dongGoi']
        data['hamLuong'] = (item['thongTinThuocCoBan'])['hamLuong']
        data['hoatChatChinh'] = (item['thongTinThuocCoBan'])['hoatChatChinh']
        data['tieuChuan'] = (item['thongTinThuocCoBan'])['tieuChuan']
        data['tieuChuanId'] = (item['thongTinThuocCoBan'])['tieuChuanId']
        data['tuoiTho'] = (item['thongTinThuocCoBan'])['tuoiTho']

        data['creationTime'] = item['creationTime']
        data['lastModificationTime'] = item['lastModificationTime']

        modification_time = parse(item["lastModificationTime"])
        modification_time_utc = modification_time.astimezone(timezone.utc)
# neu du lieu cu hơn ngưỡng đó thì bỏ qua (continue) ngược lại thì giữ bản ghi đó
        if last_crawl_time:
            threshold_time = last_crawl_time - timedelta(days=1)
            if modification_time_utc <= threshold_time:
                continue
        else:

            pass

        # list_items.append({ item["maTiepNhan"], item["tenCoSoSX"], item["tenThuongMai"]})
        list_items.append(data)
    return (list_items)


def extract_dvc_data():
    job_name = "extract_data_from_DVC"
    last_crawl_time = get_last_crawl_time(job_name)

    if last_crawl_time:
        if last_crawl_time.tzinfo is None:
            last_crawl_time = last_crawl_time.replace(tzinfo=timezone.utc)


    total_count = get_total_count()
    if total_count is None:
        raise ValueError("Total count is not available.")
    # all_data = []
    skip_count = 0

    producer = KafkaProducer(bootstrap_servers=['kafka:9092'], max_block_ms=5000)
    while skip_count < 1000:
        # total_count+100:

        try:
            data = get_data_per_page(skip_count)
            if data is None:
                print(f"Skipping page with skip_count {skip_count} due to failure")
                time.sleep(5)  # Wait before retrying
                continue
            data_formated = format_data(data, last_crawl_time)

            # all_data.extend(data_formated)
            # Log progress
            print(f"Fetched {skip_count + 100}/{total_count}")

            skip_count += 100

            # Respect rate limits or add a delay if needed

            time.sleep(1)  # Adjust as needed

            json_rows = json.dumps(data_formated, ensure_ascii=False).encode('utf-8')

            producer.send('all_data', json_rows)
            producer.flush()
            print('message sent')
            # kwargs['ti'].xcom_push(key='rows', value=json_rows)
        # return "OK"
        except Exception as e:
            logging.error(f'An error occured: {e}')
            continue


    update_last_crawl_time(job_name)


