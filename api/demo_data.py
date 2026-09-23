"""
Demo Data Module for Indore ETL & AI Parsing
Contains 10 curated unparsed raw Marine Hull records with multi-ship descriptions in fac_desc.
Includes the exact acceptance criteria examples:
- 22FKAAV8: 1 vessel (SEMESTA 6 / TB STEEL 2018 BKI 143T)
- 22FCAB0Y: 2 vessels (TB THEODORE III and BG ATHENA 2701)
Plus 8 additional realistic multi-vessel / complex records.
"""

import os
import openpyxl

DEMO_RAW_10_ROWS = [
    {
        "id": 1,
        "fac_code": "22FKAAV8",
        "fac_risk": "TUG BOAT",
        "fac_desc": "SEMESTA 6 / TB STEEL 2018 BKI 143T",
        "fac_old_ref": "REF/2022/MH/0101",
        "fac_cedant": "PT ASURANSI JASA INDONESIA",
        "fac_broker": "MARSH INDONESIA",
        "fac_insured": "PT SEMESTA MARITIM INDONESIA",
        "currency": "IDR",
        "fac_totsi": 12500000000.0,
        "fac_our_amt": 450000000.0
    },
    {
        "id": 2,
        "fac_code": "22FCAB0Y",
        "fac_risk": "TUG BOAT\nBARGE",
        "fac_desc": "TB THEODORE III / TB UNCLASS STEEL 2004 154T \nBG ATHENA 2701 / BG STEEL 2011 UNCLASS 2156T",
        "fac_old_ref": "REF/2022/MH/0102",
        "fac_cedant": "PT ASURANSI CENTRAL ASIA",
        "fac_broker": "AON INDONESIA",
        "fac_insured": "PT PELAYARAN THEODORE OFFSHORE",
        "currency": "IDR",
        "fac_totsi": 28000000000.0,
        "fac_our_amt": 1200000000.0
    },
    {
        "id": 3,
        "fac_code": "23MDA001",
        "fac_risk": "TUG BOAT\nBARGE",
        "fac_desc": "TB TRANS POWER 01 / TB STEEL 2016 BKI 180T \nBG TRANS POWER 02 / BG STEEL 2016 BKI 2400T",
        "fac_old_ref": "REF/2023/MH/0215",
        "fac_cedant": "PT ASURANSI ASTRA BUANA",
        "fac_broker": "DIRECT",
        "fac_insured": "PT TRANS POWER MARINE TBK",
        "currency": "IDR",
        "fac_totsi": 34000000000.0,
        "fac_our_amt": 850000000.0
    },
    {
        "id": 4,
        "fac_code": "23MDA002",
        "fac_risk": "BULK CARRIER",
        "fac_desc": "MV SAMUDERA JAYA 88 / BC STEEL 2012 NK 32000 DWT",
        "fac_old_ref": "REF/2023/MH/0216",
        "fac_cedant": "PT ASURANSI WAHANA TATA",
        "fac_broker": "WILLIS TOWERS WATSON",
        "fac_insured": "PT SAMUDERA SHIPPING SERVICES",
        "currency": "USD",
        "fac_totsi": 9500000.0,
        "fac_our_amt": 320000.0
    },
    {
        "id": 5,
        "fac_code": "23MDA003",
        "fac_risk": "TUG BOAT\nBARGE",
        "fac_desc": "TB MITRA ANUGERAH 09 / TB STEEL 2015 BKI 160T \nBG MITRA 3001 / BG STEEL 2017 BKI 3000T",
        "fac_old_ref": "REF/2023/MH/0217",
        "fac_cedant": "PT ASURANSI SINARMAS",
        "fac_broker": "DIRECT",
        "fac_insured": "PT MITRA BAHARI SENTOSA",
        "currency": "IDR",
        "fac_totsi": 42000000000.0,
        "fac_our_amt": 980000000.0
    },
    {
        "id": 6,
        "fac_code": "23MDA004",
        "fac_risk": "OIL TANKER",
        "fac_desc": "MT PERMATA SAMUDERA / OT STEEL 2019 ABS 4500 DWT",
        "fac_old_ref": "REF/2023/MH/0218",
        "fac_cedant": "PT ASURANSI TRIPAKARTA",
        "fac_broker": "HOWDEN INDONESIA",
        "fac_insured": "PT PERMATA TANKER LINE",
        "currency": "USD",
        "fac_totsi": 6200000.0,
        "fac_our_amt": 150000.0
    },
    {
        "id": 7,
        "fac_code": "23MDA005",
        "fac_risk": "TUG BOAT\nBARGE",
        "fac_desc": "TB BAHARI PERKASA / TB STEEL 2014 BKI 150T \nBG BAHARI 2801 / BG STEEL 2014 UNCLASS 2200T",
        "fac_old_ref": "REF/2023/MH/0219",
        "fac_cedant": "PT ASURANSI TUGU PRATAMA",
        "fac_broker": "DIRECT",
        "fac_insured": "PT BAHARI LINES INDONESIA",
        "currency": "IDR",
        "fac_totsi": 26500000000.0,
        "fac_our_amt": 640000000.0
    },
    {
        "id": 8,
        "fac_code": "23MDA006",
        "fac_risk": "CARGO VESSEL",
        "fac_desc": "KM NUSANTARA RAYA / GC WOOD 2008 UNCLASS 500 GT",
        "fac_old_ref": "REF/2023/MH/0220",
        "fac_cedant": "PT ASURANSI RAMAYANA",
        "fac_broker": "DIRECT",
        "fac_insured": "PT PELAYARAN NUSANTARA RAYA",
        "currency": "IDR",
        "fac_totsi": 8500000000.0,
        "fac_our_amt": 220000000.0
    },
    {
        "id": 9,
        "fac_code": "23MDA007",
        "fac_risk": "TUG BOAT\nBARGE",
        "fac_desc": "TB SRIKANDI 05 / TB STEEL 2020 BKI 195T \nBG SRIKANDI 3302 / BG STEEL 2020 BKI 3300T",
        "fac_old_ref": "REF/2023/MH/0221",
        "fac_cedant": "PT ASURANSI DAYIN MITRA",
        "fac_broker": "MARSH INDONESIA",
        "fac_insured": "PT SRIKANDI MARITIM UTAMA",
        "currency": "IDR",
        "fac_totsi": 49000000000.0,
        "fac_our_amt": 1150000000.0
    },
    {
        "id": 10,
        "fac_code": "23MDA008",
        "fac_risk": "PASSENGER SHIP",
        "fac_desc": "KM EXPRESS BAHARI 99 / PS FIBER 2021 BKI 250 PAX",
        "fac_old_ref": "REF/2023/MH/0222",
        "fac_cedant": "PT ASURANSI BINTANG",
        "fac_broker": "DIRECT",
        "fac_insured": "PT PELAYARAN SPEEDBOAT BAHARI",
        "currency": "IDR",
        "fac_totsi": 18000000000.0,
        "fac_our_amt": 420000000.0
    }
]

def generate_demo_excel(target_path: str):
    """Generates an Excel file with the 10 unparsed raw rows."""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "MR11_Raw_Unparsed"

    headers = [
        "fac_code", "fac_risk", "fac_desc", "fac_old_ref", "fac_cedant",
        "fac_broker", "fac_insured", "currency", "fac_totsi", "fac_our_amt"
    ]
    ws.append(headers)

    for item in DEMO_RAW_10_ROWS:
        ws.append([item[h] for h in headers])

    wb.save(target_path)
    wb.close()
    return target_path

if __name__ == "__main__":
    generate_demo_excel("demo_unparsed_marine_hull_10rows.xlsx")
    print("Demo Excel generated successfully.")
