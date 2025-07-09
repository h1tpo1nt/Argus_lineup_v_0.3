import pandas as pd
import re
from datetime import datetime
import os

# Define report_date at the beginning
report_date = datetime.now()

# ======================================
# Настройки путей и параметров
# ======================================
FILES = [
    {
        "path": "/content/Argus Ammonia _ Russia version (2025-06-12).xlsx",
        "tables": ["Indian imports", "Spot Sales","Argus Urea Spot Deals Selection", 
                   "Argus Ammonium Sulphate Spot Deals Selection", "Recent spot sales", 
                   "Indian NPK arrivals", "Selected Spot Sales", "India MOP vessel line-up", 
                   "Brazil Potash line-up"]
    }
]
full_month_names = [
    'January', 'February', 'March', 'April', 'May', 'June',
    'July', 'August', 'September', 'October', 'November', 'December'
]
final_data = []

# ======================================
# Функция извлечения даты публикации из имени файла
# ======================================
def extract_publish_date(filename):
    # Ищем дату в имени файла (разные возможные форматы)
    date_match = re.search(r'(\d{4}-\d{2}-\d{2}|\d{2}\.\d{2}\.\d{4}|\d{2}[A-Za-z]{3}\d{4})', filename)

    if not date_match:
        return datetime.now().strftime("%d.%m.%Y")  # Если не найдено — текущая дата

    date_str = date_match.group(1)

    try:
        # Парсим разные форматы
        if '-' in date_str and len(date_str) == 10:
            dt = datetime.strptime(date_str, "%Y-%m-%d")
        elif '.' in date_str and len(date_str.split('.')) == 3:
            dt = datetime.strptime(date_str, "%d.%m.%Y")
        elif re.match(r'^\d{2}[A-Za-z]{3}\d{4}$', date_str):  # например: 12Jun2025
            dt = datetime.strptime(date_str, "%d%b%Y")
        else:
            return datetime.now().strftime("%d.%m.%Y")

        return dt.strftime("%d.%m.%Y")
    except Exception as e:
        print(f"[WARNING] Не удалось распознать дату из имени файла '{filename}': {e}")
        return datetime.now().strftime("%d.%m.%Y")
        
# ======================================
# Функция извлечения даты из строки
# ======================================
def parse_date(date_str, report_date=None):
    if not date_str or str(date_str).strip() == "":
        return ""

    date_str = str(date_str).strip()
    date_str_lower = date_str.lower()

    # Если не передана дата отчета, используем текущую
    if report_date is None:
        report_date = datetime.now()

    # Определяем день
    day_match = re.search(r'\b(\d{1,2})\b', date_str)
    if re.search(r'\bmid\b|\bme?i?d\b', date_str_lower):
        day = 15
    elif re.search(r'\bend\b|\ben?d\b', date_str_lower):
        day = 30
    elif day_match:
        day = int(day_match.group(1))
    else:
        day = 1

    # Определяем месяц
    month_match = re.search(
        r'\b(jan|january|feb|february|mar|march|apr|april|may|jun|june|'
        r'jul|july|aug|august|sep|september|oct|october|nov|november|dec|december)\b',
        date_str_lower
    )
    if not month_match:
        return ""
    month_abbr = month_match.group(1)[:3].capitalize()
    month_num = datetime.strptime(month_abbr, "%b").month

    # Определяем год
    year_match = re.search(r'\b(20\d{2})\b', date_str)
    if year_match:
        year = int(year_match.group(1))  # Явно указанный год в строке
    else:
        # Берем год из report_date (publish_date), если не указан явно
        year = report_date.year

    try:
        dt = datetime(year=year, month=month_num, day=day)
        return dt.strftime("%d.%m.%Y")
    except Exception as e:
        print(f"[WARNING] Ошибка при парсинге даты '{date_str}': {e}")
        return ""

# ======================================
# Обработка цены: Low, High, Average
# ======================================
def process_prices(price_str):
    price_str = re.sub(r'[\s,\–\-\u2013]', ' ', price_str.strip())
    nums = list(map(int, re.findall(r'\b\d+\b', price_str)))
    low = ""
    high = ""
    avg = ""
    if len(nums) == 1:
        avg = str(nums[0])
    elif len(nums) >= 2:
        nums.sort()
        low = str(nums[0])
        high = str(nums[-1])
        avg = str(sum(nums[:2]) // 2)
    return {"Low": low, "High": high, "Average": avg}

# ======================================
# Проверка на выбросы
# ======================================
def check_price_outliers(data_with_rows, filename):
    if not data_with_rows:
        return {}

    prices = []
    valid_data = []

    for row_num, price, idx in data_with_rows:
        try:
            price_int = int(price)
            prices.append(price_int)
            valid_data.append((row_num, price_int, idx))
        except (ValueError, TypeError):
            continue

    if not valid_data:
        return {}

    avg = sum(prices) / len(prices)
    warnings_dict = {}

    for row_number, price, idx in valid_data:
        if avg != 0 and price > 2 * avg:
            warning_msg = f"🟥 Проверьте цену в строке - {row_number} ({filename})"
            warnings_dict[idx] = warning_msg

    return warnings_dict

# ======================================
# Парсинг Indian imports
# ======================================
def parse_indian_imports(df, final_data, agency, product, publish_date, file_name_short):
    start_parsing = False
    price_data = []
    print("[INFO] Начинаем парсить Indian imports...")

    for i, row in df.iterrows():
        first_cell = str(row[0]).strip() if not pd.isna(row[0]) else ""
        if not first_cell:
            continue

        # Поиск начала таблицы
        if re.search(r'indian\s*imports', first_cell, re.IGNORECASE):
            start_parsing = True
            continue

        if not start_parsing:
            continue

        # Прекращение парсинга по служебным словам
        if any(keyword in first_cell.lower() for keyword in ['copyright', 'лицензия']):
            print(f"[INFO] Встретили служебную строку → завершаем парсинг Indian imports")
            break

        # Пропуск строки с заголовком "Seller"
        if first_cell == "Seller":
            continue

        # Проверка: если заполнен только первый столбец — это неполноценные данные → пропускаем
        other_cells_empty = all(
            (idx == 0) or (pd.isna(cell) or str(cell).strip() == "")
            for idx, cell in enumerate(row)
        )
        if other_cells_empty:
            continue

        # Извлечение данных
        seller = first_cell
        buyer = str(row[1]).strip() if 1 < len(row) and not pd.isna(row[1]) else ""
        vessel = str(row[2]).strip() if 2 < len(row) and not pd.isna(row[2]) else ""
        vol_origin = str(row[3]).strip() if 3 < len(row) and not pd.isna(row[3]) else ""
        date_port = str(row[4]).strip() if 4 < len(row) and not pd.isna(row[4]) else ""
        price = str(row[5]).strip() if 5 < len(row) and not pd.isna(row[5]) else ""

        # Обработка Volume и Origin
        volume = ""
        origin = ""
        if vol_origin:
            vol_match = re.match(r'^([\d,]+)\s*(.*)$', vol_origin)
            if vol_match:
                volume = vol_match.group(1).replace(',', '')
                origin = vol_match.group(2).strip()
            else:
                origin = vol_origin

        # Обработка даты и порта разгрузки
        date_str = parse_date(date_port, report_date=report_date)
        discharge_port = ""
        if date_port:
            discharge_port = re.sub(
                r'\d{1,2}\s*-*\s*|'
                r'\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\w*\b|'
                r'\b(mid|early|end)\b',
                '', date_port, flags=re.IGNORECASE
            ).strip()
            discharge_port = re.sub(r'^-+\s*|\s*-+\s*$', '', discharge_port).strip()
            discharge_port = re.sub(r'\d+', '', discharge_port).strip()
            discharge_port = discharge_port.lstrip('-').strip()

        # Обработка цены
        price_info = process_prices(price)
        final_index = len(final_data)
        if price_info["Average"]:
            price_data.append((i + 1, int(price_info["Average"]), final_index))

        # Добавление записи
        final_data.append({
            "Publish Date": publish_date,
            "Agency": agency,
            "Product": product,
            "Seller": seller,
            "Buyer": buyer,
            "Vessel": vessel,
            "Volume (t)": volume,
            "Origin": origin,
            "Date of arrival": date_str,
            "Discharge port": discharge_port,
            "Low": price_info["Low"],
            "High": price_info["High"],
            "Average": price_info["Average"],
            "Incoterm": "",
            "Destination": "",
            "Grade": "",
            "Loading port": "",
            "Shipment Date": "",
            "Charterer": "",
            "ETB": "",
            "Type": ""
        })

    # Проверка цен на выбросы
    price_warnings = check_price_outliers(price_data, file_name_short)
    for idx, msg in price_warnings.items():
        if idx < len(final_data):
            final_data[idx]["Average"] = msg

# ======================================
# Парсинг Spot Sales
# ======================================
def parse_spot_sales(df, final_data, agency, product, publish_date, file_name_short):
    start_parsing = False
    price_data = []
    for i, row in df.iterrows():
        first_cell = str(row[0]).strip() if not pd.isna(row[0]) else ""
        if not first_cell:
            continue
        if re.search(r'spot\s*sales', first_cell, re.IGNORECASE):
            start_parsing = True
            continue
        if start_parsing and first_cell == "Shipment":
            continue
        if start_parsing and any(keyword in first_cell.lower() for keyword in ['copyright', 'лицензия']):
            break
        if start_parsing and first_cell and len(row) > 6:
            shipment = first_cell
            seller = str(row[1]).strip() if not pd.isna(row[1]) else ""
            buyer = str(row[2]).strip() if not pd.isna(row[2]) else ""
            destination_val = str(row[3]).strip() if not pd.isna(row[3]) else ""
            tonnes = str(row[4]).strip() if not pd.isna(row[4]) else ""
            price_incoterm = str(row[5]).strip() if not pd.isna(row[5]) else ""
            origin_value = str(row[6]).strip() if not pd.isna(row[6]) else ""
            
            volume = ""
            if tonnes:
                vol_match = re.search(r'([\d,]+)', tonnes)
                if vol_match:
                    volume = vol_match.group(1).replace(',', '')
            
            price_info = process_prices(price_incoterm)
            final_index = len(final_data)
            if price_info["Average"]:
                price_data.append((i + 1, int(price_info["Average"]), final_index))
            
            incoterm = ""
            incoterm_match = re.search(
                r'(fob|cfr|cif|fca|dap|cpt|c\w+?r|rail|exw|ddp|dpu|d\w+?p|f\w+?t|c\w+?y)',
                price_incoterm,
                re.IGNORECASE
            )
            if incoterm_match:
                incoterm = incoterm_match.group().upper()
            
            final_data.append({
                "Publish Date": publish_date,
                "Agency": agency,
                "Product": product,
                "Seller": seller,
                "Buyer": buyer,
                "Vessel": "",
                "Volume (t)": volume,
                "Origin": origin_value.strip(),
                "Date of arrival": parse_date(shipment, report_date=report_date),
                "Discharge port": "",
                "Low": price_info["Low"],
                "High": price_info["High"],
                "Average": price_info["Average"],
                "Incoterm": incoterm,
                "Destination": destination_val,
                "Grade": "",
                "Loading port": "",
                "Shipment Date": "",
                "Charterer": "",
                "ETB": "",
                "Type": ""
            })
    
    price_warnings = check_price_outliers(price_data, file_name_short)
    for idx, msg in price_warnings.items():
        final_data[idx]["Average"] = msg

# ======================================
# Парсинг Argus Urea Spot Deals Selection
# ======================================
def parse_argus_urea_spot_deals_selection(df, final_data, agency, product, publish_date, file_name_short):
    start_parsing = False
    header_skipped = False  # Флаг для пропуска заголовков

    print("[INFO] Начинаем парсить Argus Urea Spot Deals Selection...")

    for i, row in df.iterrows():
        first_cell = str(row[0]).strip() if not pd.isna(row[0]) else ""

        # Поиск начала таблицы с возможностью частичного совпадения текста
        if re.search(r'argus\s*urea\s*spot\s*deals?\s*selection', first_cell, re.IGNORECASE):
            start_parsing = True
            continue

        if not start_parsing:
            continue

        # Проверяем, не является ли текущая строка заголовком
        if not header_skipped and any(kw in first_cell.lower() for kw in ['grade', 'product', 'origin', 'supplier', 'buyer']):
            header_skipped = True
            continue

        # Пропуск полностью пустых строк
        if all(pd.isna(cell) or str(cell).strip() == "" for cell in row[:8]):
            continue

        # Остановка при появлении служебных строк
        if any(kw in first_cell.lower() for kw in ['copyright', 'total', 'note']):
            print(f"[INFO] Встретили служебную строку → завершаем парсинг Argus Urea Spot Deals Selection")
            break

        # Извлечение данных
        grade = str(row[0]).strip() if len(row) > 0 and not pd.isna(row[0]) else ""
        origin = str(row[1]).strip() if len(row) > 1 and not pd.isna(row[1]) else ""
        supplier = str(row[2]).strip() if len(row) > 2 and not pd.isna(row[2]) else ""
        buyer = str(row[3]).strip() if len(row) > 3 and not pd.isna(row[3]) else ""
        destination = str(row[4]).strip() if len(row) > 4 and not pd.isna(row[4]) else ""
        volume_raw = str(row[5]).strip() if len(row) > 5 and not pd.isna(row[5]) else ""
        price_raw = str(row[6]).strip() if len(row) > 6 and not pd.isna(row[6]) else ""
        shipment_raw = str(row[7]).strip() if len(row) > 7 and not pd.isna(row[7]) else ""

        # Обработка Volume
        volume = re.sub(r'[^\d]', '', volume_raw) if volume_raw else ""

        # Обработка Price
        price_clean = re.sub(r'\s+', ' ', price_raw).strip()
        incoterm = ""
        incoterm_match = re.search(r'(fob|cfr|cif|fca|dap|cpt|c\w+?r|rail|exw|ddp|dpu|d\w+?p|f\w+?t|c\w+?y)', price_clean, re.IGNORECASE)
        if incoterm_match:
            incoterm = incoterm_match.group().upper()
            price_clean = re.sub(incoterm_match.group(), '', price_clean, flags=re.IGNORECASE).strip()

        nums = list(map(int, re.findall(r'\b\d+\b', price_clean)))
        if len(nums) == 1:
            average = str(nums[0])
            low, high = "", ""
        elif len(nums) >= 2:
            low = str(min(nums))
            high = str(max(nums))
            average = str((min(nums) + max(nums)) // 2)
        else:
            low, high, average = "", "", ""

        # Обработка даты отгрузки
        shipment_date = parse_date(shipment_raw, report_date=report_date)

        # Добавление записи
        final_data.append({
            "Publish Date": publish_date,
            "Agency": agency,
            "Product": product,
            "Type": grade,
            "Origin": origin,
            "Seller": supplier,
            "Buyer": buyer,
            "Destination": destination,
            "Volume (t)": volume,
            "Low": low,
            "High": high,
            "Average": average,
            "Incoterm": incoterm,
            "Shipment Date": shipment_date,
            "Vessel": "",
            "Date of arrival": "",
            "Discharge port": "",
            "Grade": "",
            "Loading port": "",
            "Charterer": "",
            "ETB": ""
        })
# ======================================
# Парсинг Argus Ammonium Sulphate Spot Deals Selection
# ======================================
def parse_argus_ammonium_sulphate_spot_deals_selection(df, final_data, agency, product, publish_date, file_name_short):
    start_parsing = False
    header_skipped = False  # Флаг для пропуска заголовков

    for i, row in df.iterrows():
        first_cell = str(row[0]).strip() if not pd.isna(row[0]) else ""

        # Поиск начала таблицы с возможностью частичного совпадения текста
        if re.search(r'argus\s*ammonium\s*sulphate\s*spot\s*deals?\s*selection', first_cell, re.IGNORECASE):
            start_parsing = True
            continue

        if not start_parsing:
            continue

        # Проверяем, не является ли текущая строка заголовком
        if not header_skipped and any(kw in first_cell.lower() for kw in ['grade', 'product', 'origin', 'supplier', 'buyer']):
            header_skipped = True
            continue

        # Пропуск полностью пустых строк
        if all(pd.isna(cell) or str(cell).strip() == "" for cell in row[:8]):
            continue

        # Остановка при появлении служебных строк
        if any(kw in first_cell.lower() for kw in ['copyright', 'total', 'note']):
            break

        # Извлечение данных
        grade = str(row[0]).strip() if len(row) > 0 and not pd.isna(row[0]) else ""
        origin = str(row[1]).strip() if len(row) > 1 and not pd.isna(row[1]) else ""
        supplier = str(row[2]).strip() if len(row) > 2 and not pd.isna(row[2]) else ""
        buyer = str(row[3]).strip() if len(row) > 3 and not pd.isna(row[3]) else ""
        destination = str(row[4]).strip() if len(row) > 4 and not pd.isna(row[4]) else ""
        volume_raw = str(row[5]).strip() if len(row) > 5 and not pd.isna(row[5]) else ""
        price_raw = str(row[6]).strip() if len(row) > 6 and not pd.isna(row[6]) else ""
        shipment_raw = str(row[7]).strip() if len(row) > 7 and not pd.isna(row[7]) else ""

        # Обработка Volume (удаление всех нецифровых символов)
        volume = re.sub(r'[^\d]', '', volume_raw) if volume_raw else ""

        # Обработка Price
        price_clean = re.sub(r'\s+', ' ', price_raw).strip()
        incoterm = ""
        incoterm_match = re.search(r'(fob|cfr|cif|fca|dap|cpt|c\w+?r|rail|exw|ddp|dpu|d\w+?p|f\w+?t|c\w+?y)', price_clean, re.IGNORECASE)
        if incoterm_match:
            incoterm = incoterm_match.group().upper()
            price_clean = re.sub(incoterm_match.group(), '', price_clean, flags=re.IGNORECASE).strip()

        # Извлечение чисел из цены
        nums = list(map(int, re.findall(r'\b\d+\b', price_clean)))
        if len(nums) == 1:
            average = str(nums[0])
            low, high = "", ""
        elif len(nums) >= 2:
            low = str(min(nums))
            high = str(max(nums))
            average = str((min(nums) + max(nums)) // 2)
        else:
            low, high, average = "", "", ""

        # Обработка даты отгрузки
        shipment_date = parse_date(shipment_raw, report_date=report_date)

        # Добавление записи
        final_data.append({
            "Publish Date": publish_date,
            "Agency": agency,
            "Product": product,
            "Type": grade,
            "Origin": origin,
            "Seller": supplier,
            "Buyer": buyer,
            "Destination": destination,
            "Volume (t)": volume,
            "Low": low,
            "High": high,
            "Average": average,
            "Incoterm": incoterm,
            "Shipment Date": shipment_date,
            "Vessel": "",
            "Date of arrival": "",
            "Discharge port": "",
            "Grade": "",
            "Loading port": "",
            "Charterer": "",
            "ETB": ""
        })
# ======================================
# Парсинг Recent spot sales
# ======================================
def parse_recent_spot_sales(df, final_data, agency, product, publish_date, file_name_short):
    start_parsing = False
    header_skipped = False  # Флаг для пропуска заголовков
    price_data = []
    print("[INFO] Начинаем парсить Recent spot sales...")

    for i, row in df.iterrows():
        first_cell = str(row[0]).strip() if not pd.isna(row[0]) else ""
        if not first_cell:
            continue

        # Поиск начала таблицы
        if re.search(r'recent\s*spot\s*sales', first_cell, re.IGNORECASE):
            start_parsing = True
            continue

        if not start_parsing:
            continue

        # Пропуск строки с заголовками
        if not header_skipped and any(kw in first_cell.lower() for kw in ['supplier', 'buyer', 'product', 'volume']):
            header_skipped = True
            continue

        # Остановка по служебным словам
        if any(keyword in first_cell.lower() for keyword in ['copyright', 'лицензия']):
            print(f"[INFO] Встретили служебную строку → завершаем парсинг Recent spot sales")
            break

        # Проверяем, что данных достаточно (минимум 9 столбцов)
        if not len(row) >= 9 or pd.isna(row[0]):
            continue

        # Извлечение данных
        supplier = str(row[0]).strip()
        origin = str(row[1]).strip()
        buyer = str(row[2]).strip()
        destination = str(row[3]).strip()
        product_grade = str(row[4]).strip()
        volume = str(row[5]).strip()
        price_range = str(row[6]).strip()
        basis = str(row[7]).strip()
        shipment_period = str(row[9]).strip()

        # Обработка Volume
        volume_processed = ""
        if volume:
            try:
                vol_expr = re.sub(r'[хХxX*×]', '*', volume.replace(',', ''))
                vol_expr = re.sub(r'[:÷]', '/', vol_expr)
                if re.search(r'[\+\-\*/]', vol_expr):
                    result = eval(vol_expr)
                    volume_processed = str(int(result) * 1000)
                else:
                    vol_num = re.search(r'(\d+)', vol_expr)
                    if vol_num:
                        volume_processed = str(int(vol_num.group(1)) * 1000)
            except Exception:
                volume_processed = ""

        # Обработка цены
        price_info = process_prices(price_range)
        final_index = len(final_data)
        if price_info["Average"]:
            price_data.append((i + 1, int(price_info["Average"]), final_index))

        # Обработка даты отгрузки
        date_str = ""
        if shipment_period and shipment_period != 'TBC':
            shipment_lower = shipment_period.strip().lower()
            for month in full_month_names:
                if shipment_lower == month.lower():
                    month_index = full_month_names.index(month) + 1
                    date_str = f"01.{month_index:02d}.{report_date.year}"
                    break
            if not date_str:
                for month in full_month_names:
                    if shipment_lower == month[:3].lower():
                        month_index = full_month_names.index(month) + 1
                        date_str = f"01.{month_index:02d}.{report_date.year}"
                        break

        # Добавление записи
        final_data.append({
            "Publish Date": publish_date,
            "Agency": agency,
            "Product": product,
            "Seller": supplier,
            "Buyer": buyer,
            "Vessel": "",
            "Volume (t)": volume_processed,
            "Origin": origin,
            "Date of arrival": date_str,
            "Discharge port": "",
            "Low": price_info["Low"],
            "High": price_info["High"],
            "Average": price_info["Average"],
            "Incoterm": basis.upper(),
            "Destination": destination,
            "Grade": product_grade,
            "Loading port": "",
            "Shipment Date": "",
            "Charterer": "",
            "ETB": "",
            "Type": ""
        })

    # Проверка цен на выбросы
    price_warnings = check_price_outliers(price_data, file_name_short)
    for idx, msg in price_warnings.items():
        if idx < len(final_data):
            final_data[idx]["Average"] = msg

# ======================================
# Парсинг Indian NPK arrivals
# ======================================
def parse_indian_npk_arrivals(df, final_data, agency, product, publish_date, file_name_short):
    start_parsing = False
    for i, row in df.iterrows():
        first_cell = str(row[0]).strip() if not pd.isna(row[0]) else ""
        if not first_cell:
            continue
        if re.search(r'indian\s+npk\s+arrivals', first_cell, re.IGNORECASE):
            start_parsing = True
            continue
        if start_parsing and first_cell == "Supplier":
            continue
        if start_parsing and re.search(r'^grand\s+total', first_cell, re.IGNORECASE):
            break
        if start_parsing and first_cell.lower() == "total":
            continue
        if start_parsing and first_cell and len(row) >= 6:
            supplier = str(row[0]).strip()
            buyer = str(row[1]).strip()
            vessel = str(row[2]).strip()
            grade = str(row[3]).strip()
            vol_loading = str(row[4]).strip()
            discharge_port = str(row[5]).strip()
            arrival = str(row[6]).strip() if len(row) > 6 else ""

            volume_clean = ""
            loading_port = ""
            if vol_loading:
                vol_match = re.match(r'^([\d,]+)\s*(.*)$', vol_loading)
                if vol_match:
                    volume_clean = vol_match.group(1).replace(',', '').replace('.', '')
                    loading_port = vol_match.group(2).strip()
                else:
                    loading_port = vol_loading.strip()

            date_str = parse_date(arrival, report_date=report_date)

            final_data.append({
                "Publish Date": publish_date,
                "Agency": agency,
                "Product": product,
                "Seller": "",
                "Buyer": buyer,
                "Vessel": vessel,
                "Volume (t)": volume_clean,
                "Origin": supplier,
                "Date of arrival": date_str,
                "Discharge port": discharge_port,
                "Low": "",
                "High": "",
                "Average": "",
                "Incoterm": "",
                "Destination": "",
                "Grade": grade,
                "Loading port": loading_port,
                "Shipment Date": "",
                "Charterer": "",
                "ETB": "",
                "Type": ""
            })

# ======================================
# Парсинг Selected Spot Sales
# ======================================
def parse_selected_spot_sales(df, final_data, agency, publish_date, file_name_short):
    start_parsing = False
    file_name_base = os.path.basename(file_name_short).split('_')[0].strip()
    file_name_parts = file_name_base.split()
    default_product = file_name_parts[1] if len(file_name_parts) > 1 else ""

    for i, row in df.iterrows():
        first_cell = str(row[0]).strip() if not pd.isna(row[0]) else ""

        if re.search(r'\bselected.*spot.*sales\b', first_cell, re.IGNORECASE):
            start_parsing = True
            continue

        if start_parsing and any(
            isinstance(col, str) and col.strip().lower() in ["origin", "seller", "buyer", "destination", "volume ('000t)", "price delivery period"]
            for col in row[:7]
        ):
            continue

        if start_parsing and any(kw in first_cell.lower() for kw in ['copyright', 'total', 'note']):
            break

        if start_parsing and first_cell and len(row) >= 7:
            if all(pd.isna(cell) or str(cell).strip() == "" for cell in row[1:]):
                continue

            origin = str(row[0]).strip()
            seller = str(row[1]).strip()
            buyer = str(row[2]).strip()
            destination = str(row[3]).strip()
            volume_product = str(row[4]).strip()
            price = str(row[5]).strip()
            delivery_period = str(row[6]).strip() if len(row) > 6 else ""

            volume = ""
            product = ""
            if volume_product:
                vol_prod_match = re.match(r'^([\d,]+)\s*(.*)$', volume_product)
                if vol_prod_match:
                    vol_str = vol_prod_match.group(1)
                    vol_clean = re.sub(r'[^\d]', '', vol_str)
                    if vol_clean.isdigit():
                        volume = vol_clean + "000"
                    product = vol_prod_match.group(2).strip()

            if not product or product.upper() in ["TBC", "-", ".", "..", "...", "N/A"]:
                product = default_product

            price_info = process_prices(price)
            low = price_info["Low"]
            high = price_info["High"]
            average = price_info["Average"]

            incoterm = ""
            if price:
                incoterm_match = re.search(r'[A-Za-z]{3}$', price)
                if incoterm_match:
                    incoterm = incoterm_match.group().upper()

            shipment_date = ""
            if delivery_period:
                month_match = re.search(
                    r'\b(jan|january|feb|february|mar|march|apr|april|may|jun|june|'
                    r'jul|july|aug|august|sep|september|oct|october|nov|november|dec|december)\b',
                    delivery_period.lower()
                )
                if month_match:
                    month_str = month_match.group(1)[:3].capitalize()
                    try:
                        current_year = datetime.now().year
                        dt = datetime.strptime(f"01 {month_str} {current_year}", "%d %b %Y")
                        shipment_date = parse_date(delivery_period, report_date=report_date)
                    except ValueError:
                        pass

            final_data.append({
                "Publish Date": publish_date,
                "Agency": agency,
                "Product": product,
                "Seller": seller,
                "Buyer": buyer,
                "Vessel": "",
                "Volume (t)": volume,
                "Origin": origin,
                "Date of arrival": "",
                "Discharge port": "",
                "Low": low,
                "High": high,
                "Average": average,
                "Incoterm": incoterm,
                "Destination": destination,
                "Grade": "",
                "Loading port": "",
                "Shipment Date": shipment_date,
                "Charterer": "",
                "ETB": "",
                "Type": ""
            })

# ======================================
# Парсинг India MOP vessel line-up
# ======================================
def parse_india_mop_vessel_lineup(df, final_data, agency, product, publish_date, file_name_short):
    header_row = -1
    for i, row in df.iterrows():
        if 'Seller/Buyer' in str(row[0]) and 'Vessel' in str(row[1]) and 'Tonnes' in str(row[2]):
            header_row = i
            break
    
    if header_row == -1:
        return
    
    first_data_row = -1
    for i in range(header_row + 1, len(df)):
        row = df.iloc[i]
        first_cell = str(row[0]).strip()
        if '/' in first_cell and any(c.isdigit() for c in str(row[2])):
            first_data_row = i
            break
    
    if first_data_row == -1:
        return
    
    for i in range(first_data_row, len(df)):
        row = df.iloc[i]
        first_cell = str(row[0]).strip()
        
        if not first_cell or first_cell.lower() in ['copyright', 'total']:
            break
            
        if len(row) < 6 or '/' not in first_cell:
            continue
            
        seller_buyer = first_cell
        vessel = str(row[1]).strip()
        tonnes = str(row[2]).strip()
        load_port = str(row[3]).strip()
        discharge_port = str(row[4]).strip()
        arrival = str(row[5]).strip()

        seller, buyer = seller_buyer.split('/', 1) if '/' in seller_buyer else (seller_buyer, "")
        
        volume = ''.join(c for c in tonnes if c.isdigit())

        final_data.append({
            "Publish Date": publish_date,
            "Agency": agency,
            "Product": product,
            "Seller": seller.strip(),
            "Buyer": buyer.strip(),
            "Vessel": vessel,
            "Volume (t)": volume,
            "Origin": "",
            "Date of arrival": parse_date(arrival, report_date=report_date),
            "Discharge port": discharge_port,
            "Low": "",
            "High": "",
            "Average": "",
            "Incoterm": "",
            "Destination": "",
            "Grade": "",
            "Loading port": load_port,
            "Shipment Date": "",
            "Charterer": "",
            "ETB": "",
            "Type": ""
        })

# ======================================
# Парсинг Brazil Potash line-up
# ======================================
def parse_brazil_potash_lineup(df, final_data, agency, product, publish_date, file_name_short):
    start_row = -1
    for i, row in df.iterrows():
        row_str = ' '.join(str(cell).strip().lower() for cell in row if pd.notna(cell))
        if 'brazil potash line-up' in row_str.lower():
            start_row = i
            break
    
    if start_row == -1:
        return
    
    header_row = -1
    required_headers = ['port', 'vessel', 'charterer', 'origin', 'product', 'volume', 'receiver', 'eta', 'etb']
    
    for i in range(start_row, min(start_row + 10, len(df))):
        row = df.iloc[i]
        row_headers = [str(cell).strip().lower() for cell in row if pd.notna(cell)]
        
        if all(any(h in header for header in row_headers) for h in required_headers):
            header_row = i
            break
    
    if header_row == -1:
        return
    
    col_map = {}
    header = df.iloc[header_row]
    
    for idx, cell in enumerate(header):
        cell_str = str(cell).strip().lower()
        if 'port' in cell_str:
            col_map['port'] = idx
        elif 'vessel' in cell_str:
            col_map['vessel'] = idx
        elif 'charterer' in cell_str:
            col_map['charterer'] = idx
        elif 'origin' in cell_str:
            col_map['origin'] = idx
        elif 'product' in cell_str:
            col_map['product'] = idx
        elif 'volume' in cell_str:
            col_map['volume'] = idx
        elif 'receiver' in cell_str:
            col_map['receiver'] = idx
        elif 'eta' in cell_str:
            col_map['eta'] = idx
        elif 'etb' in cell_str:
            col_map['etb'] = idx
    
    empty_rows = 0
    for i in range(header_row + 1, len(df)):
        row = df.iloc[i]
        
        vessel_col = col_map.get('vessel', 1)
        if pd.isna(row[vessel_col]) or str(row[vessel_col]).strip() == "":
            empty_rows += 1
            if empty_rows >= 3:
                break
            continue
        
        empty_rows = 0
        
        port = str(row[col_map['port']]).strip() if 'port' in col_map and col_map['port'] < len(row) and pd.notna(row[col_map['port']]) else ""
        vessel = str(row[col_map['vessel']]).strip() if 'vessel' in col_map and col_map['vessel'] < len(row) and pd.notna(row[col_map['vessel']]) else ""
        charterer = str(row[col_map['charterer']]).strip() if 'charterer' in col_map and col_map['charterer'] < len(row) and pd.notna(row[col_map['charterer']]) else ""
        origin = str(row[col_map['origin']]).strip() if 'origin' in col_map and col_map['origin'] < len(row) and pd.notna(row[col_map['origin']]) else ""
        product_name = str(row[col_map['product']]).strip() if 'product' in col_map and col_map['product'] < len(row) and pd.notna(row[col_map['product']]) else product
        volume = re.sub(r'[^\d]', '', str(row[col_map['volume']])) if 'volume' in col_map and col_map['volume'] < len(row) and pd.notna(row[col_map['volume']]) else ""
        receiver = str(row[col_map['receiver']]).strip() if 'receiver' in col_map and col_map['receiver'] < len(row) and pd.notna(row[col_map['receiver']]) else ""
        eta_date = parse_date(str(row[col_map['eta']]), report_date=report_date) if 'eta' in col_map and col_map['eta'] < len(row) and pd.notna(row[col_map['eta']]) else ""
        etb_date = parse_date(str(row[col_map['etb']]), report_date=report_date) if 'etb' in col_map and col_map['etb'] < len(row) and pd.notna(row[col_map['etb']]) else ""
        
        final_data.append({
            "Publish Date": publish_date,
            "Agency": agency,
            "Product": product_name,
            "Seller": "",
            "Buyer": receiver,
            "Vessel": vessel,
            "Volume (t)": volume,
            "Origin": origin,
            "Date of arrival": eta_date,
            "Discharge port": port,
            "Low": "",
            "High": "",
            "Average": "",
            "Incoterm": "",
            "Destination": "",
            "Grade": "",
            "Loading port": "",
            "Shipment Date": "",
            "Charterer": charterer,
            "ETB": etb_date,
            "Type": ""
        })

# ======================================
# Основной цикл парсинга
# ======================================
for file_info in FILES:
    file_path = file_info["path"]
    tables_to_parse = file_info["tables"]
    df = pd.read_excel(file_path, header=None, engine='openpyxl')

    file_name = os.path.basename(file_path).replace('.xlsx', '')
    first_part = file_name.split('_')[0].strip()
    parts = first_part.split()

    agency = parts[0] if len(parts) >= 1 else ''
    product = parts[1] if len(parts) >= 2 else ''
    publish_date = extract_publish_date(file_name)
    file_name_short = os.path.basename(file_path)

    if "Indian imports" in tables_to_parse:
        parse_indian_imports(df, final_data, agency, product, publish_date, file_name_short)
    if "Spot Sales" in tables_to_parse:
        parse_spot_sales(df, final_data, agency, product, publish_date, file_name_short)
    if "Argus Urea Spot Deals Selection" in tables_to_parse:
        parse_argus_urea_spot_deals_selection(df, final_data, agency, product, publish_date, file_name_short)
    if "Argus Ammonium Sulphate Spot Deals Selection" in tables_to_parse:
        parse_argus_ammonium_sulphate_spot_deals_selection(df, final_data, agency, product, publish_date, file_name_short)
    if "Recent spot sales" in tables_to_parse:
        parse_recent_spot_sales(df, final_data, agency, product, publish_date, file_name_short)
    if "Indian NPK arrivals" in tables_to_parse:
        parse_indian_npk_arrivals(df, final_data, agency, product, publish_date, file_name_short)
    if "Selected Spot Sales" in tables_to_parse:
        parse_selected_spot_sales(df, final_data, agency, publish_date, file_name_short)
    if "India MOP vessel line-up" in tables_to_parse:
        parse_india_mop_vessel_lineup(df, final_data, agency, product, publish_date, file_name_short)
    if "Brazil Potash line-up" in tables_to_parse:
        parse_brazil_potash_lineup(df, final_data, agency, product, publish_date, file_name_short)

# ======================================
# Сохраняем результат в Excel
# ======================================
columns_order = [
    "Publish Date", "Agency", "Product", "Seller", "Buyer", "Vessel",
    "Volume (t)", "Origin", "Destination", "Date of arrival", "Shipment Date", 
    "ETB", "Discharge port", "Loading port", "Low", "High", "Average", "Incoterm", 
    "Grade", "Type", "Charterer"
]

result_df = pd.DataFrame(final_data, columns=columns_order)
output_file = 'lne_processed_output.xlsx'
result_df.to_excel(output_file, index=False)
print(f"✅ Файл успешно обработан и сохранён как '{output_file}'")
print(f"Таблицы Brazilian MOP, Bronka MOP vessel line-up, St Petersburg MOP vessel line-up - НЕ ВЫВЕДЕНЫ тк ИСХОДНИК БИТЫЙ")
