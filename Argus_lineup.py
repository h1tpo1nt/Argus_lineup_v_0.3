import pandas as pd
import re
from datetime import datetime
import os

# ======================================
# Настройки путей и параметров
# ======================================
FILES = [
    {
        "path": "/content/Argus Ammonia _ Russia version (2025-06-12).xlsx",
        "tables": ["Indian imports", "Spot Sales", "Recent spot sales", "Indian NPK arrivals", 
                   "Selected Spot Sales", "India MOP vessel line-up", "Brazil Potash line-up"]
    }
]
full_month_names = [
    'January', 'February', 'March', 'April', 'May', 'June',
    'July', 'August', 'September', 'October', 'November', 'December'
]
final_data = []

# ======================================
# Функция извлечения даты из имени файла
# ======================================
def extract_publish_date(filename):
    date_patterns = [
        # Формат с круглыми скобками: (2025-06-12)
        (r'(\d{4}-\d{2}-\d{2})', "%Y-%m-%d"),
        # Формат: 2025-06-11
        (r'(\d{4}-\d{2}-\d{2})', "%Y-%m-%d"),
        # Формат: 12-Jun-2025
        (r'(\d{1,2}-[a-zA-Z]{3,9}-\d{4})', "%d-%b-%Y"),
        # Формат: 12Jun2025
        (r'(\d{1,2}[a-zA-Z]{3,9}\d{4})', "%d%b%Y")
    ]
    for pattern, fmt in date_patterns:
        match = re.search(pattern, filename, re.IGNORECASE)
        if match:
            try:
                date_str = match.group(1)
                dt = datetime.strptime(date_str, fmt)
                return dt.strftime("%d.%m.%Y")
            except Exception as e:
                print(f"[WARNING] Не удалось распознать дату из '{filename}': {e}")
                continue
    print(f"[WARNING] Нет даты в названии файла: '{filename}'")
    return ""


# ======================================
# Парсинг даты по правилам
# ======================================
def parse_date(date_str):
    if not date_str:
        return ""
    date_str_lower = date_str.lower()
    if re.search(r'\bmid\b|\bearly\b|\bme?i?d\b|\bear?ly\b', date_str_lower):
        day = 15
    elif re.search(r'\bend\b|\ben?d\b', date_str_lower):
        day = 30
    else:
        day_match = re.search(r'\b(\d{1,2})\b', date_str)
        day = int(day_match.group(1)) if day_match else 1
    month_match = re.search(
        r'\b(jan|january|feb|february|mar|march|apr|april|may|jun|june|'
        r'jul|july|aug|august|sep|september|oct|october|nov|november|dec|december)\b',
        date_str_lower
    )
    if month_match:
        month_str = month_match.group(1)[:3].capitalize()
        try:
            dt = datetime.strptime(f"{day} {month_str}", "%d %b")
            return dt.strftime("%d.%m")
        except ValueError:
            return ""
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
# Проверка на выбросы (цена > 2× от среднего) с указанием номера строки в Excel и файла
# ======================================
def check_price_outliers(data_with_rows, filename):
    """
    Проверяет аномальные цены и возвращает словарь {index_in_final_data: warning_message}.
    :param data_with_rows: Список кортежей (excel_row_number, price, index_in_final_data)
    :param filename: Имя файла для указания источника
    :return: dict
    """
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
            print(f"[WARNING] Неверная цена '{price}' в строке {row_num} → пропущено")

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
        if re.search(r'indian\s*imports', first_cell, re.IGNORECASE):
            start_parsing = True
            continue
        if start_parsing and first_cell == "Seller":
            continue
        if start_parsing and any(keyword in first_cell.lower() for keyword in ['copyright', 'лицензия']):
            break
        if start_parsing and first_cell:
            seller = first_cell
            buyer = str(row[1]).strip() if 1 < len(row) and not pd.isna(row[1]) else ""
            vessel = str(row[2]).strip() if 2 < len(row) and not pd.isna(row[2]) else ""
            vol_origin = str(row[3]).strip() if 3 < len(row) and not pd.isna(row[3]) else ""
            date_port = str(row[4]).strip() if 4 < len(row) and not pd.isna(row[4]) else ""
            price = str(row[5]).strip() if 5 < len(row) and not pd.isna(row[5]) else ""
            volume = ""
            origin = ""
            if vol_origin:
                vol_match = re.match(r'^([\d,]+)\s*(.*)$', vol_origin)
                if vol_match:
                    volume = vol_match.group(1).replace(',', '')
                    origin = vol_match.group(2).strip()
                else:
                    origin = vol_origin
            date_str = parse_date(date_port)
            discharge_port = ""
            if date_port:
                discharge_port = re.sub(
                    r'\d{1,2}\s*-*\s*|'
                    r'\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\w*\b|'
                    r'\b(mid|early|end)\b|'
                    r'\bjune\b|\bjuly\b|\baugust\b|\bseptember\b|\boctober\b|\bnovember\b|\bdecember\b',
                    '', date_port, flags=re.IGNORECASE
                ).strip()
                discharge_port = re.sub(r'^-+\s*|\s*-+\s*$', '', discharge_port).strip()
                discharge_port = re.sub(r'\d+', '', discharge_port).strip()
                discharge_port = discharge_port.lstrip('-').strip()
            price_info = process_prices(price)
            final_index = len(final_data)
            if price_info["Average"]:
                price_data.append((i + 1, int(price_info["Average"]), final_index))
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
                "ETB": ""
            })
    price_warnings = check_price_outliers(price_data, file_name_short)
    for idx, msg in price_warnings.items():
        final_data[idx]["Average"] = msg


# ======================================
# Парсинг Spot Sales
# ======================================
def parse_spot_sales(df, final_data, agency, product, publish_date, file_name_short):
    start_parsing = False
    price_data = []
    print("[INFO] Переходим к парсингу Spot Sales...")
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
            origin_processed = origin_value.strip()  # Полное значение без обработки
            date_str = parse_date(shipment)
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
            origin_processed = origin_value.strip()
            final_data.append({
                "Publish Date": publish_date,
                "Agency": agency,
                "Product": product,
                "Seller": seller,
                "Buyer": buyer,
                "Vessel": "",
                "Volume (t)": volume,
                "Origin": origin_processed,
                "Date of arrival": date_str,
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
                "ETB": ""
            })
    price_warnings = check_price_outliers(price_data, file_name_short)
    for idx, msg in price_warnings.items():
        final_data[idx]["Average"] = msg


# ======================================
# Парсинг Recent spot sales
# ======================================
def parse_recent_spot_sales(df, final_data, agency, product, publish_date, file_name_short):
    start_parsing = False
    price_data = []
    print("[INFO] Переходим к парсингу Recent spot sales...")
    for i, row in df.iterrows():
        first_cell = str(row[0]).strip() if not pd.isna(row[0]) else ""
        if not first_cell:
            continue
        if re.search(r'recent\s*spot\s*sales', first_cell, re.IGNORECASE):
            start_parsing = True
            continue
        if start_parsing and first_cell == "Supplier":
            continue
        if start_parsing and any(keyword in first_cell.lower() for keyword in ['copyright', 'лицензия']):
            break
        if start_parsing and first_cell and len(row) >= 9:
            supplier = str(row[0]).strip()
            origin = str(row[1]).strip()
            buyer = str(row[2]).strip()
            destination = str(row[3]).strip()
            product_grade = str(row[4]).strip()
            volume = str(row[5]).strip()
            price_range = str(row[6]).strip()
            basis = str(row[7]).strip()
            shipment_period = str(row[9]).strip()

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
                except Exception as ve:
                    print(f"[ERROR] Ошибка при обработке Volume: {ve}")
                    volume_processed = ""

            price_info = process_prices(price_range)
            final_index = len(final_data)
            if price_info["Average"]:
                price_data.append((i + 1, int(price_info["Average"]), final_index))

            date_str = ""
            if shipment_period and shipment_period != 'TBC':
                shipment_lower = shipment_period.strip().lower()
                for month in full_month_names:
                    if shipment_lower == month.lower():
                        month_index = full_month_names.index(month) + 1
                        date_str = f"01.{month_index:02d}"
                        break
                if not date_str:
                    for month in full_month_names:
                        if shipment_lower == month[:3].lower():
                            month_index = full_month_names.index(month) + 1
                            date_str = f"01.{month_index:02d}"
                            break

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
                "ETB": ""
            })
    price_warnings = check_price_outliers(price_data, file_name_short)
    for idx, msg in price_warnings.items():
        final_data[idx]["Average"] = msg


# ======================================
# Парсинг Indian NPK arrivals
# ======================================
def parse_indian_npk_arrivals(df, final_data, agency, product, publish_date, file_name_short):
    start_parsing = False
    price_data = []
    print("[INFO] Переходим к парсингу Indian NPK arrivals...")
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
            print(f"[INFO] Найдена строка 'Grand Total' — завершаем парсинг Indian NPK arrivals")
            break
        if start_parsing and first_cell.lower() == "total":
            print(f"[DEBUG] Пропускаем строку 'Total' (Indian NPK arrivals) на строке {i+1}")
            continue
        if start_parsing and first_cell:
            if len(row) < 6:
                print(f"[WARNING] Строка {i} содержит меньше 6 колонок → пропускаем.")
                continue
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

            date_str = parse_date(arrival)
            price_info = process_prices("")
            final_index = len(final_data)
            if price_info["Average"]:
                price_data.append((i + 1, int(price_info["Average"]), final_index))

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
                "ETB": ""
            })
    price_warnings = check_price_outliers(price_data, file_name_short)
    for idx, msg in price_warnings.items():
        final_data[idx]["Average"] = msg

# ======================================
# Парсинг Selected Spot Sales
# ======================================
def parse_selected_spot_sales(df, final_data, agency, publish_date, file_name_short):
    start_parsing = False
    print("[INFO] Начинаем парсить Selected Spot Sales...")

    # Получаем product из имени файла
    file_name_base = os.path.basename(file_name_short).split('_')[0].strip()
    file_name_parts = file_name_base.split()
    default_product = file_name_parts[1] if len(file_name_parts) > 1 else ""

    for i, row in df.iterrows():
        first_cell = str(row[0]).strip() if not pd.isna(row[0]) else ""

        # Поиск начала таблицы
        if re.search(r'\bselected.*spot.*sales\b', first_cell, re.IGNORECASE):
            start_parsing = True
            continue

        # Пропуск строк с заголовками
        if start_parsing and any(
            isinstance(col, str) and col.strip().lower() in ["origin", "seller", "buyer", "destination", "volume ('000t)", "price delivery period"]
            for col in row[:7]
        ):
            continue

        # Окончание таблицы
        if start_parsing and any(kw in first_cell.lower() for kw in ['copyright', 'total', 'note']):
            break

        if start_parsing and first_cell:
            # Пропуск строк, где все ячейки, кроме первой, пустые
            if all(pd.isna(cell) or str(cell).strip() == "" for cell in row[1:]):
                continue

            if len(row) < 7:
                print(f"[WARNING] Строка {i} содержит меньше 7 колонок → пропускаем.")
                continue

            origin = str(row[0]).strip()
            seller = str(row[1]).strip()
            buyer = str(row[2]).strip()
            destination = str(row[3]).strip()
            volume_product = str(row[4]).strip()
            price = str(row[5]).strip()
            delivery_period = str(row[6]).strip() if len(row) > 6 else ""

            # Обработка Volume и Product
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

            # Если Product пустой или TBC → брать из имени файла
            if not product or product.upper() in ["TBC", "-", ".", "..", "...", "N/A"]:
                product = default_product

            # Обработка цены
            price_info = process_prices(price)
            low = price_info["Low"]
            high = price_info["High"]
            average = price_info["Average"]

            # Обработка Incoterm
            incoterm = ""
            if price:
                incoterm_match = re.search(r'[A-Za-z]{3}$', price)
                if incoterm_match:
                    incoterm = incoterm_match.group().upper()

            # Обработка Shipment Date
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
                        shipment_date = dt.strftime("%d.%m")
                    except ValueError:
                        pass

            # Добавляем запись
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
                "ETB": ""
            })
# ======================================
# Парсинг India MOP vessel line-up
# удалить вручную строки с непонятными данными
# ======================================
def parse_india_mop_vessel_lineup(df, final_data, agency, product, publish_date, file_name_short):
    print("[INFO] Начинаем парсить India MOP vessel line-up...")
    
    # Сначала найдем точное положение шапки таблицы
    header_row = -1
    for i, row in df.iterrows():
        if 'Seller/Buyer' in str(row[0]) and 'Vessel' in str(row[1]) and 'Tonnes' in str(row[2]):
            header_row = i
            break
    
    if header_row == -1:
        print("[ERROR] Не найдена шапка таблицы India MOP vessel line-up")
        return
    
    # Теперь найдем первую строку с данными после шапки
    first_data_row = -1
    for i in range(header_row + 1, len(df)):
        row = df.iloc[i]
        first_cell = str(row[0]).strip()
        if '/' in first_cell and any(c.isdigit() for c in str(row[2])):
            first_data_row = i
            break
    
    if first_data_row == -1:
        print("[ERROR] Не найдены данные после шапки таблицы")
        return
    
    print(f"[DEBUG] Шапка таблицы в строке {header_row+1}, данные начинаются с строки {first_data_row+1}")
    
    # Теперь парсим только данные, начиная с найденной строки
    for i in range(first_data_row, len(df)):
        row = df.iloc[i]
        first_cell = str(row[0]).strip()
        
        # Критерии остановки
        if not first_cell or first_cell.lower() in ['copyright', 'total']:
            break
            
        # Проверка формата данных
        if len(row) < 6 or '/' not in first_cell:
            continue
            
        # Обработка данных
        seller_buyer = first_cell
        vessel = str(row[1]).strip()
        tonnes = str(row[2]).strip()
        load_port = str(row[3]).strip()
        discharge_port = str(row[4]).strip()
        arrival = str(row[5]).strip()

        # Разделение Seller/Buyer
        seller, buyer = seller_buyer.split('/', 1) if '/' in seller_buyer else (seller_buyer, "")
        
        # Очистка объема
        volume = ''.join(c for c in tonnes if c.isdigit())

        # Добавление записи
        final_data.append({
            "Publish Date": publish_date,
            "Agency": agency,
            "Product": product,
            "Seller": seller.strip(),
            "Buyer": buyer.strip(),
            "Vessel": vessel,
            "Volume (t)": volume,
            "Origin": "",
            "Date of arrival": parse_date(arrival),
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
            "ETB": ""
        })
# ======================================
# Парсинг Brazil Potash line-up
# ======================================
def parse_brazil_potash_lineup(df, final_data, agency, product, publish_date, file_name_short):
    print("[INFO] Начинаем парсить Brazil Potash line-up...")
    
    # 1. Находим начало таблицы по ключевым словам
    start_row = -1
    for i, row in df.iterrows():
        row_str = ' '.join(str(cell).strip().lower() for cell in row if pd.notna(cell))
        if 'brazil potash line-up' in row_str.lower():
            start_row = i
            break
    
    if start_row == -1:
        print("[ERROR] Не найдено начало таблицы Brazil Potash line-up")
        return
    
    # 2. Ищем строку с заголовками
    header_row = -1
    required_headers = ['port', 'vessel', 'charterer', 'origin', 'product', 'volume', 'receiver', 'eta', 'etb']
    
    for i in range(start_row, min(start_row + 10, len(df))):  # Ищем в следующих 10 строках
        row = df.iloc[i]
        row_headers = [str(cell).strip().lower() for cell in row if pd.notna(cell)]
        
        if all(any(h in header for header in row_headers) for h in required_headers):
            header_row = i
            break
    
    if header_row == -1:
        print("[ERROR] Не найдена строка с заголовками Brazil Potash line-up")
        return
    
    # 3. Определяем индексы колонок
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
    
    # 4. Парсим данные
    empty_rows = 0
    for i in range(header_row + 1, len(df)):
        row = df.iloc[i]
        
        # Проверяем второй столбец на пустоту
        vessel_col = col_map.get('vessel', 1)
        if pd.isna(row[vessel_col]) or str(row[vessel_col]).strip() == "":
            empty_rows += 1
            if empty_rows >= 3:
                break
            continue
        
        empty_rows = 0
        
        # Получаем данные
        port = str(row[col_map['port']]).strip() if 'port' in col_map and col_map['port'] < len(row) and pd.notna(row[col_map['port']]) else ""
        vessel = str(row[col_map['vessel']]).strip() if 'vessel' in col_map and col_map['vessel'] < len(row) and pd.notna(row[col_map['vessel']]) else ""
        charterer = str(row[col_map['charterer']]).strip() if 'charterer' in col_map and col_map['charterer'] < len(row) and pd.notna(row[col_map['charterer']]) else ""
        origin = str(row[col_map['origin']]).strip() if 'origin' in col_map and col_map['origin'] < len(row) and pd.notna(row[col_map['origin']]) else ""
        
        # Product (из таблицы или названия файла)
        product_name = str(row[col_map['product']]).strip() if 'product' in col_map and col_map['product'] < len(row) and pd.notna(row[col_map['product']]) else product
        
        # Volume (очистка)
        volume = ""
        if 'volume' in col_map and col_map['volume'] < len(row) and pd.notna(row[col_map['volume']]):
            volume = re.sub(r'[^\d]', '', str(row[col_map['volume']]))
        
        receiver = str(row[col_map['receiver']]).strip() if 'receiver' in col_map and col_map['receiver'] < len(row) and pd.notna(row[col_map['receiver']]) else ""
        
        # Обработка дат
        eta_date = parse_date(str(row[col_map['eta']])) if 'eta' in col_map and col_map['eta'] < len(row) and pd.notna(row[col_map['eta']]) else ""
        etb_date = parse_date(str(row[col_map['etb']])) if 'etb' in col_map and col_map['etb'] < len(row) and pd.notna(row[col_map['etb']]) else ""
        
        # Добавляем запись
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
            "ETB": etb_date
        })

    print(f"[INFO] Обработано {len([x for x in final_data if x['Agency'] == agency and x['Product'] == product_name])} записей Brazil Potash line-up")
# ======================================
# Основной цикл парсинга
# ======================================
for file_info in FILES:
    file_path = file_info["path"]
    tables_to_parse = file_info["tables"]
    print(f"[INFO] Загружаем файл: {file_path}")
    df = pd.read_excel(file_path, header=None, engine='openpyxl')

    file_name = os.path.basename(file_path).replace('.xlsx', '')
    first_part = file_name.split('_')[0].strip()  # Берём первую часть до символа "_"
    parts = first_part.split()

    if len(parts) >= 1:
      agency = parts[0]  # Argus
    else:
      agency = ''

    if len(parts) >= 2:
      product = parts[1]  # Ammonia
    else:
      product = ''

    publish_date = extract_publish_date(file_name)
    file_name_short = os.path.basename(file_path)

    if "Indian imports" in tables_to_parse:
        parse_indian_imports(df, final_data, agency, product, publish_date, file_name_short)
    if "Spot Sales" in tables_to_parse:
        parse_spot_sales(df, final_data, agency, product, publish_date, file_name_short)
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
    "Grade", "Charterer"
]

result_df = pd.DataFrame(final_data, columns=columns_order)
output_file = 'processed_output.xlsx'
result_df.to_excel(output_file, index=False)
print(f"✅ Файл успешно обработан и сохранён как '{output_file}'")
