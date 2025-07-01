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
        "tables": ["Indian imports", "Spot Sales", "Recent spot sales", "Indian NPK arrivals"]
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
                "Grade": product_grade
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
                "Loading port": loading_port
            })
    price_warnings = check_price_outliers(price_data, file_name_short)
    for idx, msg in price_warnings.items():
        final_data[idx]["Average"] = msg


# ======================================
# Основной цикл парсинга
# ======================================
for file_info in FILES:
    file_path = file_info["path"]
    tables_to_parse = file_info["tables"]
    print(f"[INFO] Загружаем файл: {file_path}")
    df = pd.read_excel(file_path, header=None)

    file_name = os.path.basename(file_path).replace('.xlsx', '')
    file_parts = file_name.split('_')
    agency = file_parts[0].strip()
    product = ' '.join(file_parts[1:]).split(' ')[0].strip() if len(file_parts) > 1 else ''

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


# ======================================
# Сохраняем результат в Excel
# ======================================
columns_order = [
    "Publish Date", "Agency", "Product", "Seller", "Buyer", "Vessel",
    "Volume (t)", "Origin", "Date of arrival", "Discharge port",
    "Low", "High", "Average", "Incoterm", "Destination", "Grade", "Loading port"
]

result_df = pd.DataFrame(final_data, columns=columns_order)
output_file = 'processed_final_output.xlsx'
result_df.to_excel(output_file, index=False)
print(f"✅ Файл успешно обработан и сохранён как '{output_file}'")
