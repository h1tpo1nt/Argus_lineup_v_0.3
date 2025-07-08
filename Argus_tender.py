import pandas as pd
import re
from datetime import datetime
import os

# ======================================
# Колонки итоговой таблицы
# ======================================
columns_order = [
    "Publish Date", "Agency", "Product", "Country", "Holder", "Grade",
    "Volume", "Issue date", "Closing date", "Status", "Shipment"
]

# ======================================
# Настройки путей и параметров
# ======================================
FILES = [
    {
        "path": "/content/Argus NPKs _ Russia version (2025-07-03).xlsx",
        "tables": ["Latest African NPK tender", "Indian NPK, NPS tenders",
                   "phosphate tenders"]
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
        (r'(\d{4}-\d{2}-\d{2})', "%Y-%m-%d"),
        (r'(\d{1,2}-[a-zA-Z]{3,9}-\d{4})', "%d-%b-%Y"),
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
    date_str = str(date_str).strip()
    date_str_lower = date_str.lower()

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

    try:
        # Проверяем, есть ли в строке год
        year_match = re.search(r'\b(20\d{2})\b', date_str)
        if year_match:
            year = int(year_match.group(1))
        else:
            year = datetime.now().year

        # Формат DD MMM → DD.MM
        if re.search(rf'\b{day}\s+{month_abbr}\b', date_str, re.IGNORECASE):
            dt = datetime.strptime(f"{day} {month_abbr}", "%d %b")
            return dt.strftime("%d.%m")

        # Формат MMM DD или MMM YYYY → DD.MM.YYYY
        elif re.search(rf'\b{month_abbr}\s+\d{{1,2}}|\b{month_abbr}\s+\d{{4}}', date_str, re.IGNORECASE):
            dt = datetime.strptime(f"01 {month_abbr} {year}", "%d %b %Y")
            return dt.strftime("%d.%m.%Y")

        # По умолчанию — первое число месяца
        else:
            dt = datetime.strptime(f"01 {month_abbr} {year}", "%d %b %Y")
            return dt.strftime("%d.%m.%Y")

    except Exception as e:
        print(f"[WARNING] Ошибка при парсинге даты '{date_str}': {e}")
        return ""

# ======================================
# Обработка Vol. OOOt: умножение на 1000, с поддержкой диапазонов
# ======================================
def process_volume(vol_str):
    if not vol_str:
        return ""

    cleaned = str(vol_str).strip()

    # Проверяем, является ли строка диапазоном (например, "100-200", "150 - 250")
    range_match = re.search(r'^\s*(\d+)\s*[-–]\s*(\d+)\s*$', cleaned)
    if range_match:
        low = int(range_match.group(1))
        high = int(range_match.group(2))
        avg = (low + high) / 2
        return str(int(avg * 1000))  # Умножаем на 1000 и округляем

    # Убираем всё, кроме чисел и точки/запятой
    cleaned = re.sub(r'[^\d.,]', '', cleaned).replace(',', '.')

    try:
        volume = float(cleaned)
        return str(int(volume * 1000))
    except ValueError:
        return vol_str.strip()  # Оставляем как есть, если не распознано
# ======================================
# Парсинг Latest African NPK tender
# ======================================
def parse_latest_african_npk_tender(df, final_data, agency, product, publish_date, file_name_short):
    start_parsing = False
    empty_count = 0
    print("[INFO] Начинаем парсить Latest African NPK tender...")

    for i, row in df.iterrows():
        first_cell = str(row[0]).strip() if not pd.isna(row[0]) else ""

        # Поиск начала таблицы с возможностью частичного совпадения текста
        if re.search(r'.{0,5}latest.*african.*npk.*tender.{0,5}', first_cell, re.IGNORECASE):
            start_parsing = True
            continue

        if not start_parsing:
            continue

        # После начала таблицы проверяем наличие заголовка "Country/Holder"
        if re.search(r'country\s*/\s*holder', first_cell, re.IGNORECASE):
            continue  # Пропуск строки с заголовком

        # Остановка при 3 пустых строках во втором столбце
        second_cell = str(row[1]).strip() if len(row) > 1 and not pd.isna(row[1]) else ""
        if not second_cell:
            empty_count += 1
            if empty_count >= 3:
                print(f"[INFO] Обнаружено 3 пустых строки подряд → завершаем парсинг Latest African NPK tender")
                break
            continue
        else:
            empty_count = 0

        # Извлечение данных
        country_holder = str(row[0]).strip() if not pd.isna(row[0]) else ""
        product_val = str(row[1]).strip() if len(row) > 1 and not pd.isna(row[1]) else ""
        volume_raw = str(row[2]).strip() if len(row) > 2 and not pd.isna(row[2]) else ""
        issue_date = str(row[3]).strip() if len(row) > 3 and not pd.isna(row[3]) else ""
        closing_date = str(row[4]).strip() if len(row) > 4 and not pd.isna(row[4]) else ""
        status = str(row[5]).strip() if len(row) > 5 and not pd.isna(row[5]) else ""

        # Разделение Country и Holder
        if '/' in country_holder:
            country, holder = country_holder.split('/', 1)
        else:
            country = country_holder
            holder = ""

        # Обработка Volume
        volume = process_volume(volume_raw)

        # Добавление записи
        final_data.append({
            "Publish Date": publish_date,
            "Agency": agency,
            "Product": product,
            "Country": country.strip(),
            "Holder": holder.strip(),
            "Grade": product_val.strip(),
            "Volume": volume,
            "Issue date": parse_date(issue_date),
            "Closing date": parse_date(closing_date),
            "Status": status.strip(),
            "Shipment": ""
        })

    print(f"[INFO] Завершили парсинг Latest African NPK tender, добавлено записей: {len(final_data)}")
  
# ======================================
# Парсинг Shipment по названию месяца
# ======================================
def parse_shipment_month(month_str):
    if not month_str:
        return ""
    
    month_str = str(month_str).strip().lower()
    
    # Словарь сопоставления месяцев
    month_map = {
        "jan": "01", "january": "01",
        "feb": "02", "february": "02",
        "mar": "03", "march": "03",
        "apr": "04", "april": "04",
        "may": "05",
        "jun": "06", "june": "06",
        "jul": "07", "july": "07",
        "aug": "08", "august": "08",
        "sep": "09", "september": "09",
        "oct": "10", "october": "10",
        "nov": "11", "november": "11",
        "dec": "12", "december": "12"
    }

    for key in month_map:
        if key in month_str:
            return f"01.{month_map[key]}"
    
    return ""  # Если месяц не распознан

# ======================================
# Парсинг Indian NPK, NPS tenders
# ======================================
def parse_indian_npk_nps_tenders(df, final_data, agency, product, publish_date, file_name_short):
    start_parsing = False
    skip_next_row = False  # Флаг для пропуска строки с заголовками
    empty_count = 0
    print("[INFO] Начинаем парсить Indian NPK, NPS tenders...")
    
    for i, row in df.iterrows():
        first_cell = str(row[0]).strip() if not pd.isna(row[0]) else ""
        
        # Поиск начала таблицы
        if re.search(r'.{0,5}indian.*npk[\s,]+nps.*tenders?.{0,5}', first_cell, re.IGNORECASE):
            start_parsing = True
            skip_next_row = True  # Следующую строку будем пропускать
            continue
        
        if not start_parsing:
            continue
        
        # Проверяем второй столбец (индекс 1)
        second_cell = str(row[1]).strip() if len(row) > 1 and not pd.isna(row[1]) else ""
        
        # Если второй столбец пуст, увеличиваем счетчик
        if not second_cell:
            empty_count += 1
            if empty_count >= 3:
                print(f"[INFO] Обнаружено 3 пустых строки подряд → завершаем парсинг Indian NPK, NPS tenders")
                break
            continue
        else:
            empty_count = 0  # Сброс счётчика при наличии данных
        
        # Пропускаем строку с заголовками (например, "Country/Holder", "Product", "Volume")
        if skip_next_row:
            skip_next_row = False
            continue
        
        # Извлечение данных по индексам
        holder = str(row[0]).strip() if len(row) > 0 and not pd.isna(row[0]) else ""
        product_val = product
        volume_raw = str(row[2]).strip() if len(row) > 2 and not pd.isna(row[2]) else ""
        issue_date = str(row[3]).strip() if len(row) > 3 and not pd.isna(row[3]) else ""
        closing_date = str(row[4]).strip() if len(row) > 4 and not pd.isna(row[4]) else ""
        shipment_raw = str(row[5]).strip() if len(row) > 5 and not pd.isna(row[5]) else ""
        shipment = parse_shipment_month(shipment_raw)
        status = str(row[6]).strip() if len(row) > 6 and not pd.isna(row[6]) else ""
        
        # Обработка Volume
        volume = process_volume(volume_raw)
        
        # Добавление записи
        final_data.append({
            "Publish Date": publish_date,
            "Agency": agency,
            "Product": product,
            "Country": "",
            "Holder": holder,
            "Grade": product_val,
            "Volume": volume,
            "Issue date": parse_date(issue_date),
            "Closing date": parse_date(closing_date),
            "Status": status,
            "Shipment": shipment  # ← Новое поле
        })
    
    print(f"[INFO] Завершили парсинг Indian NPK, NPS tenders, добавлено записей: {len(final_data)}")

# ======================================
# Парсинг Shipment с датами внутри текста
# ======================================
def parse_shipment_text(text):
    if not text:
        return ""

    # Ищем дату в строке (например, "31 July", "15 Aug", "Sep", "Aug")
    date_match = re.search(r'(\d{1,2})?\s*(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\w*', text, re.IGNORECASE)
    if date_match:
        day = date_match.group(1) if date_match.group(1) else '01'
        month_abbr = date_match.group(2).lower()[:3]

        # Сопоставление месяца
        month_map = {
            "jan": "01", "feb": "02", "mar": "03",
            "apr": "04", "may": "05", "jun": "06",
            "jul": "07", "aug": "08", "sep": "09",
            "oct": "10", "nov": "11", "dec": "12"
        }

        if month_abbr in month_map:
            # Убираем найденную часть и заменяем на DD.MM
            replaced_month = f"{day}.{month_map[month_abbr]}"
            # Убираем оригинал месяца из строки
            cleaned_text = re.sub(r'\s*(\d{1,2})?\s*(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\w*', '', text, flags=re.IGNORECASE)
            # Возвращаем остаток строки + новый формат
            return f"{cleaned_text.strip()} {replaced_month}".strip()
    
    return text  # Если не найдено — возвращаем как есть
# ======================================
# Парсинг phosphate tenders (без привязки к заголовкам)
# ======================================
def parse_phosphate_tenders(df, final_data, agency, product, publish_date, file_name_short):
    start_parsing = False
    skip_next_row = False  # Пропустить заголовок
    empty_count = 0
    print("[INFO] Начинаем парсить phosphate tenders...")

    for i, row in df.iterrows():
        first_cell = str(row[0]).strip() if not pd.isna(row[0]) else ""

        # Поиск начала таблицы
        if re.search(r'.{0,5}phosphate[\s_]+tenders?.{0,5}', first_cell, re.IGNORECASE):
            start_parsing = True
            skip_next_row = True  # Пропустить следующую строку (заголовок)
            continue

        if not start_parsing:
            continue

        # Проверяем второй столбец (индекс 1)
        second_cell = str(row[1]).strip() if len(row) > 1 and not pd.isna(row[1]) else ""

        # Если второй столбец пуст, увеличиваем счетчик
        if not second_cell:
            empty_count += 1
            if empty_count >= 3:
                print(f"[INFO] Обнаружено 3 пустых строки подряд → завершаем парсинг phosphate tenders")
                break
            continue
        else:
            empty_count = 0  # Сброс счётчика при наличии данных

        # Пропускаем строку с заголовками
        if skip_next_row:
            skip_next_row = False
            continue

        # Извлечение данных по индексам
        holder_country = str(row[0]).strip() if len(row) > 0 and not pd.isna(row[0]) else ""
        product_val = str(row[1]).strip() if len(row) > 1 and not pd.isna(row[1]) else ""
        volume_raw = str(row[2]).strip() if len(row) > 2 and not pd.isna(row[2]) else ""
        closing_date = str(row[3]).strip() if len(row) > 3 and not pd.isna(row[3]) else ""
        shipment_raw = str(row[4]).strip() if len(row) > 4 and not pd.isna(row[4]) else ""
        status = str(row[5]).strip() if len(row) > 5 and not pd.isna(row[5]) else ""

        # Разделение Holder / Country
        if '/' in holder_country:
            holder, country = map(str.strip, holder_country.split('/', 1))
        else:
            holder = holder_country
            country = ""

        # Обработка Volume
        volume = process_volume(volume_raw)

        # Обработка Shipment
        shipment = parse_shipment_text(shipment_raw)

        # Добавление записи
        final_data.append({
            "Publish Date": publish_date,
            "Agency": agency,
            "Product": product_val,
            "Country": country,
            "Holder": holder,
            "Grade": product_val,
            "Volume": volume,
            "Issue date": "",  # Не заполняется
            "Closing date": parse_date(closing_date),
            "Status": status,
            "Shipment": shipment
        })

    print(f"[INFO] Завершили парсинг phosphate tenders, добавлено записей: {len(final_data)}")
# ======================================
# Основной цикл парсинга
# ======================================
for file_info in FILES:
    file_path = file_info["path"]
    tables_to_parse = file_info["tables"]
    print(f"[INFO] Загружаем файл: {file_path}")
    df = pd.read_excel(file_path, header=None, engine='openpyxl')
    file_name = os.path.basename(file_path).replace('.xlsx', '')
    first_part = file_name.split('_')[0].strip()
    parts = first_part.split()
    agency = parts[0] if len(parts) >= 1 else ''
    product = parts[1] if len(parts) >= 2 else ''
    publish_date = extract_publish_date(file_name)
    file_name_short = os.path.basename(file_path)

    if "Latest African NPK tender" in tables_to_parse:
        parse_latest_african_npk_tender(df, final_data, agency, product, publish_date, file_name_short)
    if "Indian NPK, NPS tenders" in tables_to_parse:
        parse_indian_npk_nps_tenders(df, final_data, agency, product, publish_date, file_name_short)
    if "phosphate tenders" in tables_to_parse:
        parse_phosphate_tenders(df, final_data, agency, product, publish_date, file_name_short)
# ======================================
# Сохраняем результат в Excel
# ======================================
result_df = pd.DataFrame(final_data, columns=columns_order)
output_file = 'processed_output_Indian_NPK_NPS_Tenders.xlsx'
result_df.to_excel(output_file, index=False)
print(f"✅ Файл успешно обработан и сохранён как '{output_file}'")
