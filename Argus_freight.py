import pandas as pd
import re
from datetime import datetime
import os

# ======================================
# Настройки путей и параметров
# ======================================
FILES = [
    {
        "path": "/content/Argus NPKs _ Russia version (2025-07-03).xlsx", 
        "tables": ["Ammonia freight rates","Dry bulk fertilizer freight assessments", 
                  "Urea freight", "Phosphate freigh", "Potash freight"]
    }
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
# Парсинг  Ammonia freight rates
# ======================================
def parse_ammonia_freight_rates(df, final_data, agency, product, publish_date):
    print("[INFO] Начинаем парсить таблицу 'Ammonia freight rates'...")
    
    # 1. Находим начало таблицы по заголовку (регистр не важен)
    start_row = -1
    for i, row in df.iterrows():
        if any(cell for cell in row if isinstance(cell, str) and "ammonia freight rates" in cell.lower()):
            start_row = i
            break
    if start_row == -1:
        print("[ERROR] Не найдена таблица 'Ammonia freight rates'")
        return

    # 2. Ищем строку с заголовком "Route"
    route_header_row = -1
    for i in range(start_row + 1, start_row + 5):
        if i >= len(df):
            break
        row = df.iloc[i]
        if isinstance(row[0], str) and "route" in row[0].lower():
            route_header_row = i
            break
    if route_header_row == -1:
        print("[ERROR] Не найден заголовок 'Route'")
        return

    # 3. Парсим данные, начиная со строки после "Route"
    empty_rows = 0
    for i in range(route_header_row + 1, len(df)):
        row = df.iloc[i]

        # Пропускаем пустые строки
        if all(pd.isna(cell) or str(cell).strip() == "" for cell in row):
            continue

        # Получаем значения ячеек
        route = str(row[0]).strip() if pd.notna(row[0]) else ""
        volume = str(row[1]).strip() if len(row) > 1 and pd.notna(row[1]) else ""
        rate_change = str(row[2]).strip() if len(row) > 2 and pd.notna(row[2]) else ""

        # Пропускаем строки, где во втором столбце (Volume) пусто
        if not volume:
            empty_rows += 1
            if empty_rows >= 3:
                break
            continue
        empty_rows = 0

        # Разделение Route на Loading / Destination
        loading = ""
        destination = ""
        if route:
            if " to " in route:
                parts = route.split(" to ", 1)
                loading = parts[0].strip()
                destination = parts[1].strip()
            else:
                loading = route.strip()

        # --- Обработка Volume ---
        volume_clean = ""
        if volume:
            vol = volume.replace(" ", "")
            if any(char.isdigit() for char in vol):
                if "-" in vol:
                    parts = re.split(r'[-–—]', vol)
                    try:
                        parts = [int(float(p)) for p in parts]
                        avg = int(sum(parts) / len(parts))
                        volume_clean = f"{avg}" 
                    except ValueError:
                        volume_clean = ""
                else:
                    digits_only = re.sub(r'[^\d]', '', vol)
                    if digits_only:
                        volume_clean = digits_only 
                    else:
                        volume_clean = ""
            else:
                volume_clean = ""
        else:
            volume_clean = ""

        # Обработка Rate Change
        rate_change_clean = rate_change if rate_change and rate_change.lower() not in ['n/a', 'nan'] else ""

        # Добавляем запись
        final_data.append({
            "Publish Date": publish_date,
            "Agency": agency,
            "Product": product,
            "Loading": loading,
            "Destination": destination,
            "Volume": volume_clean,
            "Rate Low": "",  
            "Rate High": "",
            "Rate change": rate_change_clean
        })
# ======================================
# Парсинг Dry bulk fertilizer freight assessments
# ======================================
def parse_dry_bulk_freight(df, final_data, agency, product, publish_date, file_name_short):
    print("[INFO] Начинаем парсить Dry bulk fertilizer freight assessments...")
    
    # 1. Находим начало таблицы по названию
    start_row = -1
    for i, row in df.iterrows():
        if isinstance(row[0], str) and "Dry bulk fertilizer freight assessments" in row[0]:
            start_row = i
            break
    
    if start_row == -1:
        print("[ERROR] Не найдена таблица 'Dry bulk fertilizer freight assessments'")
        return
    
    # 2. Ищем колонку "Loading" в следующих строках
    loading_col = -1
    destination_col = -1
    volume_col = -1
    rate_low_col = -1
    rate_high_col = -1
    
    # Проверяем следующие 3 строки после заголовка таблицы
    for i in range(start_row + 1, start_row + 4):
        if i >= len(df):
            break
            
        row = df.iloc[i]
        for col_idx, cell in enumerate(row):
            if isinstance(cell, str):
                cell_clean = cell.strip().lower()
                if "loading" in cell_clean:
                    loading_col = col_idx
                elif "destination" in cell_clean:
                    destination_col = col_idx
                elif "ooot" in cell_clean or "volume" in cell_clean:
                    volume_col = col_idx
                elif "rate ($/t) low" in cell_clean or "rate low" in cell_clean:
                    rate_low_col = col_idx
                elif "rate ($/t) high" in cell_clean or "rate high" in cell_clean:
                    rate_high_col = col_idx
        
        # Если нашли все нужные колонки, запоминаем строку с заголовками
        if loading_col != -1 and destination_col != -1 and volume_col != -1 and rate_low_col != -1 and rate_high_col != -1:
            header_row = i
            break
    
    if loading_col == -1:
        print("[ERROR] Не найдена колонка 'Loading'")
        return
    
    # 3. Парсим данные
    empty_rows = 0
    for i in range(header_row + 1, len(df)):
        row = df.iloc[i]
        
        # Проверяем второй столбец (Destination) на пустоту
        if destination_col < len(row) and (pd.isna(row[destination_col]) or str(row[destination_col]).strip() == ""):
            empty_rows += 1
            if empty_rows >= 3:
                break
            continue
        
        empty_rows = 0
        
        # Получаем данные из строки
        loading = str(row[loading_col]).strip() if loading_col < len(row) and pd.notna(row[loading_col]) else ""
        destination = str(row[destination_col]).strip() if destination_col < len(row) and pd.notna(row[destination_col]) else ""
        volume = str(row[volume_col]).strip() if volume_col < len(row) and pd.notna(row[volume_col]) else ""
        rate_low = str(row[rate_low_col]).strip() if rate_low_col < len(row) and pd.notna(row[rate_low_col]) else ""
        rate_high = str(row[rate_high_col]).strip() if rate_high_col < len(row) and pd.notna(row[rate_high_col]) else ""

        # --- Rate Low обработка ---
        try:
            rate_low_clean = float(rate_low) if rate_low and rate_low.lower() not in ['n/a', 'nan'] else ""
        except ValueError:
            rate_low_clean = ""

        # --- Rate High обработка ---
        try:
            rate_high_clean = float(rate_high) if rate_high and rate_high.lower() not in ['n/a', 'nan'] else ""
        except ValueError:
            rate_high_clean = ""

        # --- Volume обработка по новым правилам ---
        volume_clean = ""
        if volume:
            vol = str(volume).strip().replace(' ', '')  # Убираем пробелы

            # Проверяем, есть ли цифры вообще
            if any(char.isdigit() for char in vol):
                # Заменяем запятую на точку для унификации
                vol = vol.replace(',', '.')

                # Проверка формата с . или , и тремя нулями после
                decimal_pattern = re.compile(r'^\d+[.,]\d{3}$')
                if decimal_pattern.match(vol):
                    # Оставляем как есть, убираем точку/запятую
                    volume_clean = vol.replace('.', '').replace(',', '')
                elif '-' in vol:
                    # Диапазон: вычисляем среднее и добавляем 000
                    parts = re.split(r'[-–—]', vol)
                    try:
                        parts = [int(float(p)) for p in parts]
                        avg = int(sum(parts) / len(parts))
                        volume_clean = f"{avg}000"
                    except ValueError:
                        # Если не удалось распарсить числа
                        volume_clean = ""
                else:
                    # Простое число: убираем всё кроме цифр и добавляем 000
                    digits_only = re.sub(r'[^\d]', '', vol)
                    if digits_only:
                        volume_clean = digits_only + "000"
                    else:
                        volume_clean = ""
            else:
                # Нет цифр — оставляем пустым
                volume_clean = ""
        else:
            volume_clean = ""

        # Добавляем запись с метаданными
        final_data.append({
            "Publish Date": publish_date,
            "Agency": agency,
            "Product": product,
            "Loading": loading,
            "Destination": destination,
            "Volume": volume_clean,
            "Rate Low": rate_low_clean,
            "Rate High": rate_high_clean,             
            "Rate change": ""
        })

# ======================================
# Парсинг Urea freight
# ======================================
def parse_urea_freight(df, final_data, agency, product, publish_date):
    print("[INFO] Начинаем парсить таблицу 'Urea freight'...")

    # 1. Находим начало таблицы по заголовку
    start_row = -1
    for i, row in df.iterrows():
        if isinstance(row[0], str) and "Urea freight" in row[0]:
            start_row = i
            break

    if start_row == -1:
        print("[ERROR] Не найдена таблица 'Urea freight'")
        return

    # 2. Ищем нужные колонки
    loading_col = -1
    destination_col = -1
    tonnage_col = -1
    rate_low_col = -1
    rate_high_col = -1

    # Проверяем следующие 3 строки после заголовка
    for i in range(start_row + 1, start_row + 4):
        if i >= len(df):
            break

        row = df.iloc[i]
        for col_idx, cell in enumerate(row):
            if isinstance(cell, str):
                cell_clean = cell.strip().lower()
                if "loading" in cell_clean:
                    loading_col = col_idx
                elif "destination" in cell_clean:
                    destination_col = col_idx
                elif "tonnage" in cell_clean or "volume" in cell_clean:
                    tonnage_col = col_idx
                elif "rate ($/t) low" in cell_clean or "low" in cell_clean:
                    rate_low_col = col_idx
                elif "rate ($/t) high" in cell_clean or "high" in cell_clean:
                    rate_high_col = col_idx

        # Если нашли все нужные колонки, запоминаем заголовочную строку
        if (loading_col != -1 and destination_col != -1 and tonnage_col != -1 and
            rate_low_col != -1 and rate_high_col != -1):
            header_row = i
            break

    if loading_col == -1:
        print("[ERROR] Не найдена колонка 'Loading'")
        return

    # 3. Парсим данные
    empty_rows = 0
    for i in range(header_row + 1, len(df)):
        row = df.iloc[i]

        # Проверяем Destination на пустоту
        if destination_col < len(row) and (pd.isna(row[destination_col]) or str(row[destination_col]).strip() == ""):
            empty_rows += 1
            if empty_rows >= 3:
                break
            continue

        empty_rows = 0

        # Получаем значения ячеек
        loading = str(row[loading_col]).strip() if loading_col < len(row) and pd.notna(row[loading_col]) else ""
        destination = str(row[destination_col]).strip() if destination_col < len(row) and pd.notna(row[destination_col]) else ""
        tonnage = str(row[tonnage_col]).strip() if tonnage_col < len(row) and pd.notna(row[tonnage_col]) else ""
        rate_low = str(row[rate_low_col]).strip() if rate_low_col < len(row) and pd.notna(row[rate_low_col]) else ""
        rate_high = str(row[rate_high_col]).strip() if rate_high_col < len(row) and pd.notna(row[rate_high_col]) else ""

        # --- Tonnage -> Volume ---
        volume_clean = ""
        if tonnage:
            vol = str(tonnage).strip().replace(' ', '')  # Убираем пробелы

            # Проверяем, есть ли цифры вообще
            if any(char.isdigit() for char in vol):
                # Заменяем запятую на точку
                vol = vol.replace(',', '.')

                decimal_pattern = re.compile(r'^\d+[.,]\d{3}$')
                if decimal_pattern.match(vol):
                    volume_clean = vol.replace('.', '').replace(',', '')
                elif '-' in vol:
                    parts = re.split(r'[-–—]', vol)
                    try:
                        parts = [int(float(p)) for p in parts]
                        avg = int(sum(parts) / len(parts))
                        volume_clean = f"{avg}000"
                    except ValueError:
                        volume_clean = ""
                else:
                    digits_only = re.sub(r'[^\d]', '', vol)
                    if digits_only:
                        volume_clean = digits_only + "000"
                    else:
                        volume_clean = ""
            else:
                volume_clean = ""
        else:
            volume_clean = ""

        # --- Rate Low ---
        try:
            rate_low_clean = float(rate_low) if rate_low and rate_low.lower() not in ['n/a', 'nan'] else ""
        except ValueError:
            rate_low_clean = ""

        # --- Rate High ---
        try:
            rate_high_clean = float(rate_high) if rate_high and rate_high.lower() not in ['n/a', 'nan'] else ""
        except ValueError:
            rate_high_clean = ""

        # Добавляем запись
        final_data.append({
            "Publish Date": publish_date,
            "Agency": agency,
            "Product": product,
            "Loading": loading,
            "Destination": destination,
            "Volume": volume_clean,
            "Rate Low": rate_low_clean,
            "Rate High": rate_high_clean,
            "Rate change": ""
        })

# ======================================
# Парсинг Phosphate freight
# ======================================
def parse_phosphate_freight(df, final_data, agency, product, publish_date):
    print("[INFO] Начинаем парсить таблицу 'Phosphate freight'...")

    # 1. Находим начало таблицы по заголовку
    start_row = -1
    for i, row in df.iterrows():
        if isinstance(row[0], str) and "Phosphate freigh" in row[0]:
            start_row = i
            break

    if start_row == -1:
        print("[ERROR] Не найдена таблица 'Phosphate freigh'")
        return

    # 2. Ищем нужные колонки
    loading_col = -1
    destination_col = -1
    tonnage_col = -1
    rate_combined_col = -1  # Для Rate ($/£) Low/High

    # Проверяем следующие 3 строки после заголовка
    found = False
    for i in range(start_row + 1, start_row + 4):
        if i >= len(df):
            break

        row = df.iloc[i]
        for col_idx, cell in enumerate(row):
            if isinstance(cell, str):
                cell_clean = cell.strip().lower()
                if "loading" in cell_clean:
                    loading_col = col_idx
                elif "destination" in cell_clean:
                    destination_col = col_idx
                elif "tonnage" in cell_clean or "volume" in cell_clean:
                    tonnage_col = col_idx
                elif "rate" in cell_clean and ("low" in cell_clean or "high" in cell_clean):
                    rate_combined_col = col_idx

        # Проверяем, нашли ли все нужные колонки
        if (loading_col != -1 and destination_col != -1 and tonnage_col != -1 and
            rate_combined_col != -1):
            header_row = i
            found = True
            break

    if not found:
        print("[ERROR] Не найдены необходимые колонки для таблицы 'Phosphate freigh'")
        return

    # 3. Парсим данные
    empty_rows = 0
    for i in range(header_row + 1, len(df)):
        row = df.iloc[i]

        # Проверяем Destination на пустоту
        if destination_col < len(row) and (pd.isna(row[destination_col]) or str(row[destination_col]).strip() == ""):
            empty_rows += 1
            if empty_rows >= 3:
                break
            continue

        empty_rows = 0

        # Получаем значения ячеек
        loading = str(row[loading_col]).strip() if loading_col < len(row) and pd.notna(row[loading_col]) else ""
        destination = str(row[destination_col]).strip() if destination_col < len(row) and pd.notna(row[destination_col]) else ""
        tonnage = str(row[tonnage_col]).strip() if tonnage_col < len(row) and pd.notna(row[tonnage_col]) else ""
        rate_combined = str(row[rate_combined_col]).strip() if rate_combined_col < len(row) and pd.notna(row[rate_combined_col]) else ""

        # --- Tonnage -> Volume ---
        volume_clean = ""
        if tonnage:
            vol = str(tonnage).strip().replace(' ', '')  # Убираем пробелы

            # Проверяем, есть ли цифры вообще
            if any(char.isdigit() for char in vol):
                # Заменяем запятую на точку
                vol = vol.replace(',', '.')

                decimal_pattern = re.compile(r'^\d+[.,]\d{3}$')
                if decimal_pattern.match(vol):
                    volume_clean = vol.replace('.', '').replace(',', '')
                elif '-' in vol:
                    parts = re.split(r'[-–—]', vol)
                    try:
                        parts = [int(float(p)) for p in parts]
                        avg = int(sum(parts) / len(parts))
                        volume_clean = f"{avg}000"
                    except ValueError:
                        volume_clean = ""
                else:
                    digits_only = re.sub(r'[^\d]', '', vol)
                    if digits_only:
                        volume_clean = digits_only + "000"
                    else:
                        volume_clean = ""
            else:
                volume_clean = ""
        else:
            volume_clean = ""

        # --- Rate Combined -> Rate Low / Rate High ---
        rate_low_clean = ""
        rate_high_clean = ""

        if rate_combined:
            # Пробуем разделить строку на Low и High
            rates = re.findall(r'(\d+\.?\d*)', rate_combined)
            if len(rates) >= 2:
                try:
                    rate_low_clean = float(rates[0])
                    rate_high_clean = float(rates[1])
                except ValueError:
                    rate_low_clean = ""
                    rate_high_clean = ""
            else:
                try:
                    rate_single = float(rates[0]) if rates else ""
                    rate_low_clean = rate_single
                    rate_high_clean = rate_single
                except (ValueError, IndexError):
                    rate_low_clean = ""
                    rate_high_clean = ""

        # Добавляем запись
        final_data.append({
            "Publish Date": publish_date,
            "Agency": agency,
            "Product": product,
            "Loading": loading,
            "Destination": destination,
            "Volume": volume_clean,
            "Rate Low": rate_low_clean,
            "Rate High": rate_high_clean,
            "Rate change": ""
        })

# ======================================
# Парсинг Potash freight
# ======================================
def parse_potash_freight(df, final_data, agency, product, publish_date):
    print("[INFO] Начинаем парсить таблицу 'Potash freight'...")
    
    # 1. Находим начало таблицы по заголовку (регистр не важен)
    start_row = -1
    for i, row in df.iterrows():
        if any(cell for cell in row if isinstance(cell, str) and "potash freight" in cell.lower()):
            start_row = i
            break
    if start_row == -1:
        print("[ERROR] Не найдена таблица 'Potash freight'")
        return

    # 2. Ищем нужные колонки: Loading, Destination, MOP ooot, Rate (справа от MOP ooot)
    loading_col = -1
    destination_col = -1
    volume_col = -1
    rate_col = -1

    # Проверяем следующие несколько строк после заголовка для определения колонок
    found = False
    for i in range(start_row + 1, start_row + 5):
        if i >= len(df):
            break
        row = df.iloc[i]
        for col_idx, cell in enumerate(row):
            if isinstance(cell, str):
                cell_clean = cell.strip().lower()
                if "loading" in cell_clean:
                    loading_col = col_idx
                elif "destination" in cell_clean:
                    destination_col = col_idx
                elif "mop ooot" in cell_clean or "volume" in cell_clean:
                    volume_col = col_idx
        # Если нашли MOP ooot, то следующий столбец — Rate
        if volume_col != -1:
            rate_col = volume_col + 1
        # Если все колонки найдены, запоминаем заголовочную строку
        if loading_col != -1 and destination_col != -1 and volume_col != -1 and rate_col != -1:
            header_row = i
            found = True
            break

    if not found:
        print("[ERROR] Не найдены все необходимые колонки для таблицы 'Potash freight'")
        return

    # 3. Парсим данные
    empty_rows = 0
    for i in range(header_row + 1, len(df)):
        row = df.iloc[i]

        # Пропускаем строки, где во втором столбце (Destination) пусто
        if destination_col < len(row) and (pd.isna(row[destination_col]) or str(row[destination_col]).strip() == ""):
            empty_rows += 1
            if empty_rows >= 3:
                break
            continue
        empty_rows = 0

        # Получаем значения ячеек
        loading = str(row[loading_col]).strip() if loading_col < len(row) and pd.notna(row[loading_col]) else ""
        destination = str(row[destination_col]).strip() if destination_col < len(row) and pd.notna(row[destination_col]) else ""
        mop_volume = str(row[volume_col]).strip() if volume_col < len(row) and pd.notna(row[volume_col]) else ""
        rate_value = str(row[rate_col]).strip() if rate_col < len(row) and pd.notna(row[rate_col]) else ""

        # --- Обработка Volume ---
        volume_clean = ""
        if mop_volume:
            vol = mop_volume.replace(" ", "")
            if any(char.isdigit() for char in vol):
                if "-" in vol:
                    parts = re.split(r'[-–—]', vol)
                    try:
                        parts = [int(float(p)) for p in parts]
                        avg = int(sum(parts) / len(parts))
                        volume_clean = f"{avg}000"
                    except ValueError:
                        volume_clean = ""
                else:
                    digits_only = re.sub(r'[^\d]', '', vol)
                    if digits_only:
                        volume_clean = digits_only + "000"
                    else:
                        volume_clean = ""
            else:
                volume_clean = ""
        else:
            volume_clean = ""

        # --- Обработка Rate ---
        rate_low_clean = ""
        rate_high_clean = ""
        if rate_value:
            rates = re.findall(r'(\d+\.?\d*)', rate_value)
            if len(rates) >= 2:
                try:
                    rate_low_clean = float(rates[0])
                    rate_high_clean = float(rates[1])
                except ValueError:
                    rate_low_clean = ""
                    rate_high_clean = ""
            else:
                try:
                    single_rate = float(rates[0]) if rates else ""
                    rate_low_clean = single_rate
                    rate_high_clean = single_rate
                except (ValueError, IndexError):
                    rate_low_clean = ""
                    rate_high_clean = ""

        # Добавляем запись
        final_data.append({
            "Publish Date": publish_date,
            "Agency": agency,
            "Product": product,
            "Loading": loading,
            "Destination": destination,
            "Volume": volume_clean,
            "Rate Low": rate_low_clean,
            "Rate High": rate_high_clean,
            "Rate change": ""
        })
# ======================================
# Основной цикл парсинга
# ======================================
for file_info in FILES:
    file_path = file_info["path"]
    tables_to_parse = file_info["tables"]
    print(f"[INFO] Загружаем файл: {file_path}")
    
    try:
        df = pd.read_excel(file_path, header=None, engine='openpyxl')
    except Exception as e:
        print(f"[ERROR] Ошибка при загрузке файла: {e}")
        continue

    file_name = os.path.basename(file_path).replace('.xlsx', '')
    first_part = file_name.split('_')[0].strip() if '_' in file_name else file_name
    parts = first_part.split()

    agency = parts[0] if len(parts) >= 1 else ''
    product = ' '.join(parts[1:]) if len(parts) >= 2 else ''
    
    publish_date = extract_publish_date(file_name)
    file_name_short = os.path.basename(file_path)

    if "Ammonia freight rates" in tables_to_parse:
        parse_ammonia_freight_rates(df, final_data, agency, product, publish_date)
    if "Dry bulk fertilizer freight assessments" in tables_to_parse:
        parse_dry_bulk_freight(df, final_data, agency, product, publish_date, file_name_short)
    if "Urea freight" in tables_to_parse:
        parse_urea_freight(df, final_data, agency, product, publish_date)
    if "Phosphate freigh" in tables_to_parse:
        parse_phosphate_freight(df, final_data, agency, product, publish_date)
    if "Potash freight" in tables_to_parse:
        parse_potash_freight(df, final_data, agency, product, publish_date)
# ======================================
# Сохраняем результат в Excel
# ======================================
if final_data:
    # Определяем порядок колонок (3 мета-колонки слева + 5 колонок данных)
    columns_order = [
        "Publish Date", 
        "Agency", 
        "Product",
        "Loading", 
        "Destination", 
        "Volume", 
        "Rate Low", 
        "Rate High",
        "Rate change"
    ]
    
    result_df = pd.DataFrame(final_data, columns=columns_order)
    output_file = 'freight_processed.xlsx'
    result_df.to_excel(output_file, index=False)
    print(f"✅ Данные успешно обработаны и сохранены в '{output_file}'")
    print(f"Обработано записей: {len(final_data)}")
else:
    print("⚠️ Не найдено данных для сохранения")
