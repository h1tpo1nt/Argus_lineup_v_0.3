import pandas as pd
import re
from datetime import datetime
import os

# ======================================
# Настройки путей и параметров
# ======================================
file_path = '/content/Argus_Ammonia_test.xlsx'  # заменить на свой путь
file_name = os.path.basename(file_path).replace('.xlsx', '')

# Извлекаем Agency и Product из названия файла
file_parts = file_name.split('_')
agency = file_parts[0].strip()
product = ' '.join(file_parts[1:]).split(' ')[0].strip() if len(file_parts) > 1 else ''

# Загружаем данные без заголовков
df = pd.read_excel(file_path, header=None)

# Результат будем собирать здесь
final_data = []

def parse_date(date_str):
    """
    Парсит дату по новым правилам:
    - Если просто месяц: 1 число месяца
    - Если mid/early: 15 число
    - Если end: 30 число
    - Если указан конкретный день: используем его
    """
    if not date_str:
        return ""
    
    # Приводим к нижнему регистру для удобства
    date_str_lower = date_str.lower()
    
    # Определяем день месяца по ключевым словам
    if re.search(r'\bmid\b|\bearly\b|\bme?i?d\b|\bear?ly\b', date_str_lower):
        day = 15
    elif re.search(r'\bend\b|\ben?d\b', date_str_lower):
        day = 30
    else:
        # Ищем конкретный день
        day_match = re.search(r'\b(\d{1,2})\b', date_str)
        day = int(day_match.group(1)) if day_match else 1  # По умолчанию 1 число
    
    # Ищем месяц
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

# =================================

# ======================================
# Парсинг таблицы Indian imports
# ======================================
start_parsing_indian = False
print("[INFO] Начинаем парсить Indian imports...")

for i, row in df.iterrows():
    first_cell = str(row[0]).strip() if not pd.isna(row[0]) else ""

    # Пропускаем пустые строки
    if not first_cell:
        continue

    # Если начали парсить и встретили "Copyright" — завершаем парсинг
    if start_parsing_indian and any(keyword in first_cell.lower() for keyword in ['copyright', 'лицензия']):
        print("Найдена строка 'Copyright' — завершаем парсинг Indian imports.")
        break

    # Нашли "Indian imports"
    if re.search(r'indian\s*imports', first_cell, re.IGNORECASE):
        start_parsing_indian = True
        continue

    # Если начали парсить и нашли "Seller" — это заголовок
    if start_parsing_indian and first_cell == "Seller":
        continue

    # Пропускаем строки с месяцами
    if start_parsing_indian:
        month_match = re.search(r'(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)', first_cell.lower())
        if month_match:
            continue

    # Парсим данные для Indian imports
    if start_parsing_indian and first_cell:
        seller = first_cell
        buyer = str(row[1]).strip() if 1 < len(row) and not pd.isna(row[1]) else ""
        vessel = str(row[2]).strip() if 2 < len(row) and not pd.isna(row[2]) else ""
        vol_origin = str(row[3]).strip() if 3 < len(row) and not pd.isna(row[3]) else ""
        date_port = str(row[4]).strip() if 4 < len(row) and not pd.isna(row[4]) else ""
        price = str(row[5]).strip() if 5 < len(row) and not pd.isna(row[5]) else ""

        # Парсим Volume и Origin
        volume = ""
        origin = ""
        if vol_origin:
            vol_match = re.match(r'^([\d,]+)\s*(.*)$', vol_origin)
            if vol_match:
                volume = vol_match.group(1).replace(',', '')
                origin = vol_match.group(2).strip()
            else:
                origin = vol_origin

        # Парсим Date и Discharge port
        date_str = parse_date(date_port)
        discharge_port = ""
        if date_port:
            # Полностью очищаем строку от цифр, тире и месяцев
            discharge_port = re.sub(
                r'\d{1,2}\s*-*\s*|'  # Удаляем цифры и тире перед ними
                r'\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\w*\b|'
                r'\b(mid|early|end)\b|'
                r'\bjune\b|\bjuly\b|\baugust\b|\bseptember\b|\boctober\b|\bnovember\b|\bdecember\b',
                '', date_port, flags=re.IGNORECASE
            ).strip()
            # Удаляем оставшиеся тире и пробелы
            discharge_port = re.sub(r'^-+\s*|\s*-+\s*$', '', discharge_port).strip()
            # Удаляем все цифры, если остались
            discharge_port = re.sub(r'\d+', '', discharge_port).strip()
            # Окончательная очистка
            discharge_port = discharge_port.lstrip('-').strip()

        # Цена — только числа
        price_clean = ""
        price_match = re.search(r'([\d\.]+)', price)
        if price_match:
            price_clean = price_match.group(1)

        # Добавляем в результат
        final_data.append({
            "Agency": agency,
            "Product": product,
            "Seller": seller,
            "Buyer": buyer,
            "Vessel": vessel,
            "Volume (t)": volume,
            "Origin": origin,
            "Date of arrival": date_str,
            "Discharge port": discharge_port,
            "Price": price_clean,
            "Incoterm": "",
            "Destination": ""
        })

# ======================================
# Парсинг таблицы Spot Sales
# ======================================
start_parsing_spot = False
print("[INFO] Переходим к парсингу Spot Sales...")

for i, row in df.iterrows():
    first_cell = str(row[0]).strip() if not pd.isna(row[0]) else ""

    # Пропускаем пустые строки
    if not first_cell:
        continue

    # Нашли "Spot sales"
    if re.search(r'spot\s*sales', first_cell, re.IGNORECASE):
        start_parsing_spot = True
        continue

    # Если начали парсить и нашли "Shipment" — это заголовок
    if start_parsing_spot and first_cell == "Shipment":
        continue

    # Если начали парсить и встретили "Copyright" — завершаем
    if start_parsing_spot and any(keyword in first_cell.lower() for keyword in ['copyright', 'лицензия']):
        print("Найдена строка 'Copyright' — завершаем парсинг Spot Sales")
        break


    # Парсим данные для Spot Sales (ТОЛЬКО ЕСЛИ ДОСТАТОЧНО КОЛОНОК)
    if start_parsing_spot and first_cell and len(row) > 6:
        shipment = first_cell
        seller = str(row[1]).strip() if not pd.isna(row[1]) else ""
        buyer = str(row[2]).strip() if not pd.isna(row[2]) else ""
        destination_val = str(row[3]).strip() if not pd.isna(row[3]) else ""
        tonnes = str(row[4]).strip() if not pd.isna(row[4]) else ""
        price_incoterm = str(row[5]).strip() if not pd.isna(row[5]) else ""
        origin_value = str(row[6]).strip() if not pd.isna(row[6]) else ""

        # Парсим Date из Shipment с использованием новой функции
        date_str = parse_date(shipment)

        # Парсим Volume (t) - убираем запятые в числах
        volume = ""
        if tonnes:
            vol_match = re.search(r'([\d,]+)', tonnes)
            if vol_match:
                volume = vol_match.group(1).replace(',', '')

        # Парсим Price и Incoterm
        price_clean = ""
        incoterm = ""
        if price_incoterm:
            price_match = re.search(r'([\d\.,]+)', price_incoterm)
            if price_match:
                price_clean = price_match.group(1).replace(',', '')

            incoterm_match = re.search(
                r'(fob|cfr|cif|fca|dap|cpt|c\w+?r|rail|exw|ddp|dpu|d\w+?p|f\w+?t|c\w+?y)',
                price_incoterm,
                re.IGNORECASE
            )
            if incoterm_match:
                incoterm = incoterm_match.group().upper()

        # Обрабатываем Origin - берем только часть до / или -
        origin_processed = origin_value.split('/')[0].split('-')[0].strip()

        # Добавляем в результат
        final_data.append({
            "Agency": agency,
            "Product": product,
            "Seller": seller,
            "Buyer": buyer,
            "Vessel": "",
            "Volume (t)": volume,
            "Origin": origin_processed,  # Используем обработанное значение
            "Date of arrival": date_str,
            "Discharge port": "",
            "Price": price_clean,
            "Incoterm": incoterm,
            "Destination": destination_val
        })


# ======================================
# Создаём DataFrame и сохраняем результат
# ======================================
columns_order = [
    "Agency", "Product", "Seller", "Buyer", "Vessel",
    "Volume (t)", "Origin", "Date of arrival", "Discharge port", "Price", "Incoterm", "Destination"
]
result_df = pd.DataFrame(final_data, columns=columns_order)

output_file = 'processed_combined.xlsx'
result_df.to_excel(output_file, index=False)

print(f"✅ Файл успешно обработан и сохранён как '{output_file}'")
