import pandas as pd
import re
from datetime import datetime
import os

# ======================================
# Настройки путей и параметров
# ======================================
file_path = '/content/Argus Ammonia _ Russia version (2025-06-12).xlsx'  # заменить на свой путь
file_name = os.path.basename(file_path).replace('.xlsx', '')

# Извлекаем Agency и Product из названия файла
file_parts = file_name.split()
agency = file_parts[0]
product = ' '.join(file_parts[1:]) if len(file_parts) > 1 else ''

# Загружаем данные без заголовков
df = pd.read_excel(file_path, header=None)

# Результат будем собирать здесь
processed_data = []

# Поиск начала таблицы
start_parsing = False
for i, row in df.iterrows():
    first_cell = str(row[0]).strip() if not pd.isna(row[0]) else ""

    # Пропускаем пустые строки
    if not first_cell:
        continue

    # Останавливаем парсинг, если встретили "Copyright" или "Лицензия"
    if any(keyword in first_cell.lower() for keyword in ['copyright', 'лицензия']):
        print("Найдена строка с 'Copyright' или 'Лицензия' — завершаем парсинг.")
        break

    # Нашли "Indian imports"
    if "Indian imports" in first_cell:
        start_parsing = True
        continue

    # Если начали парсить и нашли "Seller" — это заголовок
    if start_parsing and first_cell == "Seller":
        continue  # пропускаем заголовок

    # Если начали парсить, но попалась строка с месяцем — пропускаем
    if start_parsing:
        month_match = re.search(r'(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)', first_cell.lower())
        if month_match:
            continue

    # Если начали парсить и есть данные — обрабатываем
    if start_parsing and first_cell:
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
                volume = vol_match.group(1)
                origin = vol_match.group(2).strip()
            else:
                origin = vol_origin

        # Парсим Date и Discharge port
        date_str = ""
        discharge_port = ""
        if date_port:
            # Попробуем найти дату в форматах: 31-May, 2Jun, Jun
            date_match = re.search(r'(\d{1,2})\s*([A-Za-z]{3,9})', date_port, re.IGNORECASE)  # например 31May
            if date_match:
                day = date_match.group(1)
                month_abbr = date_match.group(2).capitalize()
                try:
                    dt = datetime.strptime(f"{day} {month_abbr[:3]}", "%d %b")
                    date_str = dt.strftime("%d.%m")
                    discharge_port = re.sub(r'\d{1,2}[a-zA-Z]*\s*[A-Za-z]{3,9}', '', date_port, flags=re.IGNORECASE).strip()
                except ValueError:
                    pass

            # Если не нашли через первый паттерн, проверим вариант с "18-Jun"
            if not date_str:
                date_match = re.search(r'(\d{1,2})[-/\s]+([A-Za-z]{3,9})', date_port, re.IGNORECASE)  # например 18-Jun
                if date_match:
                    day = date_match.group(1)
                    month_abbr = date_match.group(2).capitalize()
                    try:
                        dt = datetime.strptime(f"{day} {month_abbr[:3]}", "%d %b")
                        date_str = dt.strftime("%d.%m")
                        discharge_port = re.sub(r'\d{1,2}\s*[\/\-]*\s*[A-Za-z]{3,9}', '', date_port, flags=re.IGNORECASE).strip()
                    except ValueError:
                        pass

            # Если всё ещё нет даты — проверяем формат только с месяцем, например "June Sikka"
            if not date_str:
                month_match = re.search(r'\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\b', date_port.lower())
                if month_match:
                    month_num = ['jan', 'feb', 'mar', 'apr', 'may', 'jun',
                                 'jul', 'aug', 'sep', 'oct', 'nov', 'dec'].index(month_match.group().lower()) + 1
                    date_str = f"15.{month_num:02d}"
                    discharge_port = re.sub(r'\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\b',
                                          '', date_port, flags=re.IGNORECASE).strip()

            # Если ничего не подошло — пытаемся извлечь только название порта из строки
            if not date_str:
                # Убираем ключевые слова (early, end, mid, e и т.п.)
                cleaned = re.sub(r'\b(early|end|mid|e)\s+', '', date_port, flags=re.IGNORECASE).strip()
                # Извлекаем последнее слово как порт
                discharge_port = re.sub(r'^.*\s+(\w+)$', r'\1', cleaned).strip()

            # Дополнительно чистим порт от лишних символов
            discharge_port = re.sub(r'^[\d\.\-\s]+', '', discharge_port).strip()

        # Цена — только числа
        price_clean = ""
        price_match = re.search(r'([\d\.]+)', price)
        if price_match:
            price_clean = price_match.group(1)

        # Добавляем строку в результат ВСЕГДА
        processed_data.append({
            "Agency": agency,
            "Product": product,
            "Seller": seller,
            "Buyer": buyer,
            "Vessel": vessel,
            "Volume (t)": volume,
            "Origin": origin,
            "Date": date_str,
            "Discharge port": discharge_port,
            "Price": price_clean
        })

# Создаём DataFrame
columns_order = [
    "Agency", "Product", "Seller", "Buyer", "Vessel",
    "Volume (t)", "Origin", "Date", "Discharge port", "Price"
]

result_df = pd.DataFrame(processed_data, columns=columns_order)

# Сохраняем результат
output_file = 'processed_Indian_imports.xlsx'
result_df.to_excel(output_file, index=False)

print(f"Файл успешно обработан и сохранён как '{output_file}'")
