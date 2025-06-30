import pandas as pd
import re
from datetime import datetime
import os

# ======================================
# Настройки путей и параметров
# ======================================
file_path = 'IFFCO Ammonia.xlsx'  # заменить на свой путь
file_name = os.path.basename(file_path).replace('.xlsx', '')

# Извлекаем Agency и Product из названия файла
file_parts = file_name.split()
agency = file_parts[0]
product = ' '.join(file_parts[1:]) if len(file_parts) > 1 else ''

# Загружаем данные
df = pd.read_excel(file_path)

# Проверяем, что нужные столбцы присутствуют
required_columns = ['Seller', 'Buyer', 'Vessel', 'Volume (t) Origin', 'Date Discharge port', 'Price']
if not all(col in df.columns for col in required_columns):
    raise ValueError("Не найдены необходимые колонки в файле")

# Новый список строк
processed_data = []

for _, row in df.iterrows():
    # Парсим "Volume (t) Origin"
    vol_origin = str(row['Volume (t) Origin']).strip()
    vol_match = re.match(r'^([\d,]+)\s*(.*)$', vol_origin)
    volume = vol_match.group(1) if vol_match else ""
    origin = vol_match.group(2).strip() if vol_match else vol_origin

    # Парсим "Date Discharge port"
    date_port = str(row['Date Discharge port']).strip()
    date_match = re.search(r'(\d{1,2})\s*[-\s]*([A-Za-z]{3,9})', date_port, re.IGNORECASE)
    discharge_port = date_port
    date_str = ""

    if date_match:
        day = date_match.group(1)
        month_abbr = date_match.group(2).capitalize()
        try:
            dt = datetime.strptime(f"{day} {month_abbr[:3]}", "%d %b")
            date_str = dt.strftime("%d.%m")
            discharge_port = re.sub(r'\d{1,2}\s+[A-Za-z]{3,9}', '', date_port, flags=re.IGNORECASE).strip()
        except ValueError:
            pass

    # Если даты нет, но есть слово вроде "mid July"
    if not date_str:
        month_match = re.search(r'(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)', date_port.lower())
        if month_match:
            month_num = month_match.start() // 3 + 1
            date_str = f"15.{month_num:02d}"
            discharge_port = re.sub(r'(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)',
                                    '', date_port, flags=re.IGNORECASE).strip()

    # Цена — только число
    price = ""
    price_match = re.search(r'([\d\.]+)', str(row['Price']))
    if price_match:
        price = price_match.group(1)

    # Добавляем обработанную строку
    processed_data.append({
        "Agency": agency,
        "Product": product,
        "Seller": row['Seller'],
        "Buyer": row['Buyer'],
        "Vessel": row['Vessel'],
        "Volume (t)": volume,
        "Origin": origin,
        "Date": date_str,
        "Discharge port": discharge_port,
        "Price": price
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
