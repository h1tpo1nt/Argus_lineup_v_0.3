import pandas as pd
from datetime import datetime
import re

# Путь к файлу
file_path = 'your_file.xlsx'

# Загружаем данные
df = pd.read_excel(file_path, header=None)

# Результат будет собираться здесь
result_rows = []

# Поиск начала таблицы "Ammonia prices"
start_row = None
dates = []
incoterm_section = None  # fob или cfr

for i, row in df.iterrows():
    cell_value = str(row[0]).strip()

    # Начало таблицы
    if cell_value.lower() == "ammonia prices":
        start_row = i + 1
        continue

    if start_row is None:
        continue

    # Если дошли до конца файла
    if i >= len(df):
        break

    # Ищем даты выше слова "fob"
    if cell_value.lower() == "fob":
        incoterm_section = "FOB"
        continue

    if cell_value.lower() == "cfr":
        incoterm_section = "CFR"
        continue

    # Если еще не нашли даты, ищем их
    if not dates and incoterm_section is None:
        match = re.search(r'(\d{1,2})\s([A-Za-z]{3})', cell_value)
        if match:
            day = match.group(1)
            month_abbr = match.group(2).capitalize()
            try:
                date_str = f"{day} {month_abbr}"
                parsed_date = datetime.strptime(date_str, "%d %b").strftime("%d.%m")
                dates.append(parsed_date)
            except ValueError:
                pass

    # Теперь начинаем парсить данные под FOB или CFR
    if incoterm_section:
        location = cell_value
        low_high = str(row[1]).strip() if 1 < len(row) else ""
        mid = str(row[2]).strip() if 2 < len(row) else ""

        # Чистка значений
        def clean_value(val):
            val = val.lower()
            if val in ["-", "na", "n/a"]:
                return ""
            return val

        low_high = clean_value(low_high)
        mid = clean_value(mid)

        # Классификация Incoterms
        incoterm = ""
        if incoterm_section == "FOB":
            if location in [
                "Baltic",
                "Pivdenny",
                "North Africa",
                "Middle East",
                "Middle East spot",
                "Middle East contract",
                "US Gulf domestic (barge) $/st",
                "Caribbean",
                "US Gulf",
                "SE Asia and Australia",
                "SE Asia and Australia spot",
                "SE Asia and Australia contract"
            ]:
                incoterm = "FOB"

        elif incoterm_section == "CFR":
            if location in [
                "NW Europe (duty unpaid)",
                "NW Europe (duty paid/free)",
                "NW Europe weekly indext Turkey Morocco India India spot India contract East Asia (excl Taiwan)",
                "East Asia (excl Taiwan) spot",
                "East Asia (excl Taiwan) contract",
                "Taiwan",
                "China",
                "ex-works Jiangsu Yn/t",
                "Tampa",
                "US Gulf"
            ]:
                incoterm = "CFR"

            elif location in [
                "NW Europe Plus Carbon-Price Adjustment (assumes no free credits)",
                "NW Europe Plus Carbon-Price Adjustment (assumes free credits)"
            ]:
                incoterm = "Carbon-Adjusted Price of Ammonia (CAPA)"

            elif location in [
                "Ammonia low-C cfr Ulsan (JKLAB) excl US 45Qtax credit",
                "Ammonia low-C cfr Ulsan (JKLAB) inc US 45Qtax credit",
                "Gas carrier ammonia Niihama (Ulsan basis) diff to cfr Ulsan"
            ]:
                incoterm = "JKLAB"

            elif location in [
                "Henry hub $/mn Btu",
                "TTF month ahead $/mn Btu",
                "Ammonia cost of production (TTF)"
            ]:
                incoterm = "Natural gas"

        # Добавляем строку только если есть что-то кроме пустых значений
        if location or low_high or mid or incoterm:
            result_rows.append({
                "Location": location,
                "Low-High": low_high,
                "Mid": mid,
                "Incoterms": incoterm
            })

# Создаем результирующий DataFrame
result_df = pd.DataFrame(result_rows)

# Добавляем шапку с датами
if len(dates) >= 2:
    result_df.columns = pd.MultiIndex.from_tuples([
        ("", "Location"),
        (dates[0], "Low-High"),
        (dates[0], "Mid"),
        (dates[1], "Low-High"),
        (dates[1], "Mid"),
        ("", "Incoterms")
    ])

# Сохраняем результат
output_path = "processed_ammonia_prices.xlsx"
result_df.to_excel(output_path, index=False, header=True)

print(f"Файл успешно обработан и сохранен как {output_path}")
