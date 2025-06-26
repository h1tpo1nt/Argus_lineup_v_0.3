import pandas as pd
import numpy as np

def find_table_after_phrase(excel_file, phrase, sheet_name=None, skip_rows_after_phrase=0, min_columns=3):
    """
    Ищет таблицу после указанной фразы в Excel-файле.
    
    Параметры:
    - excel_file: путь к Excel-файлу
    - phrase: фраза, после которой искать таблицу
    - sheet_name: название листа (если None, ищет во всех листах)
    - skip_rows_after_phrase: сколько строк пропустить после фразы
    - min_columns: минимальное количество заполненных колонок для строки таблицы
    
    Возвращает:
    - DataFrame с найденной таблицей или None, если таблица не найдена
    """
    
    def process_sheet(sheet):
        # Читаем весь лист как строки
        data = pd.read_excel(excel_file, sheet_name=sheet, header=None)
        
        # Ищем фразу в ячейках
        phrase_found = False
        start_row = 0
        
        for i, row in data.iterrows():
            if phrase_found:
                # После нахождения фразы пропускаем указанное количество строк
                if i < start_row + skip_rows_after_phrase + 1:
                    continue
                
                # Проверяем, есть ли в строке достаточно данных для таблицы
                if row.count() >= min_columns:
                    # Нашли начало таблицы
                    table_start = i
                    # Ищем конец таблицы
                    table_end = table_start
                    empty_rows = 0
                    
                    # Проверяем следующие строки
                    for j in range(i+1, len(data)):
                        next_row = data.iloc[j]
                        # Если в строке достаточно данных - это продолжение таблицы
                        if next_row.count() >= min_columns:
                            table_end = j
                            empty_rows = 0
                        else:
                            empty_rows += 1
                            # Если несколько пустых строк подряд - предполагаем конец таблицы
                            if empty_rows >= 2:
                                break
                    
                    # Извлекаем таблицу
                    table = data.iloc[table_start:table_end+1]
                    
                    # Первая строка - заголовки, если она содержит текст
                    if table.iloc[0].apply(lambda x: isinstance(x, str)).any():
                        table.columns = table.iloc[0]
                        table = table[1:]
                        table = table.reset_index(drop=True)
                    
                    return table
                else:
                    # Если после фразы нет данных для таблицы, продолжаем поиск
                    continue
            
            # Поиск фразы в ячейках строки
            for cell in row:
                if isinstance(cell, str) and phrase.lower() in cell.lower():
                    phrase_found = True
                    start_row = i
                    break
        
        return None
    
    # Обработка листов
    if sheet_name:
        sheets = [sheet_name]
    else:
        sheets = pd.ExcelFile(excel_file).sheet_names
    
    results = []
    for sheet in sheets:
        table = process_sheet(sheet)
        if table is not None:
            table['Source_Sheet'] = sheet
            results.append(table)
    
    if not results:
        return None
    elif len(results) == 1:
        return results[0]
    else:
        return pd.concat(results, ignore_index=True)

# Пример использования
excel_file = "path_to_your_file.xlsx"
phrase = "Selection of recent spot sales"

# Ищем таблицу после указанной фразы, пропуская 1 строку после фразы
result_table = find_table_after_phrase(
    excel_file=excel_file,
    phrase=phrase,
    skip_rows_after_phrase=1,  # можно увеличить, если таблица начинается не сразу
    min_columns=3  # минимальное количество заполненных колонок для строки таблицы
)

if result_table is not None:
    print("Таблица найдена:")
    print(result_table)
    result_table.to_csv("extracted_table.csv", index=False)
else:
    print("Таблица не найдена")
