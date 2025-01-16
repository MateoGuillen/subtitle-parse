from utils.csv_utility import CSVUtility
import os

def main():
    outline_pdf_root_dir = "./outline_pdfs/"
    output_dir = "./outputs/"
    combined_output_file = f"{output_dir}combined_all_categories.csv"
    total_categories = 2  

    CSVUtility.ensure_directory_exists(output_dir)

    for category in range(1, total_categories + 1):
        category_str = str(category) 
        output_file = f'{output_dir}combined_category_{category_str}.csv'
        print(f"Procesando categoría {category_str}...")

        CSVUtility.combine_csv_by_category(outline_pdf_root_dir, output_file, category_str)
        print(f"Categoría {category_str} procesada y guardada en {output_file}.\n")

    CSVUtility.combine_all_categories_excluding_duplicates(output_dir, combined_output_file, total_categories)
    print(f"Todas las categorías combinadas y sin duplicados se guardaron en {combined_output_file}")

if __name__ == "__main__":
    main()
