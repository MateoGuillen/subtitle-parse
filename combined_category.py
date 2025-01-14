from utils.csv_utility import CSVUtility
import os


def main():
    outline_pdf_root_dir = "./outline_pdfs/"
    category = "1"
    output_file = f'./outputs/combined_category_{category}.csv'
    
    # Asegurarse de que el directorio de salida existe
    CSVUtility.ensure_directory_exists(os.path.dirname(output_file))
    
    # Combinar archivos CSV de la categoría especificada
    CSVUtility.combine_csv_by_category(outline_pdf_root_dir, output_file, category)


if __name__ == "__main__":
    main()
