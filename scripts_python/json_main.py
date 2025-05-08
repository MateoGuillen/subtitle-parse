from json_extractor import JSONExtractor

def main():
    json_path = "./inputs/test_json.json"
    extractor = JSONExtractor(json_path)
    
    extractor.load_json()
    clauses = extractor.extract_clauses()
    extractor.save_to_csv(clauses)

if __name__ == "__main__":
    main()
