def get_warc_file_path() -> list:
    with open('cc-index.paths', 'r') as file:
        lines = [line.strip() for line in file.readlines()]
        return lines