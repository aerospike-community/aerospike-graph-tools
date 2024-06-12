import sys, os, getopt, csv

def print_help():
    print("Expected arguments:\n"
          "\t'-i', '--input_directory'\tInput csv file directory. This directory should contain only neo4j export format csv files.\n"
          "\t'-o', '--output_directory'\tOutput csv file directory. This directory should be empty and is where the output files will be placed.\n")
    exit(1)


def parse_args():
    output_directory = ""
    input_directory = ""

    argv = sys.argv[1:]
    try:
        options, args = getopt.getopt(argv, "i:o:", ["input_directory=", "output_directory="])
    except:
        print_help()
        exit(1)

    for name, value in options:
        if name in ["-i", "--input_directory"]:
            input_directory = value
        elif name in ["-o", "--output_directory"]:
            output_directory = value

    if input_directory == "" or output_directory == "":
        print_help()
        exit(1)
    return input_directory, output_directory


def validate_arguments(input_directory, output_directory):
    if not os.path.exists(input_directory):
        print("Input directory '" + input_directory + "' does not exist.")
        print_help()
    elif not os.path.exists(output_directory):
        print("Output directory '" + output_directory + "' does not exist.")
        print_help()


def enumerate_input_files(input_directory):
    input_file_list = []
    for absolute_directory_path, directories, files in os.walk(os.path.abspath(input_directory)):
        for file_path in files:
            input_file_absolute_path = os.path.join(absolute_directory_path, file_path)
            input_file_list.append(input_file_absolute_path)
    return input_file_list


def parse_header(header, reserved_neo4j_headers):
    id_index = header.index("_id")
    label_index = header.index("_labels")
    end_index = header.index("_end")
    start_index = header.index("_start")
    type_index = header.index("_type")
    property_headers = []
    for header_item in header:
        if header_item not in reserved_neo4j_headers:
            property_headers.append(header_item)
    return id_index, label_index, start_index, end_index, type_index, property_headers


def find_next_file(output_file_directory, file_name, is_vertex):
    index = 0
    actual_directory = os.path.join(output_file_directory, "vertices" if is_vertex else "edges")
    if not os.path.exists(actual_directory):
        os.makedirs(actual_directory)
    valid_file_path = os.path.join(actual_directory, file_name + "_" + str(index) + ".csv")
    while os.path.exists(valid_file_path):
        index += 1
        valid_file_path = os.path.join(actual_directory, file_name + "_" + str(index) + ".csv")
    return valid_file_path


def batch_process_input_files(input_file_list, output_directory):
    for input_file in input_file_list:
        print("Currently parsing input file:" + input_file)
        with open(input_file, "r", newline='') as input_csv_file:
            csv_reader = csv.reader(input_csv_file, delimiter=",", quotechar='"')
            header = None
            id_index = label_index = start_index = end_index = type_index = None
            reserved_neo4j_headers = ["_id", "_labels", "_start", "_end", "_type"]
            vertex_files = {}
            edge_files = {}
            batch_size = 1000000

            def write_headers(files_dict, headers, file_name, is_vertex):
                files_dict[file_name] = {}
                headers
                output_file = find_next_file(output_directory, file_name, is_vertex)
                print("Creating new output file: " + output_file)
                files_dict[file_name]["file_handle"] = open(output_file, "w")
                files_dict[file_name]["file_handle"].write(",".join(headers) + "\n")
                files_dict[file_name]["written_lines"] = 1


            def write_line(files_dict, headers, file_name, line, is_vertex, required_headers_in_order):
                output_row = []
                if file_name not in files_dict:
                    write_headers(files_dict, headers, file_name, is_vertex)
                for required_header in required_headers_in_order:
                    output_row.append(line[header.index(required_header)] if required_header != "_labels" else
                                      line[header.index(required_header)][1:])
                for property_header in property_headers:
                    output_row.append(line[header.index(property_header)])
                files_dict[file_name]["file_handle"].write(",".join(output_row) + "\n")
                files_dict[file_name]["written_lines"] += 1
                if files_dict[file_name]["written_lines"] == batch_size:
                    files_dict[file_name]["file_handle"].close()
                    write_headers(files_dict, headers, file_name, is_vertex)


            # Rows are presented as a list, the header is the first list presented.
            for row in csv_reader:
                if header is None:
                    header = row
                    id_index, label_index, start_index, end_index, type_index, property_headers = parse_header(header, reserved_neo4j_headers)
                    vertex_headers = ["~id", "~label"] + property_headers
                    edge_headers = ["~id", "~label", "~from", "~to"] + property_headers
                    continue

                label, type = row[label_index], row[type_index]
                if label != "":
                    label = label[1:]
                    write_line(vertex_files, vertex_headers, label, row, True, ["_id", "_labels"])
                elif type != "":
                    write_line(edge_files, edge_headers, type, row, False, ["_id", "_type", "_start", "_end"])
                else:
                    print("Error, encountered invalid row that is neither a vertex or an edge: " + str(row))

            for file_name in vertex_files:
                vertex_files[file_name]["file_handle"].close()

            for file_name in edge_files:
                edge_files[file_name]["file_handle"].close()


def main():
    input_directory, output_directory = parse_args()
    validate_arguments(input_directory, output_directory)
    input_files = enumerate_input_files(input_directory)
    batch_process_input_files(input_files, output_directory)


if __name__ == "__main__":
    main()