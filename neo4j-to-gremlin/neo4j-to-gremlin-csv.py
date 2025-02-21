import sys, os, getopt, csv
from process_queue import ProcessQueue


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
    process_queue = ProcessQueue()
    for input_file in input_file_list:
        print("Currently parsing input file:" + input_file)
        total_lines = 0
        batch_size = 1000
        vertex_headers = ["_id", "_labels"]
        edge_headers = ["_id", "_type", "_start", "_end"]
        with open(input_file, "r", newline='', buffering=batch_size) as input_csv_file:
            csv_reader = csv.reader(input_csv_file, delimiter=",", quotechar='"')
            label_index = type_index = header = None
            reserved_neo4j_headers = ["_id", "_labels", "_start", "_end", "_type"]
            files_dict = {}

            def create_file_if_not_exists(is_vertex, property_headers):
                actual_directory = os.path.join(output_directory, "vertices" if is_vertex else "edges")
                output_label = label
                if ":" in label:
                    output_label = label.split(":")[0] + "_" + label.split(":")[1]
                if label == "":
                    output_label = "vertex" if is_vertex else "edge"
                if not os.path.exists(actual_directory):
                    os.makedirs(actual_directory)
                valid_file_path = os.path.join(actual_directory, str(output_label) + ".csv")
                if files_dict.get(valid_file_path) is None:
                    files_dict[valid_file_path] = {}
                    files_dict[valid_file_path]["file_handle"] = open(valid_file_path, "w")
                    if is_vertex:
                        files_dict[valid_file_path]["file_handle"].write(",".join(vertex_headers) + "," + ",".join(property_headers) + "\n")
                    else:
                        files_dict[valid_file_path]["file_handle"].write(",".join(edge_headers) + "," + ",".join(property_headers) + "\n")
                    files_dict[valid_file_path]["written_lines"] = 1
                    files_dict[valid_file_path]["type"] = "vertex" if is_vertex else "edge"
                    files_dict[valid_file_path]["rows"] = []
                return valid_file_path

            # Rows are presented as a list, the header is the first list presented.
            for row in csv_reader:
                total_lines += 1
                if total_lines % 100_000 == 0:
                    print("Processed " + str(total_lines) + " lines.")

                if header is None:
                    header = row
                    id_index, label_index, start_index, end_index, type_index, property_headers = parse_header(header, reserved_neo4j_headers)
                    continue

                label, type = row[label_index], row[type_index]
                if type != "":
                    relevant_headers = ["_id", "_type", "_start", "_end"]
                    local_file_name = create_file_if_not_exists(False, property_headers)
                else:
                    relevant_headers = ["_id", "_labels"]
                    if label != "":
                        label = label[1:]
                    local_file_name = create_file_if_not_exists(True, property_headers)

                #else:
                #    print("Error, encountered invalid row that is neither a vertex or an edge: " + str(row))
                #    sys.exit(1)
                files_dict[local_file_name]["rows"].append(row)
                if len(files_dict[local_file_name]["rows"]) == batch_size:
                    # Submit to process queue
                    process_queue.submit(files_dict, local_file_name, files_dict[local_file_name]["rows"], header,
                                         relevant_headers, property_headers)
                    print("Submitting " + str(batch_size) + " rows to process queue for " + local_file_name + ".")
                    files_dict[local_file_name]["rows"] = []

    for local_file_name in files_dict.keys():
        if files_dict[local_file_name]["rows"] is not None:
            print("Submitting " + str(
                len(files_dict[local_file_name]["rows"])) + " rows to process queue for " + local_file_name + ".")
            if files_dict[local_file_name]["type"] == "vertex":
                process_queue.submit(files_dict, local_file_name, files_dict[local_file_name]["rows"], header,
                                     ["_id", "_labels"], property_headers)
            else:
                process_queue.submit(files_dict, local_file_name, files_dict[local_file_name]["rows"], header,
                                     ["_id", "_type", "_start", "_end"], property_headers)

    print("waiting for processing to complete")
    failures = process_queue.get_failures()

    for file_name in files_dict.keys():
        files_dict[file_name]["file_handle"].close()

    if len(failures) > 0:
        print("Failed to process the following files:")
        for failure in failures:
            print(failure)
        sys.exit(1)


def main():
    input_directory, output_directory = parse_args()
    validate_arguments(input_directory, output_directory)
    input_files = enumerate_input_files(input_directory)
    print("files: " + str(input_files))
    batch_process_input_files(input_files, output_directory)


if __name__ == "__main__":
    main()
