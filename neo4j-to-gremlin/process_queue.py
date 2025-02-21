import multiprocessing
import time
import threading
from multiprocessing.pool import ThreadPool


def attempt_to_process(files_dict, file_name, lines, header, required_headers_in_order, property_headers):
    for line in lines:
        output_row = []
        for required_header in required_headers_in_order:
            output_row.append(line[header.index(required_header)] if required_header != "_labels" else
                              line[header.index(required_header)][1:])
        for property_header in property_headers:
            output_row.append(line[header.index(property_header)])
        files_dict[file_name]["file_handle"].write(",".join(output_row) + "\n")
    files_dict[file_name]["written_lines"] += len(lines)

class ProcessQueue:
    __cpu_count = multiprocessing.cpu_count()
    __pool = ThreadPool(__cpu_count)
    __failures = []
    __futures = []
    __events = []
    __lock = threading.Lock()

    def __init__(self):
        pass

    def get_failures(self):
        for f in self.__futures:
            f.wait()
        return self.__failures

    def submit(self, files_dict, file_name, lines, header, required_headers_in_order, property_header):
        self.__futures = [future for future in self.__futures if not future.ready()]
        while len(self.__futures) >= self.__cpu_count:
            # Need to block until a thread is finished
            time.sleep(0.1)
            self.__futures = [future for future in self.__futures if not future.ready()]

        self.__futures.append(
            self.__pool.apply_async(
                attempt_to_process,
                args=(files_dict, file_name, lines, header, required_headers_in_order, property_header),
                error_callback=self.__error_callback)
        )

    def __error_callback(self, e):
        self.__failures.append(e)
