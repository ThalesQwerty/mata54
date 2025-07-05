import heapq
import sys
import os
import tempfile
import shutil

from pathlib import Path

class MinHeapNode:
    def __init__(self, element, file_index):
        self.element = element
        self.file_index = file_index

    def __lt__(self, other):
        return self.element < other.element

def generate_initial_runs(input_file_path: Path, p: int) -> tuple[list[Path], int, int]:
    try:
        input_file = open(input_file_path, 'r')
    except FileNotFoundError:
        print(f"Erro: Arquivo de entrada '{input_file_path}' não encontrado.", file=sys.stderr)
        return [], 0, 0

    active_heap: list[int] = []
    next_heap: list[int] = []

    run_files: list[Path] = []
    run_count = 0
    total_records = 0

    number_buffer = []
    buffer_index = 0

    def read_next_number():
        nonlocal number_buffer, buffer_index, total_records
        
        if buffer_index < len(number_buffer):
            value = number_buffer[buffer_index]
            buffer_index += 1
            return value
        
        while True:
            line = input_file.readline()
            if not line:
                return None
            
            line = line.strip()
            if not line:
                continue
                
            try:
                numbers = [int(x) for x in line.split() if x.strip()]
                if numbers:
                    number_buffer = numbers
                    buffer_index = 0
                    total_records += len(numbers)
                    value = number_buffer[buffer_index]
                    buffer_index += 1
                    return value
            except ValueError:
                continue

    for _ in range(p):
        value = read_next_number()
        if value is None:
            break
        heapq.heappush(active_heap, value)

    if not active_heap:
        input_file.close()
        return [], 0, 0

    while active_heap:
        run_file_path = Path(tempfile.gettempdir()) / f"run_{run_count}.tmp"
        run_files.append(run_file_path)
        with open(run_file_path, 'w') as output_stream:
            last_output = -sys.maxsize - 1

            while active_heap:
                min_value = heapq.heappop(active_heap)
                output_stream.write(str(min_value) + '\n')
                last_output = min_value

                new_val = read_next_number()
                if new_val is not None:
                    if new_val >= last_output:
                        heapq.heappush(active_heap, new_val)
                    else:
                        heapq.heappush(next_heap, new_val)

        run_count += 1
        active_heap, next_heap = next_heap, []

    input_file.close()
    return run_files, run_count, total_records

def merge_runs(run_files: list[Path], output_file_path: Path, p: int) -> int:
    current_run_files = run_files
    passes = 0
    
    while len(current_run_files) > 1:
        passes += 1
        next_pass_files = []

        for i in range(0, len(current_run_files), p):
            min_heap = []
            input_streams = []
            
            temp_output_file_path = Path(tempfile.gettempdir()) / f"temp_pass_{passes}_{i//p}.tmp"
            next_pass_files.append(temp_output_file_path)
            output_stream = open(temp_output_file_path, 'w')
            
            files_to_merge = current_run_files[i:i+p]
            for file_index, file_path in enumerate(files_to_merge):
                try:
                    stream = open(file_path, 'r')
                    input_streams.append(stream)
                    line = stream.readline()
                    if line:
                        element = int(line.strip())
                        heapq.heappush(min_heap, MinHeapNode(element, file_index))
                except FileNotFoundError:
                    print(f"Erro: Arquivo temporário '{file_path}' não encontrado.", file=sys.stderr)
            
            while min_heap:
                min_node = heapq.heappop(min_heap)
                output_stream.write(str(min_node.element) + '\n')
                
                file_index = min_node.file_index
                line = input_streams[file_index].readline()
                if line:
                    element = int(line.strip())
                    heapq.heappush(min_heap, MinHeapNode(element, file_index))
            
            for stream in input_streams:
                stream.close()
            output_stream.close()

        for old_file in current_run_files:
            try:
                os.remove(old_file)
            except OSError as e:
                print(f"Erro ao remover o arquivo temporário: {e}", file=sys.stderr)
        
        current_run_files = next_pass_files

    if current_run_files:
        shutil.move(current_run_files[0], output_file_path)
    
    return passes

def main():
    if len(sys.argv) != 4:
        print(f"Uso: python {sys.argv[0]} <p> <arquivo_entrada> <arquivo_saida>", file=sys.stderr)
        sys.exit(1)

    try:
        p = int(sys.argv[1])
        if p < 2:
            print("O valor de p deve ser maior ou igual a 2.", file=sys.stderr)
            sys.exit(1)
            
        input_file_name = sys.argv[2]
        output_file_name = sys.argv[3]
        input_file_path = Path(input_file_name)
        output_file_path = Path(output_file_name)
        
        if not input_file_path.exists():
            print(f"Erro: Arquivo de entrada '{input_file_path}' não encontrado.", file=sys.stderr)
            sys.exit(1)

    except ValueError:
        print("Erro: O valor de 'p' deve ser um número inteiro.", file=sys.stderr)
        sys.exit(1)
    
    try:
        run_files, initial_runs_count, total_records = generate_initial_runs(input_file_path, p)
        
        if not run_files:
            print("Erro: Nenhuma run foi gerada.", file=sys.stderr)
            sys.exit(1)
        
        num_passes = merge_runs(run_files, output_file_path, p)

        print("#Regs Ways #Runs #Parses")
        print(f"{total_records} {p} {initial_runs_count} {num_passes}")
        
    except Exception as e:
        print(f"Erro durante execução: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()