from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    def __init__(self) -> None:
        self.data: list[str] = []
        self.rank: int = 0
        self.data_count: int = 0

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    def output(self) -> tuple[int, str]:
        if not self.data:
            return (0, "No data available")
        value = self.rank
        self.rank += 1
        return value, self.data.pop(0)


class NumericProcessor(DataProcessor):
    def ingest(self, data: Any) -> None:
        if not self.validate(data):
            raise ValueError("Improper numeric data")
        if isinstance(data, list):
            for n in data:
                self.data.append(str(n))
                self.data_count += 1
        else:
            self.data.append(str(data))
            self.data_count += 1

    def validate(self, data: Any) -> bool:
        if isinstance(data, list):
            for n in data:
                if not isinstance(n, (int, float)):
                    return False
        elif not isinstance(data, (int, float)):
            return False
        return True


class TextProcessor(DataProcessor):
    def ingest(self, data: Any) -> None:
        if not self.validate(data):
            raise ValueError("Improper text data")
        if isinstance(data, list):
            for s in data:
                self.data.append(s)
                self.data_count += 1
        else:
            self.data.append(str(data))
            self.data_count += 1

    def validate(self, data: Any) -> bool:
        if isinstance(data, list):
            for s in data:
                if not isinstance(s, str):
                    return False
            return True
        elif isinstance(data, str):
            return True
        return False


class LogProcessor(DataProcessor):
    def ingest(self, data: Any) -> None:
        if not self.validate(data):
            raise ValueError("Improper log data")
        if isinstance(data, list):
            for dic in data:
                self.data.append(f"{dic['log_level']}: {dic['log_message']}")
                self.data_count += 1
        else:
            self.data.append(f"{data['log_level']}: {data['log_message']}")
            self.data_count += 1

    def validate(self, data: Any) -> bool:
        if isinstance(data, list):
            for item in data:
                if not self.is_valid_dict(item):
                    return False
            return True
        return self.is_valid_dict(data)

    def is_valid_dict(self, item: Any) -> bool:
        if not isinstance(item, dict):
            return False
        if "log_level" not in item or "log_message" not in item:
            return False
        if not isinstance(item["log_level"], str):
            return False
        if not isinstance(item["log_message"], str):
            return False
        return True


class DataStream:
    def __init__(self):
        self.processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self.processors.append(proc)

    def process_stream(self, stream: list[Any]) -> None:
        for item in stream:
            processed = False
            for proc in self.processors:
                if proc.validate(item):
                    processed = True
                    proc.ingest(item)
                    break
            if not processed:
                print(f"DataStream error - Can't process element in stream:"
                      f" {item}")

    def print_processors_stats(self) -> None:
        print("=== DataStream statistics ===")
        if not self.processors:
            print("No processor found, no data")
        else:
            for proc in self.processors:
                proc_name: str = ""
                for i, c in enumerate(proc.__class__.__name__):
                    if c.isupper() and i > 0:
                        proc_name += " "
                    proc_name += c
                print(
                    f"{proc_name}: total {proc.data_count} items processed,"
                    f"remaining {len(proc.data)} on processor"
                )


def main():
    print("=== Code Nexus - Data Stream ===\n")
    print("Initialize Data Stream...")
    d_stream = DataStream()

    d_stream.print_processors_stats()
    print("\nRegistering Numeric Processor\n")
    n_processor = NumericProcessor()
    d_stream.register_processor(n_processor)
    data = [
        "Hello world",
        [3.14, -1, 2.71],
        [
            {"log_level": "WARNING", "log_message":
             "Telnet access! Use ssh instead"},
            {"log_level": "INFO", "log_message": "User wil is connected"},
        ],
        42,
        ["Hi", "five"],
    ]

    print(f"Send first batch of data on stream: {data}")
    d_stream.process_stream(data)
    d_stream.print_processors_stats()
    print("\nRegistering other data processor")
    t_processor = TextProcessor()
    l_processor = LogProcessor()
    d_stream.register_processor(t_processor)
    d_stream.register_processor(l_processor)
    print("Send the same batch again")
    d_stream.process_stream(data)
    d_stream.print_processors_stats()
    print("\nConsume some elements from the data processors:"
          "Numeric 3, Text 2, Log 1")
    for _ in range(3):
        n_processor.output()
    t_processor.output()
    t_processor.output()
    l_processor.output()
    d_stream.print_processors_stats()


if __name__ == "__main__":
    main()
