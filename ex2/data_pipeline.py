from abc import ABC, abstractmethod
from typing import Any, Protocol


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
            return True
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
                self.data.append(
                    f"{dic['log_level']}: {dic['log_message']}"
                )
                self.data_count += 1
        else:
            self.data.append(
                f"{data['log_level']}: {data['log_message']}"
            )
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


class ExportPlugin(Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        ...


class CSVExportPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        if not data:
            return
        print("CSV Output:")
        csv_string = ",".join(item[1] for item in data)
        print(csv_string)


class JSONExportPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        if not data:
            return
        print("JSON Output:")
        json_parts = []
        for rank, value in data:
            escaped_value = value.replace('"', '\\"')
            json_parts.append(f'"item_{rank}": "{escaped_value}"')

        json_string = "{" + ", ".join(json_parts) + "}"
        print(json_string)


class DataStream:
    def __init__(self) -> None:
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
                print(
                    f"DataStream error - Can't process element in "
                    f"stream: {item}"
                )

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for proc in self.processors:
            collected_data: list[tuple[int, str]] = []
            for _ in range(nb):
                if not proc.data:
                    break
                collected_data.append(proc.output())

            if collected_data:
                plugin.process_output(collected_data)

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
                    f"{proc_name}: total {proc.data_count} items "
                    f"processed, remaining {len(proc.data)} on "
                    f"processor"
                )


def main() -> None:
    print("=== Code Nexus - Data Pipeline ===\n")
    print("Initialize Data Stream...")
    d_stream = DataStream()
    d_stream.print_processors_stats()

    print("\nRegistering Processors")
    n_processor = NumericProcessor()
    t_processor = TextProcessor()
    l_processor = LogProcessor()

    d_stream.register_processor(n_processor)
    d_stream.register_processor(t_processor)
    d_stream.register_processor(l_processor)

    batch_1 = [
        "Hello world",
        [3.14, -1, 2.71],
        [
            {
                "log_level": "WARNING",
                "log_message": "Telnet access! Use ssh instead",
            },
            {
                "log_level": "INFO",
                "log_message": "User wil is connected",
            },
        ],
        42,
        ["Hi", "five"],
    ]

    print(f"Send first batch of data on stream: {batch_1}")
    d_stream.process_stream(batch_1)
    d_stream.print_processors_stats()

    print("\nSend 3 processed data from each processor to a CSV plugin:")
    csv_plugin = CSVExportPlugin()
    d_stream.output_pipeline(3, csv_plugin)
    d_stream.print_processors_stats()

    print(
        "\nSend another batch of data: [21, ['I love AI', 'LLMs are "
        "wonderful', 'Stay healthy'], [{'log_level': 'ERROR', "
        "'log_message': '500 server crash'}, {'log_level': 'NOTICE', "
        "'log_message': 'Certificate expires in 10 days'}], [32, 42, 64, "
        "84, 128, 168], 'World hello']"
    )

    batch_2 = [
        21,
        ["I love AI", "LLMs are wonderful", "Stay healthy"],
        [
            {
                "log_level": "ERROR",
                "log_message": "500 server crash",
            },
            {
                "log_level": "NOTICE",
                "log_message": "Certificate expires in 10 days",
            },
        ],
        [32, 42, 64, 84, 128, 168],
        "World hello",
    ]
    d_stream.process_stream(batch_2)
    d_stream.print_processors_stats()

    print("\nSend 5 processed data from each processor to a JSON plugin:")
    json_plugin = JSONExportPlugin()
    d_stream.output_pipeline(5, json_plugin)
    d_stream.print_processors_stats()


if __name__ == "__main__":
    main()
